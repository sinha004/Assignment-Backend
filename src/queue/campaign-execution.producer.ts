import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { PrismaClient } from '@prisma/client';
import { CAMPAIGN_QUEUE } from './queue.constants';
import { Readable } from 'stream';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { ConfigService } from '@nestjs/config';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const csvParser = require('csv-parser');

export interface CampaignJobData {
  campaignId: string;
  recipientEmail: string;
  recipientName: string | null;
  recipientData: Record<string, any>;
  webhookUrl: string;
  attempt: number;
}

@Injectable()
export class CampaignExecutionProducer {
  private readonly logger = new Logger(CampaignExecutionProducer.name);
  private prisma: PrismaClient;
  private s3Client: S3Client;
  private bucketName: string;

  constructor(
    @InjectQueue(CAMPAIGN_QUEUE) private campaignQueue: Queue,
    private configService: ConfigService,
  ) {
    this.prisma = new PrismaClient();
    
    // Initialize S3 client
    this.bucketName = this.configService.get<string>('AWS_S3_BUCKET_NAME') || '';
    this.s3Client = new S3Client({
      region: this.configService.get<string>('AWS_REGION'),
      credentials: {
        accessKeyId: this.configService.get<string>('AWS_ACCESS_KEY_ID') || '',
        secretAccessKey: this.configService.get<string>('AWS_SECRET_ACCESS_KEY') || '',
      },
    });
  }

  /**
   * Start campaign execution - downloads CSV from S3 and queues all recipients
   */
  async startCampaign(campaignId: string): Promise<{ success: boolean; message: string; totalQueued?: number }> {
    this.logger.log(`Starting campaign execution: ${campaignId}`);

    try {
      // Get campaign with segment info
      const campaign = await this.prisma.campaign.findUnique({
        where: { id: campaignId },
        include: {
          segment: true,
        },
      });

      if (!campaign) {
        throw new Error(`Campaign not found: ${campaignId}`);
      }

      // Validate campaign state
      if (campaign.status !== 'scheduled') {
        throw new Error(`Campaign is not in scheduled status. Current status: ${campaign.status}`);
      }

      if (!campaign.n8nWorkflowId) {
        throw new Error('Campaign does not have a deployed n8n workflow');
      }

      if (!campaign.segment) {
        throw new Error('Campaign does not have a segment assigned');
      }

      // Update campaign status to running
      await this.prisma.campaign.update({
        where: { id: campaignId },
        data: {
          status: 'running',
          startedAt: new Date(),
          processedCount: 0,
          successCount: 0,
          failedCount: 0,
        },
      });

      this.logger.log(`Campaign ${campaignId} status updated to running`);

      // Get webhook URL
      const n8nWebhookBaseUrl = this.configService.get<string>('N8N_WEBHOOK_URL') || 'http://localhost:5678/webhook';
      const webhookUrl = `${n8nWebhookBaseUrl}/campaign-${campaignId}`;

      // Stream CSV from S3 and queue jobs
      const totalQueued = await this.streamAndQueueRecipients(
        campaign.segment.s3Key,
        campaignId,
        webhookUrl,
      );

      // Update total recipients count
      await this.prisma.campaign.update({
        where: { id: campaignId },
        data: {
          totalRecipients: totalQueued,
          totalJobsCreated: totalQueued,
        },
      });

      this.logger.log(`Campaign ${campaignId}: Queued ${totalQueued} recipients for processing`);

      return {
        success: true,
        message: `Successfully queued ${totalQueued} recipients for campaign ${campaignId}`,
        totalQueued,
      };
    } catch (error: any) {
      this.logger.error(`Failed to start campaign ${campaignId}: ${error.message}`);

      // Mark campaign as failed
      await this.prisma.campaign.update({
        where: { id: campaignId },
        data: {
          status: 'failed',
          executionStatus: 'failed',
        },
      });

      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Stream CSV from S3 and add jobs to queue
   */
  private async streamAndQueueRecipients(
    s3Key: string,
    campaignId: string,
    webhookUrl: string,
  ): Promise<number> {
    return new Promise(async (resolve, reject) => {
      try {
        // Get object from S3
        const command = new GetObjectCommand({
          Bucket: this.bucketName,
          Key: s3Key,
        });

        const response = await this.s3Client.send(command);
        
        if (!response.Body) {
          throw new Error('Empty response from S3');
        }

        const jobs: { name: string; data: CampaignJobData }[] = [];
        let rowCount = 0;
        const batchSize = 100;

        // Convert S3 body to readable stream
        const bodyStream = response.Body as Readable;

        bodyStream
          .pipe(csvParser())
          .on('data', async (row: any) => {
            rowCount++;

            // Extract email and name from row (handle different column names)
            const email = row.email || row.Email || row.EMAIL || row.e_mail;
            const name = row.name || row.Name || row.NAME || row.full_name || row.fullName || null;

            if (!email) {
              this.logger.warn(`Row ${rowCount} has no email, skipping`);
              return;
            }

            // Create job data
            const jobData: CampaignJobData = {
              campaignId,
              recipientEmail: email.trim().toLowerCase(),
              recipientName: name ? name.trim() : null,
              recipientData: row,
              webhookUrl,
              attempt: 1,
            };

            jobs.push({
              name: `campaign-${campaignId}-recipient-${rowCount}`,
              data: jobData,
            });

            // Add jobs in batches for efficiency
            if (jobs.length >= batchSize) {
              try {
                await this.campaignQueue.addBulk(jobs.splice(0, batchSize));
              } catch (err: any) {
                this.logger.error(`Failed to add batch to queue: ${err.message}`);
              }
            }
          })
          .on('end', async () => {
            // Add remaining jobs
            if (jobs.length > 0) {
              try {
                await this.campaignQueue.addBulk(jobs);
              } catch (err: any) {
                this.logger.error(`Failed to add final batch to queue: ${err.message}`);
              }
            }

            this.logger.log(`Finished queueing ${rowCount} recipients for campaign ${campaignId}`);
            resolve(rowCount);
          })
          .on('error', (error: any) => {
            this.logger.error(`CSV parsing error: ${error.message}`);
            reject(error);
          });
      } catch (error: any) {
        this.logger.error(`Failed to stream CSV from S3: ${error.message}`);
        reject(error);
      }
    });
  }

  /**
   * Add a single job to the queue (for retries or manual triggers)
   */
  async addJob(jobData: CampaignJobData): Promise<void> {
    await this.campaignQueue.add(
      `campaign-${jobData.campaignId}-${jobData.recipientEmail}`,
      jobData,
    );
  }

  /**
   * Get queue statistics
   */
  async getQueueStats(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
  }> {
    const [waiting, active, completed, failed] = await Promise.all([
      this.campaignQueue.getWaitingCount(),
      this.campaignQueue.getActiveCount(),
      this.campaignQueue.getCompletedCount(),
      this.campaignQueue.getFailedCount(),
    ]);

    return { waiting, active, completed, failed };
  }

  /**
   * Pause all jobs for a specific campaign
   */
  async pauseCampaign(campaignId: string): Promise<void> {
    // Pause the queue
    await this.campaignQueue.pause();
    
    // Update campaign status
    await this.prisma.campaign.update({
      where: { id: campaignId },
      data: {
        status: 'paused',
        executionStatus: 'paused',
      },
    });

    this.logger.log(`Campaign ${campaignId} paused`);
  }

  /**
   * Resume campaign processing
   */
  async resumeCampaign(campaignId: string): Promise<void> {
    // Resume the queue
    await this.campaignQueue.resume();
    
    // Update campaign status
    await this.prisma.campaign.update({
      where: { id: campaignId },
      data: {
        status: 'running',
        executionStatus: 'active',
      },
    });

    this.logger.log(`Campaign ${campaignId} resumed`);
  }
}
