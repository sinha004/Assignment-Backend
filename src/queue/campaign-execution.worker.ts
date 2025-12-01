import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { PrismaClient } from '@prisma/client';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import { CAMPAIGN_QUEUE } from './queue.constants';
import { CampaignJobData } from './campaign-execution.producer';

@Processor(CAMPAIGN_QUEUE, {
  concurrency: 5, // Process 5 jobs at a time
  limiter: {
    max: 100, // Max 100 jobs
    duration: 60000, // Per minute (rate limiting for email providers)
  },
})
export class CampaignExecutionWorker extends WorkerHost {
  private readonly logger = new Logger(CampaignExecutionWorker.name);
  private prisma: PrismaClient;

  constructor(private configService: ConfigService) {
    super();
    this.prisma = new PrismaClient();
    this.logger.log('🚀 CampaignExecutionWorker initialized');
  }

  /**
   * Called when the worker is ready to process jobs
   */
  @OnWorkerEvent('ready')
  onReady() {
    this.logger.log('✅ Worker is READY and listening for jobs');
  }

  /**
   * Called when the worker is stalled
   */
  @OnWorkerEvent('stalled')
  onStalled(jobId: string) {
    this.logger.warn(`⚠️ Job ${jobId} stalled`);
  }

  /**
   * Called when worker starts processing
   */
  @OnWorkerEvent('active')
  onActive(job: Job<CampaignJobData>) {
    this.logger.log(`▶️ Starting to process job ${job.id} for ${job.data.recipientEmail}`);
  }

  /**
   * Process each job - call n8n webhook with recipient data
   */
  async process(job: Job<CampaignJobData>): Promise<any> {
    const { campaignId, recipientEmail, recipientName, recipientData, webhookUrl } = job.data;

    this.logger.debug(`Processing job for campaign ${campaignId}: ${recipientEmail}`);

    try {
      // Create or update execution record
      const execution = await this.prisma.campaignExecution.upsert({
        where: {
          id: `${campaignId}-${recipientEmail}`,
        },
        create: {
          id: `${campaignId}-${recipientEmail}`,
          campaignId,
          email: recipientEmail,
          name: recipientName,
          status: 'processing',
          attempts: job.attemptsMade + 1,
        },
        update: {
          status: 'processing',
          attempts: job.attemptsMade + 1,
        },
      });

      // Ensure we use PRODUCTION webhook URL (not webhook-test)
      // Production: /webhook/xxx - works when workflow is ACTIVE (24/7 listening)
      // Test: /webhook-test/xxx - only works with manual "Execute Workflow" click
      const productionWebhookUrl = webhookUrl
        .replace('/webhook-test/', '/webhook/')
        .replace('/webhook-test', '/webhook');

      // Call n8n webhook with recipient data
      // Send data directly - n8n will receive it as $json.email, $json.name, etc.
      const webhookPayload = {
        email: recipientEmail,
        name: recipientName || '',
        ...recipientData,
        _meta: {
          campaignId,
          executionId: execution.id,
          attempt: job.attemptsMade + 1,
          timestamp: new Date().toISOString(),
        },
      };

      this.logger.debug(`Calling n8n PRODUCTION webhook: ${productionWebhookUrl}`);

      // Call production webhook
      let response;
      try {
        response = await axios.post(productionWebhookUrl, webhookPayload, {
          timeout: 30000, // 30 seconds timeout
          headers: {
            'Content-Type': 'application/json',
          },
        });
        this.logger.log(`✅ Webhook success for ${recipientEmail}: ${response.status}`);
      } catch (webhookError: any) {
        if (webhookError.response?.status === 404) {
          this.logger.error(`❌ Webhook 404 - Workflow is NOT ACTIVE!`);
          this.logger.error(`   Please go to n8n and toggle the workflow to ACTIVE.`);
          this.logger.error(`   URL attempted: ${productionWebhookUrl}`);
        }
        throw webhookError;
      }

      // Update execution record as success
      await this.prisma.campaignExecution.update({
        where: { id: execution.id },
        data: {
          status: 'success',
          n8nExecutionId: response.data?.executionId || null,
          processedAt: new Date(),
        },
      });

      // Increment success count
      await this.prisma.campaign.update({
        where: { id: campaignId },
        data: {
          processedCount: { increment: 1 },
          successCount: { increment: 1 },
        },
      });

      // Check if campaign is complete
      await this.checkCampaignCompletion(campaignId);

      this.logger.debug(`Successfully processed ${recipientEmail} for campaign ${campaignId}`);

      return {
        success: true,
        email: recipientEmail,
        n8nResponse: response.data,
      };
    } catch (error: any) {
      this.logger.error(`Failed to process ${recipientEmail}: ${error.message}`);

      // If this is the last attempt, mark as failed
      if (job.attemptsMade >= (job.opts.attempts || 3) - 1) {
        await this.handleFinalFailure(campaignId, recipientEmail, recipientName, error.message);
      }

      throw error; // Rethrow to trigger retry
    }
  }

  /**
   * Handle final failure after all retries exhausted
   */
  private async handleFinalFailure(
    campaignId: string,
    recipientEmail: string,
    recipientName: string | null,
    errorMessage: string,
  ): Promise<void> {
    // Update execution record as failed
    await this.prisma.campaignExecution.upsert({
      where: {
        id: `${campaignId}-${recipientEmail}`,
      },
      create: {
        id: `${campaignId}-${recipientEmail}`,
        campaignId,
        email: recipientEmail,
        name: recipientName,
        status: 'failed',
        error: errorMessage,
        processedAt: new Date(),
      },
      update: {
        status: 'failed',
        error: errorMessage,
        processedAt: new Date(),
      },
    });

    // Increment failed count
    await this.prisma.campaign.update({
      where: { id: campaignId },
      data: {
        processedCount: { increment: 1 },
        failedCount: { increment: 1 },
      },
    });

    // Check if campaign is complete
    await this.checkCampaignCompletion(campaignId);
  }

  /**
   * Check if all jobs for a campaign are processed
   */
  private async checkCampaignCompletion(campaignId: string): Promise<void> {
    const campaign = await this.prisma.campaign.findUnique({
      where: { id: campaignId },
    });

    if (!campaign) return;

    // Check if all recipients have been processed
    if (campaign.processedCount >= campaign.totalRecipients && campaign.totalRecipients > 0) {
      await this.prisma.campaign.update({
        where: { id: campaignId },
        data: {
          status: 'completed',
          executionStatus: 'completed',
          completedAt: new Date(),
        },
      });

      this.logger.log(
        `Campaign ${campaignId} completed! Success: ${campaign.successCount}, Failed: ${campaign.failedCount}`,
      );
    }
  }

  /**
   * Event handler for job completion
   */
  @OnWorkerEvent('completed')
  onCompleted(job: Job<CampaignJobData>) {
    this.logger.debug(`Job ${job.id} completed for ${job.data.recipientEmail}`);
  }

  /**
   * Event handler for job failure
   */
  @OnWorkerEvent('failed')
  onFailed(job: Job<CampaignJobData>, error: Error) {
    this.logger.warn(
      `Job ${job.id} failed for ${job.data.recipientEmail}: ${error.message} (attempt ${job.attemptsMade})`,
    );
  }

  /**
   * Event handler for job error
   */
  @OnWorkerEvent('error')
  onError(error: Error) {
    this.logger.error(`Worker error: ${error.message}`);
  }
}
