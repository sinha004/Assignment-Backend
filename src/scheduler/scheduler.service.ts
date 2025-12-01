import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PrismaClient } from '@prisma/client';
import { CampaignExecutionProducer } from '../queue/campaign-execution.producer';

@Injectable()
export class SchedulerService {
  private readonly logger = new Logger(SchedulerService.name);
  private prisma: PrismaClient;
  private isProcessing = false;

  constructor(private campaignExecutionProducer: CampaignExecutionProducer) {
    this.prisma = new PrismaClient();
  }

  /**
   * Check for scheduled campaigns every minute
   * Runs at the start of every minute
   */
  @Cron(CronExpression.EVERY_MINUTE)
  async checkScheduledCampaigns(): Promise<void> {
    // Prevent overlapping executions
    if (this.isProcessing) {
      this.logger.debug('Scheduler already processing, skipping...');
      return;
    }

    this.isProcessing = true;

    try {
      const now = new Date();
      this.logger.debug(`Checking for scheduled campaigns at ${now.toISOString()}`);

      // Find all campaigns that:
      // 1. Have status 'scheduled'
      // 2. Have scheduledAt <= now
      // 3. Have a deployed n8n workflow
      const dueCampaigns = await this.prisma.campaign.findMany({
        where: {
          status: 'scheduled',
          scheduledAt: {
            lte: now,
          },
          n8nWorkflowId: {
            not: null,
          },
        },
        include: {
          segment: {
            select: {
              id: true,
              name: true,
              totalRecords: true,
            },
          },
        },
      });

      if (dueCampaigns.length === 0) {
        this.logger.debug('No scheduled campaigns due for execution');
        return;
      }

      this.logger.log(`Found ${dueCampaigns.length} campaigns due for execution`);

      // Process each due campaign
      for (const campaign of dueCampaigns) {
        try {
          this.logger.log(
            `Starting campaign: ${campaign.name} (${campaign.id}) - Scheduled for: ${campaign.scheduledAt}`,
          );

          // Start the campaign execution
          const result = await this.campaignExecutionProducer.startCampaign(campaign.id);

          if (result.success) {
            this.logger.log(
              `Campaign ${campaign.id} started successfully. Queued ${result.totalQueued} recipients.`,
            );
          } else {
            this.logger.error(`Failed to start campaign ${campaign.id}: ${result.message}`);
          }
        } catch (error: any) {
          this.logger.error(`Error starting campaign ${campaign.id}: ${error.message}`);

          // Mark campaign as failed
          await this.prisma.campaign.update({
            where: { id: campaign.id },
            data: {
              status: 'failed',
              executionStatus: 'failed',
            },
          });
        }
      }
    } catch (error: any) {
      this.logger.error(`Scheduler error: ${error.message}`);
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Check for stale running campaigns (campaigns that have been running for too long)
   * Runs every 5 minutes
   */
  @Cron(CronExpression.EVERY_5_MINUTES)
  async checkStaleCampaigns(): Promise<void> {
    try {
      const staleThreshold = new Date();
      staleThreshold.setHours(staleThreshold.getHours() - 24); // 24 hours ago

      // Find campaigns that have been running for more than 24 hours
      const staleCampaigns = await this.prisma.campaign.findMany({
        where: {
          status: 'running',
          startedAt: {
            lt: staleThreshold,
          },
        },
      });

      if (staleCampaigns.length > 0) {
        this.logger.warn(`Found ${staleCampaigns.length} stale campaigns`);

        for (const campaign of staleCampaigns) {
          // Check if there's actually progress
          if (campaign.processedCount < campaign.totalRecipients) {
            this.logger.warn(
              `Campaign ${campaign.id} appears stale. Processed: ${campaign.processedCount}/${campaign.totalRecipients}`,
            );
            // Optionally mark as failed or send notification
          }
        }
      }
    } catch (error: any) {
      this.logger.error(`Error checking stale campaigns: ${error.message}`);
    }
  }

  /**
   * Log queue statistics every 5 minutes
   */
  @Cron(CronExpression.EVERY_5_MINUTES)
  async logQueueStats(): Promise<void> {
    try {
      const stats = await this.campaignExecutionProducer.getQueueStats();
      this.logger.log(
        `Queue stats - Waiting: ${stats.waiting}, Active: ${stats.active}, Completed: ${stats.completed}, Failed: ${stats.failed}`,
      );
    } catch (error: any) {
      this.logger.error(`Error getting queue stats: ${error.message}`);
    }
  }

  /**
   * Manually trigger a campaign (for testing or manual override)
   */
  async triggerCampaign(campaignId: string): Promise<{ success: boolean; message: string }> {
    try {
      const campaign = await this.prisma.campaign.findUnique({
        where: { id: campaignId },
      });

      if (!campaign) {
        return { success: false, message: 'Campaign not found' };
      }

      if (!campaign.n8nWorkflowId) {
        return { success: false, message: 'Campaign does not have a deployed workflow' };
      }

      // Update to scheduled status first if it's in draft
      if (campaign.status === 'draft') {
        await this.prisma.campaign.update({
          where: { id: campaignId },
          data: {
            status: 'scheduled',
            scheduledAt: new Date(),
          },
        });
      }

      // Start the campaign
      const result = await this.campaignExecutionProducer.startCampaign(campaignId);
      return result;
    } catch (error: any) {
      return { success: false, message: error.message };
    }
  }
}
