"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var SchedulerService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchedulerService = void 0;
const common_1 = require("@nestjs/common");
const schedule_1 = require("@nestjs/schedule");
const client_1 = require("@prisma/client");
const campaign_execution_producer_1 = require("../queue/campaign-execution.producer");
let SchedulerService = SchedulerService_1 = class SchedulerService {
    constructor(campaignExecutionProducer) {
        this.campaignExecutionProducer = campaignExecutionProducer;
        this.logger = new common_1.Logger(SchedulerService_1.name);
        this.isProcessing = false;
        this.prisma = new client_1.PrismaClient();
    }
    /**
     * Check for scheduled campaigns every minute
     * Runs at the start of every minute
     */
    async checkScheduledCampaigns() {
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
                    this.logger.log(`Starting campaign: ${campaign.name} (${campaign.id}) - Scheduled for: ${campaign.scheduledAt}`);
                    // Start the campaign execution
                    const result = await this.campaignExecutionProducer.startCampaign(campaign.id);
                    if (result.success) {
                        this.logger.log(`Campaign ${campaign.id} started successfully. Queued ${result.totalQueued} recipients.`);
                    }
                    else {
                        this.logger.error(`Failed to start campaign ${campaign.id}: ${result.message}`);
                    }
                }
                catch (error) {
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
        }
        catch (error) {
            this.logger.error(`Scheduler error: ${error.message}`);
        }
        finally {
            this.isProcessing = false;
        }
    }
    /**
     * Check for stale running campaigns (campaigns that have been running for too long)
     * Runs every 5 minutes
     */
    async checkStaleCampaigns() {
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
                        this.logger.warn(`Campaign ${campaign.id} appears stale. Processed: ${campaign.processedCount}/${campaign.totalRecipients}`);
                        // Optionally mark as failed or send notification
                    }
                }
            }
        }
        catch (error) {
            this.logger.error(`Error checking stale campaigns: ${error.message}`);
        }
    }
    /**
     * Log queue statistics every 5 minutes
     */
    async logQueueStats() {
        try {
            const stats = await this.campaignExecutionProducer.getQueueStats();
            this.logger.log(`Queue stats - Waiting: ${stats.waiting}, Active: ${stats.active}, Completed: ${stats.completed}, Failed: ${stats.failed}`);
        }
        catch (error) {
            this.logger.error(`Error getting queue stats: ${error.message}`);
        }
    }
    /**
     * Manually trigger a campaign (for testing or manual override)
     */
    async triggerCampaign(campaignId) {
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
        }
        catch (error) {
            return { success: false, message: error.message };
        }
    }
};
exports.SchedulerService = SchedulerService;
__decorate([
    (0, schedule_1.Cron)(schedule_1.CronExpression.EVERY_MINUTE),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], SchedulerService.prototype, "checkScheduledCampaigns", null);
__decorate([
    (0, schedule_1.Cron)(schedule_1.CronExpression.EVERY_5_MINUTES),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], SchedulerService.prototype, "checkStaleCampaigns", null);
__decorate([
    (0, schedule_1.Cron)(schedule_1.CronExpression.EVERY_5_MINUTES),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], SchedulerService.prototype, "logQueueStats", null);
exports.SchedulerService = SchedulerService = SchedulerService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [campaign_execution_producer_1.CampaignExecutionProducer])
], SchedulerService);
