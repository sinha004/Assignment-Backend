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
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var CampaignExecutionProducer_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.CampaignExecutionProducer = void 0;
const common_1 = require("@nestjs/common");
const bullmq_1 = require("@nestjs/bullmq");
const bullmq_2 = require("bullmq");
const client_1 = require("@prisma/client");
const queue_constants_1 = require("./queue.constants");
const client_s3_1 = require("@aws-sdk/client-s3");
const config_1 = require("@nestjs/config");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const csvParser = require('csv-parser');
let CampaignExecutionProducer = CampaignExecutionProducer_1 = class CampaignExecutionProducer {
    constructor(campaignQueue, configService) {
        this.campaignQueue = campaignQueue;
        this.configService = configService;
        this.logger = new common_1.Logger(CampaignExecutionProducer_1.name);
        this.prisma = new client_1.PrismaClient();
        // Initialize S3 client
        this.bucketName = this.configService.get('Amazon_S3_BUCKET_NAME') || '';
        this.s3Client = new client_s3_1.S3Client({
            region: this.configService.get('Amazon_REGION'),
            credentials: {
                accessKeyId: this.configService.get('Amazon_ACCESS_KEY_ID') || '',
                secretAccessKey: this.configService.get('Amazon_SECRET_ACCESS_KEY') || '',
            },
        });
    }
    /**
     * Start campaign execution - downloads CSV from S3 and queues all recipients
     */
    async startCampaign(campaignId) {
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
            const n8nWebhookBaseUrl = this.configService.get('N8N_WEBHOOK_URL') || 'http://localhost:5678/webhook';
            const webhookUrl = `${n8nWebhookBaseUrl}/campaign-${campaignId}`;
            // Stream CSV from S3 and queue jobs
            const totalQueued = await this.streamAndQueueRecipients(campaign.segment.s3Key, campaignId, webhookUrl);
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
        }
        catch (error) {
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
    async streamAndQueueRecipients(s3Key, campaignId, webhookUrl) {
        return new Promise(async (resolve, reject) => {
            try {
                // Get object from S3
                const command = new client_s3_1.GetObjectCommand({
                    Bucket: this.bucketName,
                    Key: s3Key,
                });
                const response = await this.s3Client.send(command);
                if (!response.Body) {
                    throw new Error('Empty response from S3');
                }
                const jobs = [];
                let rowCount = 0;
                const batchSize = 100;
                // Convert S3 body to readable stream
                const bodyStream = response.Body;
                bodyStream
                    .pipe(csvParser())
                    .on('data', async (row) => {
                    rowCount++;
                    // Extract email and name from row (handle different column names)
                    const email = row.email || row.Email || row.EMAIL || row.e_mail;
                    const name = row.name || row.Name || row.NAME || row.full_name || row.fullName || null;
                    if (!email) {
                        this.logger.warn(`Row ${rowCount} has no email, skipping`);
                        return;
                    }
                    // Create job data
                    const jobData = {
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
                        }
                        catch (err) {
                            this.logger.error(`Failed to add batch to queue: ${err.message}`);
                        }
                    }
                })
                    .on('end', async () => {
                    // Add remaining jobs
                    if (jobs.length > 0) {
                        try {
                            await this.campaignQueue.addBulk(jobs);
                        }
                        catch (err) {
                            this.logger.error(`Failed to add final batch to queue: ${err.message}`);
                        }
                    }
                    this.logger.log(`Finished queueing ${rowCount} recipients for campaign ${campaignId}`);
                    resolve(rowCount);
                })
                    .on('error', (error) => {
                    this.logger.error(`CSV parsing error: ${error.message}`);
                    reject(error);
                });
            }
            catch (error) {
                this.logger.error(`Failed to stream CSV from S3: ${error.message}`);
                reject(error);
            }
        });
    }
    /**
     * Add a single job to the queue (for retries or manual triggers)
     */
    async addJob(jobData) {
        await this.campaignQueue.add(`campaign-${jobData.campaignId}-${jobData.recipientEmail}`, jobData);
    }
    /**
     * Get queue statistics
     */
    async getQueueStats() {
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
    async pauseCampaign(campaignId) {
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
    async resumeCampaign(campaignId) {
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
};
exports.CampaignExecutionProducer = CampaignExecutionProducer;
exports.CampaignExecutionProducer = CampaignExecutionProducer = CampaignExecutionProducer_1 = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, bullmq_1.InjectQueue)(queue_constants_1.CAMPAIGN_QUEUE)),
    __metadata("design:paramtypes", [bullmq_2.Queue,
        config_1.ConfigService])
], CampaignExecutionProducer);
