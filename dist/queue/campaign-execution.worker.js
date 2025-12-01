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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var CampaignExecutionWorker_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.CampaignExecutionWorker = void 0;
const bullmq_1 = require("@nestjs/bullmq");
const common_1 = require("@nestjs/common");
const bullmq_2 = require("bullmq");
const client_1 = require("@prisma/client");
const config_1 = require("@nestjs/config");
const axios_1 = __importDefault(require("axios"));
const queue_constants_1 = require("./queue.constants");
let CampaignExecutionWorker = CampaignExecutionWorker_1 = class CampaignExecutionWorker extends bullmq_1.WorkerHost {
    constructor(configService) {
        super();
        this.configService = configService;
        this.logger = new common_1.Logger(CampaignExecutionWorker_1.name);
        this.prisma = new client_1.PrismaClient();
        this.logger.log('🚀 CampaignExecutionWorker initialized');
    }
    /**
     * Called when the worker is ready to process jobs
     */
    onReady() {
        this.logger.log('✅ Worker is READY and listening for jobs');
    }
    /**
     * Called when the worker is stalled
     */
    onStalled(jobId) {
        this.logger.warn(`⚠️ Job ${jobId} stalled`);
    }
    /**
     * Called when worker starts processing
     */
    onActive(job) {
        this.logger.log(`▶️ Starting to process job ${job.id} for ${job.data.recipientEmail}`);
    }
    /**
     * Process each job - call n8n webhook with recipient data
     */
    async process(job) {
        var _a, _b;
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
            const webhookPayload = Object.assign(Object.assign({ email: recipientEmail, name: recipientName || '' }, recipientData), { _meta: {
                    campaignId,
                    executionId: execution.id,
                    attempt: job.attemptsMade + 1,
                    timestamp: new Date().toISOString(),
                } });
            this.logger.debug(`Calling n8n PRODUCTION webhook: ${productionWebhookUrl}`);
            // Call production webhook
            let response;
            try {
                response = await axios_1.default.post(productionWebhookUrl, webhookPayload, {
                    timeout: 30000, // 30 seconds timeout
                    headers: {
                        'Content-Type': 'application/json',
                    },
                });
                this.logger.log(`✅ Webhook success for ${recipientEmail}: ${response.status}`);
            }
            catch (webhookError) {
                if (((_a = webhookError.response) === null || _a === void 0 ? void 0 : _a.status) === 404) {
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
                    n8nExecutionId: ((_b = response.data) === null || _b === void 0 ? void 0 : _b.executionId) || null,
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
        }
        catch (error) {
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
    async handleFinalFailure(campaignId, recipientEmail, recipientName, errorMessage) {
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
    async checkCampaignCompletion(campaignId) {
        const campaign = await this.prisma.campaign.findUnique({
            where: { id: campaignId },
        });
        if (!campaign)
            return;
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
            this.logger.log(`Campaign ${campaignId} completed! Success: ${campaign.successCount}, Failed: ${campaign.failedCount}`);
        }
    }
    /**
     * Event handler for job completion
     */
    onCompleted(job) {
        this.logger.debug(`Job ${job.id} completed for ${job.data.recipientEmail}`);
    }
    /**
     * Event handler for job failure
     */
    onFailed(job, error) {
        this.logger.warn(`Job ${job.id} failed for ${job.data.recipientEmail}: ${error.message} (attempt ${job.attemptsMade})`);
    }
    /**
     * Event handler for job error
     */
    onError(error) {
        this.logger.error(`Worker error: ${error.message}`);
    }
};
exports.CampaignExecutionWorker = CampaignExecutionWorker;
__decorate([
    (0, bullmq_1.OnWorkerEvent)('ready'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onReady", null);
__decorate([
    (0, bullmq_1.OnWorkerEvent)('stalled'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onStalled", null);
__decorate([
    (0, bullmq_1.OnWorkerEvent)('active'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [bullmq_2.Job]),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onActive", null);
__decorate([
    (0, bullmq_1.OnWorkerEvent)('completed'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [bullmq_2.Job]),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onCompleted", null);
__decorate([
    (0, bullmq_1.OnWorkerEvent)('failed'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [bullmq_2.Job, Error]),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onFailed", null);
__decorate([
    (0, bullmq_1.OnWorkerEvent)('error'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Error]),
    __metadata("design:returntype", void 0)
], CampaignExecutionWorker.prototype, "onError", null);
exports.CampaignExecutionWorker = CampaignExecutionWorker = CampaignExecutionWorker_1 = __decorate([
    (0, bullmq_1.Processor)(queue_constants_1.CAMPAIGN_QUEUE, {
        concurrency: 5, // Process 5 jobs at a time
        limiter: {
            max: 100, // Max 100 jobs
            duration: 60000, // Per minute (rate limiting for email providers)
        },
    }),
    __metadata("design:paramtypes", [config_1.ConfigService])
], CampaignExecutionWorker);
