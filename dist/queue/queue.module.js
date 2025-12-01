"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueueModule = void 0;
const common_1 = require("@nestjs/common");
const bullmq_1 = require("@nestjs/bullmq");
const config_1 = require("@nestjs/config");
const campaign_execution_producer_1 = require("./campaign-execution.producer");
const campaign_execution_worker_1 = require("./campaign-execution.worker");
const s3_service_1 = require("../services/s3.service");
const queue_constants_1 = require("./queue.constants");
let QueueModule = class QueueModule {
};
exports.QueueModule = QueueModule;
exports.QueueModule = QueueModule = __decorate([
    (0, common_1.Module)({
        imports: [
            bullmq_1.BullModule.forRootAsync({
                imports: [config_1.ConfigModule],
                inject: [config_1.ConfigService],
                useFactory: (configService) => ({
                    connection: {
                        host: configService.get('REDIS_HOST', 'localhost'),
                        port: configService.get('REDIS_PORT', 6379),
                        password: configService.get('REDIS_PASSWORD') || undefined,
                    },
                }),
            }),
            bullmq_1.BullModule.registerQueue({
                name: queue_constants_1.CAMPAIGN_QUEUE,
                defaultJobOptions: {
                    attempts: 3,
                    backoff: {
                        type: 'exponential',
                        delay: 60000, // 1 minute
                    },
                    removeOnComplete: 100, // Keep last 100 completed jobs
                    removeOnFail: 500, // Keep last 500 failed jobs
                },
            }),
        ],
        providers: [campaign_execution_producer_1.CampaignExecutionProducer, campaign_execution_worker_1.CampaignExecutionWorker, s3_service_1.S3Service],
        exports: [campaign_execution_producer_1.CampaignExecutionProducer],
    })
], QueueModule);
