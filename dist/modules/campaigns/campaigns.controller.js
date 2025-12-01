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
Object.defineProperty(exports, "__esModule", { value: true });
exports.CampaignsController = void 0;
const common_1 = require("@nestjs/common");
const swagger_1 = require("@nestjs/swagger");
const campaigns_service_1 = require("./campaigns.service");
const jwt_auth_guard_1 = require("../auth/guards/jwt-auth.guard");
const create_campaign_dto_1 = require("./dto/create-campaign.dto");
const update_campaign_dto_1 = require("./dto/update-campaign.dto");
const update_status_dto_1 = require("./dto/update-status.dto");
const save_flow_dto_1 = require("./dto/save-flow.dto");
const campaign_response_dto_1 = require("./dto/campaign-response.dto");
let CampaignsController = class CampaignsController {
    constructor(campaignsService) {
        this.campaignsService = campaignsService;
    }
    async create(req, createCampaignDto) {
        return this.campaignsService.create(req.user.userId, createCampaignDto);
    }
    async findAll(req) {
        return this.campaignsService.findAll(req.user.userId);
    }
    async getStatistics(req) {
        return this.campaignsService.getStatistics(req.user.userId);
    }
    async findOne(id, req) {
        return this.campaignsService.findOne(id, req.user.userId);
    }
    async update(id, req, updateCampaignDto) {
        return this.campaignsService.update(id, req.user.userId, updateCampaignDto);
    }
    async updateStatus(id, req, updateStatusDto) {
        return this.campaignsService.updateStatus(id, req.user.userId, updateStatusDto.status);
    }
    async saveFlow(id, req, saveFlowDto) {
        return this.campaignsService.saveFlow(id, req.user.userId, saveFlowDto);
    }
    async updateFlow(id, req, saveFlowDto) {
        return this.campaignsService.saveFlow(id, req.user.userId, saveFlowDto);
    }
    async getFlow(id, req) {
        return this.campaignsService.getFlow(id, req.user.userId);
    }
    async deployFlow(id, req) {
        return this.campaignsService.deployFlow(id, req.user.userId);
    }
    async triggerWorkflow(id, req) {
        return this.campaignsService.triggerWorkflow(id, req.user.userId);
    }
    async getWorkflowStatus(id, req) {
        return this.campaignsService.getWorkflowStatus(id, req.user.userId);
    }
    async testN8nConnection() {
        return this.campaignsService.testN8nConnection();
    }
    async getQueueStats() {
        return this.campaignsService.getQueueStats();
    }
    async getProgress(id, req) {
        return this.campaignsService.getProgress(id, req.user.userId);
    }
    async getExecutions(id, req, status, page, limit) {
        return this.campaignsService.getExecutions(id, req.user.userId, {
            status,
            page: page ? parseInt(page, 10) : undefined,
            limit: limit ? parseInt(limit, 10) : undefined,
        });
    }
    async scheduleCampaign(id, req, body) {
        return this.campaignsService.scheduleCampaign(id, req.user.userId, body.scheduledAt);
    }
    async runCampaignNow(id, req) {
        return this.campaignsService.runCampaignNow(id, req.user.userId);
    }
    async pauseCampaign(id, req) {
        return this.campaignsService.pauseCampaign(id, req.user.userId);
    }
    async resumeCampaign(id, req) {
        return this.campaignsService.resumeCampaign(id, req.user.userId);
    }
    async retryFailedExecutions(id, req) {
        return this.campaignsService.retryFailedExecutions(id, req.user.userId);
    }
    async delete(id, req) {
        return this.campaignsService.delete(id, req.user.userId);
    }
};
exports.CampaignsController = CampaignsController;
__decorate([
    (0, common_1.Post)(),
    (0, swagger_1.ApiOperation)({ summary: 'Create a new campaign' }),
    (0, swagger_1.ApiResponse)({
        status: 201,
        description: 'Campaign created successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Invalid input data' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Segment not found' }),
    __param(0, (0, common_1.Request)()),
    __param(1, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, create_campaign_dto_1.CreateCampaignDto]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "create", null);
__decorate([
    (0, common_1.Get)(),
    (0, swagger_1.ApiOperation)({ summary: 'Get all campaigns for the current user' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'List of campaigns retrieved successfully',
        type: [campaign_response_dto_1.CampaignResponseDto],
    }),
    __param(0, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "findAll", null);
__decorate([
    (0, common_1.Get)('statistics'),
    (0, swagger_1.ApiOperation)({ summary: 'Get campaign statistics for the current user' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign statistics retrieved successfully',
    }),
    __param(0, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getStatistics", null);
__decorate([
    (0, common_1.Get)(':id'),
    (0, swagger_1.ApiOperation)({ summary: 'Get a campaign by ID' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign retrieved successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "findOne", null);
__decorate([
    (0, common_1.Put)(':id'),
    (0, swagger_1.ApiOperation)({ summary: 'Update a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign updated successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Invalid input data or campaign status' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, update_campaign_dto_1.UpdateCampaignDto]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "update", null);
__decorate([
    (0, common_1.Patch)(':id/status'),
    (0, swagger_1.ApiOperation)({ summary: 'Update campaign status' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign status updated successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Invalid status transition' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, update_status_dto_1.UpdateStatusDto]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "updateStatus", null);
__decorate([
    (0, common_1.Post)(':id/flow'),
    (0, swagger_1.ApiOperation)({ summary: 'Save flow builder data for a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Flow data saved successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, save_flow_dto_1.SaveFlowDto]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "saveFlow", null);
__decorate([
    (0, common_1.Patch)(':id/flow'),
    (0, swagger_1.ApiOperation)({ summary: 'Update flow builder data for a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Flow data updated successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, save_flow_dto_1.SaveFlowDto]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "updateFlow", null);
__decorate([
    (0, common_1.Get)(':id/flow'),
    (0, swagger_1.ApiOperation)({ summary: 'Get flow builder data for a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Flow data retrieved successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getFlow", null);
__decorate([
    (0, common_1.Post)(':id/deploy-flow'),
    (0, swagger_1.ApiOperation)({ summary: 'Deploy flow to n8n workflow engine' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Flow deployed successfully to n8n',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Flow validation failed or deployment error' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "deployFlow", null);
__decorate([
    (0, common_1.Post)(':id/trigger-workflow'),
    (0, swagger_1.ApiOperation)({ summary: 'Trigger the n8n workflow for this campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Workflow triggered successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'No workflow deployed or trigger error' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "triggerWorkflow", null);
__decorate([
    (0, common_1.Get)(':id/workflow-status'),
    (0, swagger_1.ApiOperation)({ summary: 'Get n8n workflow status and recent executions' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Workflow status retrieved successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getWorkflowStatus", null);
__decorate([
    (0, common_1.Get)('n8n/test-connection'),
    (0, swagger_1.ApiOperation)({ summary: 'Test connection to n8n' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Connection test result',
    }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "testN8nConnection", null);
__decorate([
    (0, common_1.Get)('queue/stats'),
    (0, swagger_1.ApiOperation)({ summary: 'Get campaign execution queue statistics' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Queue statistics retrieved successfully',
    }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getQueueStats", null);
__decorate([
    (0, common_1.Get)(':id/progress'),
    (0, swagger_1.ApiOperation)({ summary: 'Get campaign execution progress' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign progress retrieved successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getProgress", null);
__decorate([
    (0, common_1.Get)(':id/executions'),
    (0, swagger_1.ApiOperation)({ summary: 'Get individual execution records for a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiQuery)({ name: 'status', required: false, description: 'Filter by status (pending, processing, success, failed)' }),
    (0, swagger_1.ApiQuery)({ name: 'page', required: false, description: 'Page number' }),
    (0, swagger_1.ApiQuery)({ name: 'limit', required: false, description: 'Items per page' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Execution records retrieved successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Query)('status')),
    __param(3, (0, common_1.Query)('page')),
    __param(4, (0, common_1.Query)('limit')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, String, String, String]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "getExecutions", null);
__decorate([
    (0, common_1.Post)(':id/schedule'),
    (0, swagger_1.ApiOperation)({ summary: 'Schedule a campaign for execution' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign scheduled successfully',
        type: campaign_response_dto_1.CampaignResponseDto,
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Invalid scheduling request' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __param(2, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "scheduleCampaign", null);
__decorate([
    (0, common_1.Post)(':id/run-now'),
    (0, swagger_1.ApiOperation)({ summary: 'Run a campaign immediately' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign started successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Cannot run campaign' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "runCampaignNow", null);
__decorate([
    (0, common_1.Post)(':id/pause'),
    (0, swagger_1.ApiOperation)({ summary: 'Pause a running campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign paused successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Cannot pause campaign' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "pauseCampaign", null);
__decorate([
    (0, common_1.Post)(':id/resume'),
    (0, swagger_1.ApiOperation)({ summary: 'Resume a paused campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Campaign resumed successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Cannot resume campaign' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "resumeCampaign", null);
__decorate([
    (0, common_1.Post)(':id/retry-failed'),
    (0, swagger_1.ApiOperation)({ summary: 'Retry all failed executions for a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({
        status: 200,
        description: 'Failed executions re-queued successfully',
    }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'No workflow deployed' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "retryFailedExecutions", null);
__decorate([
    (0, common_1.Delete)(':id'),
    (0, swagger_1.ApiOperation)({ summary: 'Delete a campaign' }),
    (0, swagger_1.ApiParam)({ name: 'id', description: 'Campaign ID' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Campaign deleted successfully' }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Cannot delete running campaign' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Campaign not found' }),
    __param(0, (0, common_1.Param)('id')),
    __param(1, (0, common_1.Request)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], CampaignsController.prototype, "delete", null);
exports.CampaignsController = CampaignsController = __decorate([
    (0, swagger_1.ApiTags)('Campaigns'),
    (0, swagger_1.ApiBearerAuth)('JWT-auth'),
    (0, common_1.Controller)('campaigns'),
    (0, common_1.UseGuards)(jwt_auth_guard_1.JwtAuthGuard),
    __metadata("design:paramtypes", [campaigns_service_1.CampaignsService])
], CampaignsController);
