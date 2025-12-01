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
var N8nApiService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.N8nApiService = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
const axios_1 = __importDefault(require("axios"));
let N8nApiService = N8nApiService_1 = class N8nApiService {
    constructor(configService) {
        this.configService = configService;
        this.logger = new common_1.Logger(N8nApiService_1.name);
        this.apiUrl = this.configService.get('N8N_API_URL') || 'http://localhost:5678/api/v1';
        this.webhookBaseUrl = this.configService.get('N8N_WEBHOOK_BASE_URL') || 'http://localhost:5678/webhook';
        const apiKey = this.configService.get('N8N_API_KEY');
        this.httpClient = axios_1.default.create({
            baseURL: this.apiUrl,
            headers: Object.assign({ 'Content-Type': 'application/json' }, (apiKey && { 'X-N8N-API-KEY': apiKey })),
            timeout: 30000,
        });
        this.logger.log(`n8n API Service initialized with URL: ${this.apiUrl}`);
    }
    /**
     * Create a new workflow in n8n
     */
    async createWorkflow(workflow) {
        try {
            this.logger.log(`Creating workflow: ${workflow.name}`);
            const response = await this.httpClient.post('/workflows', workflow);
            this.logger.log(`Workflow created successfully: ${response.data.id}`);
            return response.data;
        }
        catch (error) {
            this.logger.error(`Failed to create workflow: ${error.message}`);
            this.handleApiError(error, 'create workflow');
        }
    }
    /**
     * Update an existing workflow in n8n
     */
    async updateWorkflow(workflowId, workflow) {
        try {
            this.logger.log(`Updating workflow: ${workflowId}`);
            // n8n API uses PUT for workflow updates, not PATCH
            const response = await this.httpClient.put(`/workflows/${workflowId}`, workflow);
            this.logger.log(`Workflow updated successfully: ${workflowId}`);
            return response.data;
        }
        catch (error) {
            this.logger.error(`Failed to update workflow: ${error.message}`);
            this.handleApiError(error, 'update workflow');
        }
    }
    /**
     * Get a workflow by ID
     */
    async getWorkflow(workflowId) {
        var _a;
        try {
            const response = await this.httpClient.get(`/workflows/${workflowId}`);
            return response.data;
        }
        catch (error) {
            if (((_a = error.response) === null || _a === void 0 ? void 0 : _a.status) === 404) {
                return null;
            }
            this.handleApiError(error, 'get workflow');
        }
    }
    /**
     * Delete a workflow
     */
    async deleteWorkflow(workflowId) {
        try {
            this.logger.log(`Deleting workflow: ${workflowId}`);
            await this.httpClient.delete(`/workflows/${workflowId}`);
            this.logger.log(`Workflow deleted successfully: ${workflowId}`);
        }
        catch (error) {
            this.handleApiError(error, 'delete workflow');
        }
    }
    /**
     * Activate a workflow
     */
    async activateWorkflow(workflowId) {
        try {
            this.logger.log(`Activating workflow: ${workflowId}`);
            // First, get the current workflow
            const workflow = await this.getWorkflow(workflowId);
            if (!workflow) {
                throw new Error(`Workflow ${workflowId} not found`);
            }
            // Try the new API first (PATCH with active: true)
            try {
                await this.httpClient.patch(`/workflows/${workflowId}`, { active: true });
                this.logger.log(`Workflow activated via PATCH: ${workflowId}`);
                return;
            }
            catch (patchError) {
                this.logger.debug(`PATCH activation failed, trying POST: ${patchError.message}`);
            }
            // Fallback to older API (POST /activate)
            await this.httpClient.post(`/workflows/${workflowId}/activate`);
            this.logger.log(`Workflow activated via POST: ${workflowId}`);
        }
        catch (error) {
            this.logger.error(`Failed to activate workflow ${workflowId}: ${error.message}`);
            this.handleApiError(error, 'activate workflow');
        }
    }
    /**
     * Deactivate a workflow
     */
    async deactivateWorkflow(workflowId) {
        try {
            this.logger.log(`Deactivating workflow: ${workflowId}`);
            await this.httpClient.post(`/workflows/${workflowId}/deactivate`);
            this.logger.log(`Workflow deactivated successfully: ${workflowId}`);
        }
        catch (error) {
            this.handleApiError(error, 'deactivate workflow');
        }
    }
    /**
     * Execute a workflow via test webhook (works when workflow is open in n8n editor)
     * Test webhooks use /webhook-test/ path
     */
    async executeWorkflowViaTestWebhook(webhookPath, data) {
        var _a;
        try {
            // Use test webhook path which works when workflow is open in editor
            const testWebhookUrl = `${this.webhookBaseUrl}-test/${webhookPath}`;
            this.logger.log(`Executing workflow via test webhook: ${testWebhookUrl}`);
            const response = await axios_1.default.post(testWebhookUrl, data, {
                timeout: 30000, // 30 seconds
            });
            this.logger.log(`Test webhook execution completed`);
            return response.data;
        }
        catch (error) {
            // Check if it's a timeout error - workflow might still be running
            if (error.code === 'ECONNABORTED' || ((_a = error.message) === null || _a === void 0 ? void 0 : _a.includes('timeout'))) {
                this.logger.warn(`Test webhook request timed out, but workflow may still be running`);
                return {
                    message: 'Workflow triggered (running in background)',
                    status: 'accepted'
                };
            }
            this.logger.error(`Failed to execute test webhook: ${error.message}`);
            throw new common_1.HttpException('Webhook not available. Either: 1) Activate the workflow in n8n (toggle switch ON), or 2) Open the workflow in n8n editor and click "Listen for Test Event"', common_1.HttpStatus.BAD_REQUEST);
        }
    }
    /**
     * Execute a workflow via production webhook (workflow must be active)
     * Note: This uses a short timeout because workflows with Wait nodes take a long time
     */
    async executeWorkflowViaWebhook(webhookPath, data) {
        var _a;
        try {
            const webhookUrl = `${this.webhookBaseUrl}/${webhookPath}`;
            this.logger.log(`Executing workflow via webhook: ${webhookUrl}`);
            // Use a short timeout - we just need to confirm the webhook was received
            // The workflow will continue running in n8n even after we get the response
            const response = await axios_1.default.post(webhookUrl, data, {
                timeout: 30000, // 30 seconds to receive initial response
            });
            this.logger.log(`Webhook triggered successfully`);
            return response.data;
        }
        catch (error) {
            // Check if it's a timeout error - workflow might still be running
            if (error.code === 'ECONNABORTED' || ((_a = error.message) === null || _a === void 0 ? void 0 : _a.includes('timeout'))) {
                this.logger.warn(`Webhook request timed out, but workflow may still be running`);
                return {
                    message: 'Workflow triggered (running in background)',
                    status: 'accepted'
                };
            }
            this.logger.error(`Failed to execute webhook: ${error.message}`);
            this.handleApiError(error, 'execute webhook');
        }
    }
    /**
     * Get execution details
     */
    async getExecution(executionId) {
        var _a;
        try {
            const response = await this.httpClient.get(`/executions/${executionId}`);
            return response.data;
        }
        catch (error) {
            if (((_a = error.response) === null || _a === void 0 ? void 0 : _a.status) === 404) {
                return null;
            }
            this.handleApiError(error, 'get execution');
        }
    }
    /**
     * Get executions for a workflow
     */
    async getWorkflowExecutions(workflowId, options) {
        try {
            const params = new URLSearchParams();
            params.append('workflowId', workflowId);
            if (options === null || options === void 0 ? void 0 : options.limit) {
                params.append('limit', options.limit.toString());
            }
            if (options === null || options === void 0 ? void 0 : options.status) {
                params.append('status', options.status);
            }
            const response = await this.httpClient.get(`/executions?${params.toString()}`);
            return response.data.data || [];
        }
        catch (error) {
            this.handleApiError(error, 'get workflow executions');
        }
    }
    /**
     * Test n8n connection
     */
    async testConnection() {
        var _a, _b;
        try {
            // Try to get workflows list as a connection test
            await this.httpClient.get('/workflows?limit=1');
            return { connected: true };
        }
        catch (error) {
            return {
                connected: false,
                error: ((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.message) || error.message,
            };
        }
    }
    /**
     * Get webhook URL for a path
     * @param path The webhook path (e.g., 'campaign-xxxx')
     */
    getWebhookUrl(path) {
        // Don't add campaign- prefix here, it should already be in the path
        return `${this.webhookBaseUrl}/${path}`;
    }
    /**
     * Handle API errors
     */
    handleApiError(error, operation) {
        var _a, _b, _c;
        const message = ((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.message) || error.message || 'Unknown error';
        const status = ((_c = error.response) === null || _c === void 0 ? void 0 : _c.status) || common_1.HttpStatus.INTERNAL_SERVER_ERROR;
        this.logger.error(`n8n API error during ${operation}: ${message}`);
        if (status === 401 || status === 403) {
            throw new common_1.HttpException('n8n authentication failed. Check your API key.', common_1.HttpStatus.UNAUTHORIZED);
        }
        if (status === 404) {
            throw new common_1.HttpException(`n8n resource not found during ${operation}`, common_1.HttpStatus.NOT_FOUND);
        }
        throw new common_1.HttpException(`n8n API error: ${message}`, status >= 400 && status < 600 ? status : common_1.HttpStatus.INTERNAL_SERVER_ERROR);
    }
};
exports.N8nApiService = N8nApiService;
exports.N8nApiService = N8nApiService = N8nApiService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_1.ConfigService])
], N8nApiService);
