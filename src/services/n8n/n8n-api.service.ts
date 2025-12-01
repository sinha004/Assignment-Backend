import { Injectable, HttpException, HttpStatus, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';

interface N8nWorkflow {
  id?: string;
  name: string;
  nodes: any[];
  connections: Record<string, any>;
  active?: boolean;
  settings: Record<string, any>;
  staticData: null;
}

interface N8nExecution {
  id: string;
  finished: boolean;
  mode: string;
  startedAt: string;
  stoppedAt?: string;
  workflowId: string;
  status: string;
  data?: any;
}

interface CreateWorkflowResponse {
  id: string;
  name: string;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

interface ExecuteWorkflowResponse {
  executionId: string;
  data?: any;
}

@Injectable()
export class N8nApiService {
  private readonly logger = new Logger(N8nApiService.name);
  private httpClient: AxiosInstance;
  private apiUrl: string;
  private webhookBaseUrl: string;

  constructor(private configService: ConfigService) {
    this.apiUrl = this.configService.get('N8N_API_URL') || 'http://localhost:5678/api/v1';
    this.webhookBaseUrl = this.configService.get('N8N_WEBHOOK_BASE_URL') || 'http://localhost:5678/webhook';
    
    const apiKey = this.configService.get('N8N_API_KEY');
    
    this.httpClient = axios.create({
      baseURL: this.apiUrl,
      headers: {
        'Content-Type': 'application/json',
        ...(apiKey && { 'X-N8N-API-KEY': apiKey }),
      },
      timeout: 30000,
    });

    this.logger.log(`n8n API Service initialized with URL: ${this.apiUrl}`);
  }

  /**
   * Create a new workflow in n8n
   */
  async createWorkflow(workflow: N8nWorkflow): Promise<CreateWorkflowResponse> {
    try {
      this.logger.log(`Creating workflow: ${workflow.name}`);
      
      const response = await this.httpClient.post('/workflows', workflow);
      
      this.logger.log(`Workflow created successfully: ${response.data.id}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Failed to create workflow: ${error.message}`);
      this.handleApiError(error, 'create workflow');
    }
  }

  /**
   * Update an existing workflow in n8n
   */
  async updateWorkflow(workflowId: string, workflow: N8nWorkflow): Promise<CreateWorkflowResponse> {
    try {
      this.logger.log(`Updating workflow: ${workflowId}`);
      
      // n8n API uses PUT for workflow updates, not PATCH
      const response = await this.httpClient.put(`/workflows/${workflowId}`, workflow);
      
      this.logger.log(`Workflow updated successfully: ${workflowId}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Failed to update workflow: ${error.message}`);
      this.handleApiError(error, 'update workflow');
    }
  }

  /**
   * Get a workflow by ID
   */
  async getWorkflow(workflowId: string): Promise<N8nWorkflow | null> {
    try {
      const response = await this.httpClient.get(`/workflows/${workflowId}`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      this.handleApiError(error, 'get workflow');
    }
  }

  /**
   * Delete a workflow
   */
  async deleteWorkflow(workflowId: string): Promise<void> {
    try {
      this.logger.log(`Deleting workflow: ${workflowId}`);
      await this.httpClient.delete(`/workflows/${workflowId}`);
      this.logger.log(`Workflow deleted successfully: ${workflowId}`);
    } catch (error: any) {
      this.handleApiError(error, 'delete workflow');
    }
  }

  /**
   * Activate a workflow
   */
  async activateWorkflow(workflowId: string): Promise<void> {
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
      } catch (patchError: any) {
        this.logger.debug(`PATCH activation failed, trying POST: ${patchError.message}`);
      }

      // Fallback to older API (POST /activate)
      await this.httpClient.post(`/workflows/${workflowId}/activate`);
      this.logger.log(`Workflow activated via POST: ${workflowId}`);
    } catch (error: any) {
      this.logger.error(`Failed to activate workflow ${workflowId}: ${error.message}`);
      this.handleApiError(error, 'activate workflow');
    }
  }

  /**
   * Deactivate a workflow
   */
  async deactivateWorkflow(workflowId: string): Promise<void> {
    try {
      this.logger.log(`Deactivating workflow: ${workflowId}`);
      await this.httpClient.post(`/workflows/${workflowId}/deactivate`);
      this.logger.log(`Workflow deactivated successfully: ${workflowId}`);
    } catch (error: any) {
      this.handleApiError(error, 'deactivate workflow');
    }
  }

  /**
   * Execute a workflow via test webhook (works when workflow is open in n8n editor)
   * Test webhooks use /webhook-test/ path
   */
  async executeWorkflowViaTestWebhook(webhookPath: string, data: any): Promise<any> {
    try {
      // Use test webhook path which works when workflow is open in editor
      const testWebhookUrl = `${this.webhookBaseUrl}-test/${webhookPath}`;
      this.logger.log(`Executing workflow via test webhook: ${testWebhookUrl}`);
      
      const response = await axios.post(testWebhookUrl, data, {
        timeout: 30000, // 30 seconds
      });
      
      this.logger.log(`Test webhook execution completed`);
      return response.data;
    } catch (error: any) {
      // Check if it's a timeout error - workflow might still be running
      if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
        this.logger.warn(`Test webhook request timed out, but workflow may still be running`);
        return { 
          message: 'Workflow triggered (running in background)',
          status: 'accepted'
        };
      }
      this.logger.error(`Failed to execute test webhook: ${error.message}`);
      throw new HttpException(
        'Webhook not available. Either: 1) Activate the workflow in n8n (toggle switch ON), or 2) Open the workflow in n8n editor and click "Listen for Test Event"',
        HttpStatus.BAD_REQUEST
      );
    }
  }

  /**
   * Execute a workflow via production webhook (workflow must be active)
   * Note: This uses a short timeout because workflows with Wait nodes take a long time
   */
  async executeWorkflowViaWebhook(webhookPath: string, data: any): Promise<any> {
    try {
      const webhookUrl = `${this.webhookBaseUrl}/${webhookPath}`;
      this.logger.log(`Executing workflow via webhook: ${webhookUrl}`);
      
      // Use a short timeout - we just need to confirm the webhook was received
      // The workflow will continue running in n8n even after we get the response
      const response = await axios.post(webhookUrl, data, {
        timeout: 30000, // 30 seconds to receive initial response
      });
      
      this.logger.log(`Webhook triggered successfully`);
      return response.data;
    } catch (error: any) {
      // Check if it's a timeout error - workflow might still be running
      if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
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
  async getExecution(executionId: string): Promise<N8nExecution | null> {
    try {
      const response = await this.httpClient.get(`/executions/${executionId}`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      this.handleApiError(error, 'get execution');
    }
  }

  /**
   * Get executions for a workflow
   */
  async getWorkflowExecutions(
    workflowId: string,
    options?: { limit?: number; status?: string }
  ): Promise<N8nExecution[]> {
    try {
      const params = new URLSearchParams();
      params.append('workflowId', workflowId);
      
      if (options?.limit) {
        params.append('limit', options.limit.toString());
      }
      if (options?.status) {
        params.append('status', options.status);
      }

      const response = await this.httpClient.get(`/executions?${params.toString()}`);
      return response.data.data || [];
    } catch (error: any) {
      this.handleApiError(error, 'get workflow executions');
    }
  }

  /**
   * Test n8n connection
   */
  async testConnection(): Promise<{ connected: boolean; version?: string; error?: string }> {
    try {
      // Try to get workflows list as a connection test
      await this.httpClient.get('/workflows?limit=1');
      
      return { connected: true };
    } catch (error: any) {
      return {
        connected: false,
        error: error.response?.data?.message || error.message,
      };
    }
  }

  /**
   * Get webhook URL for a path
   * @param path The webhook path (e.g., 'campaign-xxxx')
   */
  getWebhookUrl(path: string): string {
    // Don't add campaign- prefix here, it should already be in the path
    return `${this.webhookBaseUrl}/${path}`;
  }

  /**
   * Handle API errors
   */
  private handleApiError(error: any, operation: string): never {
    const message = error.response?.data?.message || error.message || 'Unknown error';
    const status = error.response?.status || HttpStatus.INTERNAL_SERVER_ERROR;

    this.logger.error(`n8n API error during ${operation}: ${message}`);

    if (status === 401 || status === 403) {
      throw new HttpException(
        'n8n authentication failed. Check your API key.',
        HttpStatus.UNAUTHORIZED
      );
    }

    if (status === 404) {
      throw new HttpException(
        `n8n resource not found during ${operation}`,
        HttpStatus.NOT_FOUND
      );
    }

    throw new HttpException(
      `n8n API error: ${message}`,
      status >= 400 && status < 600 ? status : HttpStatus.INTERNAL_SERVER_ERROR
    );
  }
}
