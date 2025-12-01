import {
  Controller,
  Get,
  Post,
  Put,
  Patch,
  Delete,
  Body,
  Param,
  Query,
  UseGuards,
  Request,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiBearerAuth,
  ApiParam,
  ApiQuery,
} from '@nestjs/swagger';
import { CampaignsService } from './campaigns.service';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CreateCampaignDto } from './dto/create-campaign.dto';
import { UpdateCampaignDto } from './dto/update-campaign.dto';
import { UpdateStatusDto } from './dto/update-status.dto';
import { SaveFlowDto } from './dto/save-flow.dto';
import { CampaignResponseDto } from './dto/campaign-response.dto';

@ApiTags('Campaigns')
@ApiBearerAuth('JWT-auth')
@Controller('campaigns')
@UseGuards(JwtAuthGuard)
export class CampaignsController {
  constructor(private readonly campaignsService: CampaignsService) {}

  @Post()
  @ApiOperation({ summary: 'Create a new campaign' })
  @ApiResponse({
    status: 201,
    description: 'Campaign created successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid input data' })
  @ApiResponse({ status: 404, description: 'Segment not found' })
  async create(@Request() req: any, @Body() createCampaignDto: CreateCampaignDto) {
    return this.campaignsService.create(req.user.userId, createCampaignDto);
  }

  @Get()
  @ApiOperation({ summary: 'Get all campaigns for the current user' })
  @ApiResponse({
    status: 200,
    description: 'List of campaigns retrieved successfully',
    type: [CampaignResponseDto],
  })
  async findAll(@Request() req: any) {
    return this.campaignsService.findAll(req.user.userId);
  }

  @Get('statistics')
  @ApiOperation({ summary: 'Get campaign statistics for the current user' })
  @ApiResponse({
    status: 200,
    description: 'Campaign statistics retrieved successfully',
  })
  async getStatistics(@Request() req: any) {
    return this.campaignsService.getStatistics(req.user.userId);
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get a campaign by ID' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign retrieved successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async findOne(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.findOne(id, req.user.userId);
  }

  @Put(':id')
  @ApiOperation({ summary: 'Update a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign updated successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid input data or campaign status' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async update(
    @Param('id') id: string,
    @Request() req: any,
    @Body() updateCampaignDto: UpdateCampaignDto,
  ) {
    return this.campaignsService.update(id, req.user.userId, updateCampaignDto);
  }

  @Patch(':id/status')
  @ApiOperation({ summary: 'Update campaign status' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign status updated successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid status transition' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async updateStatus(
    @Param('id') id: string,
    @Request() req: any,
    @Body() updateStatusDto: UpdateStatusDto,
  ) {
    return this.campaignsService.updateStatus(id, req.user.userId, updateStatusDto.status);
  }

  @Post(':id/flow')
  @ApiOperation({ summary: 'Save flow builder data for a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Flow data saved successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async saveFlow(
    @Param('id') id: string,
    @Request() req: any,
    @Body() saveFlowDto: SaveFlowDto,
  ) {
    return this.campaignsService.saveFlow(id, req.user.userId, saveFlowDto);
  }

  @Patch(':id/flow')
  @ApiOperation({ summary: 'Update flow builder data for a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Flow data updated successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async updateFlow(
    @Param('id') id: string,
    @Request() req: any,
    @Body() saveFlowDto: SaveFlowDto,
  ) {
    return this.campaignsService.saveFlow(id, req.user.userId, saveFlowDto);
  }

  @Get(':id/flow')
  @ApiOperation({ summary: 'Get flow builder data for a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Flow data retrieved successfully',
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async getFlow(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.getFlow(id, req.user.userId);
  }

  @Post(':id/deploy-flow')
  @ApiOperation({ summary: 'Deploy flow to n8n workflow engine' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Flow deployed successfully to n8n',
  })
  @ApiResponse({ status: 400, description: 'Flow validation failed or deployment error' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async deployFlow(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.deployFlow(id, req.user.userId);
  }

  @Post(':id/trigger-workflow')
  @ApiOperation({ summary: 'Trigger the n8n workflow for this campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Workflow triggered successfully',
  })
  @ApiResponse({ status: 400, description: 'No workflow deployed or trigger error' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async triggerWorkflow(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.triggerWorkflow(id, req.user.userId);
  }

  @Get(':id/workflow-status')
  @ApiOperation({ summary: 'Get n8n workflow status and recent executions' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Workflow status retrieved successfully',
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async getWorkflowStatus(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.getWorkflowStatus(id, req.user.userId);
  }

  @Get('n8n/test-connection')
  @ApiOperation({ summary: 'Test connection to n8n' })
  @ApiResponse({
    status: 200,
    description: 'Connection test result',
  })
  async testN8nConnection() {
    return this.campaignsService.testN8nConnection();
  }

  @Get('queue/stats')
  @ApiOperation({ summary: 'Get campaign execution queue statistics' })
  @ApiResponse({
    status: 200,
    description: 'Queue statistics retrieved successfully',
  })
  async getQueueStats() {
    return this.campaignsService.getQueueStats();
  }

  @Get(':id/progress')
  @ApiOperation({ summary: 'Get campaign execution progress' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign progress retrieved successfully',
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async getProgress(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.getProgress(id, req.user.userId);
  }

  @Get(':id/executions')
  @ApiOperation({ summary: 'Get individual execution records for a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiQuery({ name: 'status', required: false, description: 'Filter by status (pending, processing, success, failed)' })
  @ApiQuery({ name: 'page', required: false, description: 'Page number' })
  @ApiQuery({ name: 'limit', required: false, description: 'Items per page' })
  @ApiResponse({
    status: 200,
    description: 'Execution records retrieved successfully',
  })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async getExecutions(
    @Param('id') id: string,
    @Request() req: any,
    @Query('status') status?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    return this.campaignsService.getExecutions(id, req.user.userId, {
      status,
      page: page ? parseInt(page, 10) : undefined,
      limit: limit ? parseInt(limit, 10) : undefined,
    });
  }

  @Post(':id/schedule')
  @ApiOperation({ summary: 'Schedule a campaign for execution' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign scheduled successfully',
    type: CampaignResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid scheduling request' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async scheduleCampaign(
    @Param('id') id: string,
    @Request() req: any,
    @Body() body: { scheduledAt: string },
  ) {
    return this.campaignsService.scheduleCampaign(id, req.user.userId, body.scheduledAt);
  }

  @Post(':id/run-now')
  @ApiOperation({ summary: 'Run a campaign immediately' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign started successfully',
  })
  @ApiResponse({ status: 400, description: 'Cannot run campaign' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async runCampaignNow(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.runCampaignNow(id, req.user.userId);
  }

  @Post(':id/pause')
  @ApiOperation({ summary: 'Pause a running campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign paused successfully',
  })
  @ApiResponse({ status: 400, description: 'Cannot pause campaign' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async pauseCampaign(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.pauseCampaign(id, req.user.userId);
  }

  @Post(':id/resume')
  @ApiOperation({ summary: 'Resume a paused campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Campaign resumed successfully',
  })
  @ApiResponse({ status: 400, description: 'Cannot resume campaign' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async resumeCampaign(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.resumeCampaign(id, req.user.userId);
  }

  @Post(':id/retry-failed')
  @ApiOperation({ summary: 'Retry all failed executions for a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({
    status: 200,
    description: 'Failed executions re-queued successfully',
  })
  @ApiResponse({ status: 400, description: 'No workflow deployed' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async retryFailedExecutions(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.retryFailedExecutions(id, req.user.userId);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Delete a campaign' })
  @ApiParam({ name: 'id', description: 'Campaign ID' })
  @ApiResponse({ status: 200, description: 'Campaign deleted successfully' })
  @ApiResponse({ status: 400, description: 'Cannot delete running campaign' })
  @ApiResponse({ status: 404, description: 'Campaign not found' })
  async delete(@Param('id') id: string, @Request() req: any) {
    return this.campaignsService.delete(id, req.user.userId);
  }
}
