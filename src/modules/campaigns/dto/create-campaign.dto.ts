import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, IsDateString, IsOptional } from 'class-validator';

export class CreateCampaignDto {
  @ApiProperty({ description: 'Campaign name', example: 'Summer Sale Campaign' })
  @IsNotEmpty()
  @IsString()
  name: string;

  @ApiProperty({ description: 'Campaign description', example: 'Promotional campaign for summer products', required: false })
  @IsOptional()
  @IsString()
  description?: string;

  @ApiProperty({ description: 'Segment ID to target', example: '123e4567-e89b-12d3-a456-426614174000' })
  @IsNotEmpty()
  @IsString()
  segmentId: string;

  @ApiProperty({ description: 'Campaign start date', example: '2025-12-01T00:00:00Z' })
  @IsNotEmpty()
  @IsDateString()
  startDate: string;

  @ApiProperty({ description: 'Campaign end date', example: '2025-12-31T23:59:59Z' })
  @IsNotEmpty()
  @IsDateString()
  endDate: string;

  @ApiProperty({ 
    description: 'Scheduled date and time to start the campaign', 
    example: '2025-12-01T09:00:00Z',
    required: false 
  })
  @IsOptional()
  @IsDateString()
  scheduledAt?: string;
}
