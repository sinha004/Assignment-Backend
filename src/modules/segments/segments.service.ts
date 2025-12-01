import {
  Injectable,
  BadRequestException,
  NotFoundException,
  ForbiddenException,
} from '@nestjs/common';
import { PrismaClient } from '@prisma/client';
import { S3Service } from '../../services/s3.service';
import { ConfigService } from '@nestjs/config';
import { CacheService } from '../../cache/cache.service';
import csv from 'csv-parser';
import { Readable } from 'stream';

@Injectable()
export class SegmentsService {
  private prisma: PrismaClient;

  constructor(
    private s3Service: S3Service,
    private configService: ConfigService,
    private cacheService: CacheService,
  ) {
    this.prisma = new PrismaClient();
  }

  /**
   * Upload a CSV segment file
   */
  async uploadSegment(
    userId: number,
    file: Express.Multer.File,
    segmentName?: string,
  ) {
    // Validate file type
    if (!file.originalname.toLowerCase().endsWith('.csv')) {
      throw new BadRequestException('Only CSV files are allowed');
    }

    // Validate file size
    const maxSizeMB = this.configService.get<number>('MAX_FILE_SIZE_MB') || 10;
    const maxSizeBytes = maxSizeMB * 1024 * 1024;
    if (file.size > maxSizeBytes) {
      throw new BadRequestException(
        `File size exceeds ${maxSizeMB}MB limit`,
      );
    }

    // Parse and validate CSV
    const { isValid, rowCount, errors } = await this.validateCsv(file.buffer);
    if (!isValid) {
      throw new BadRequestException(`CSV validation failed: ${errors.join(', ')}`);
    }

    // Upload to S3
    const { s3Url, s3Key } = await this.s3Service.uploadFile(
      file.buffer,
      file.originalname,
      'text/csv',
    );

    // Use custom name if provided, otherwise use filename without extension
    const finalName = segmentName?.trim() || file.originalname.replace('.csv', '');

    // Save to database
    const segment = await this.prisma.segment.create({
      data: {
        userId,
        name: finalName,
        s3Url,
        s3Key,
        fileName: file.originalname,
        fileSize: file.size,
        totalRecords: rowCount,
        status: 'active',
      },
    });

    // Invalidate user's segments cache
    console.log(`Segment created: ${segment.id}, name: ${segment.name}, invalidating cache for user ${userId}`);
    await this.cacheService.invalidateUserResource(userId, 'segments');

    return segment;
  }

  /**
   * Validate CSV file format and contents
   */
  private async validateCsv(
    buffer: Buffer,
  ): Promise<{ isValid: boolean; rowCount: number; errors: string[] }> {
    return new Promise((resolve) => {
      const errors: string[] = [];
      let rowCount = 0;
      let headerChecked = false;

      const stream = Readable.from(buffer);

      stream
        .pipe(csv())
        .on('headers', (headers: string[]) => {
          headerChecked = true;
          // Check for required columns
          const requiredColumns = ['name', 'email'];
          const lowerHeaders = headers.map((h) => h.toLowerCase().trim());

          for (const col of requiredColumns) {
            if (!lowerHeaders.includes(col)) {
              errors.push(`Missing required column: ${col}`);
            }
          }
        })
        .on('data', (row: any) => {
          rowCount++;

          // Validate email format in each row
          const email = row.email || row.Email || row.EMAIL;
          if (email && !this.isValidEmail(email)) {
            errors.push(`Invalid email at row ${rowCount}: ${email}`);
          }

          // Stop processing if too many errors
          if (errors.length > 10) {
            stream.destroy();
          }
        })
        .on('end', () => {
          resolve({
            isValid: errors.length === 0 && rowCount > 0,
            rowCount,
            errors,
          });
        })
        .on('error', (error: any) => {
          errors.push(`CSV parsing error: ${error.message}`);
          resolve({
            isValid: false,
            rowCount: 0,
            errors,
          });
        });
    });
  }

  /**
   * Simple email validation
   */
  private isValidEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Get all segments for a user
   */
  async findAllByUser(userId: number) {
    const cacheKey = this.cacheService.getUserKey(userId, 'segments');
    
    return this.cacheService.wrap(
      cacheKey,
      async () => {
        return this.prisma.segment.findMany({
          where: {
            userId,
            status: 'active',
          },
          orderBy: {
            uploadedAt: 'desc',
          },
        });
      },
      300, // 5 minutes TTL
    );
  }

  /**
   * Get a single segment by ID
   */
  async findOne(segmentId: string, userId: number) {
    const cacheKey = this.cacheService.getResourceKey('segment', segmentId);
    
    return this.cacheService.wrap(
      cacheKey,
      async () => {
        const segment = await this.prisma.segment.findUnique({
          where: { id: segmentId },
        });

        if (!segment) {
          throw new NotFoundException('Segment not found');
        }

        if (segment.userId !== userId) {
          throw new ForbiddenException('Access denied to this segment');
        }

        return segment;
      },
      600, // 10 minutes TTL
    );
  }

  /**
   * Get presigned download URL for a segment
   */
  async getDownloadUrl(segmentId: string, userId: number): Promise<string> {
    const segment = await this.findOne(segmentId, userId);
    return this.s3Service.getPresignedUrl(segment.s3Key);
  }

  /**
   * Delete a segment
   */
  async deleteSegment(segmentId: string, userId: number) {
    const segment = await this.findOne(segmentId, userId);

    // Delete from S3
    await this.s3Service.deleteFile(segment.s3Key);

    // Soft delete in database
    await this.prisma.segment.update({
      where: { id: segmentId },
      data: { status: 'deleted' },
    });

    // Invalidate cache
    await this.cacheService.del(this.cacheService.getResourceKey('segment', segmentId));
    await this.cacheService.invalidateUserResource(userId, 'segments');

    return { message: 'Segment deleted successfully' };
  }
}
