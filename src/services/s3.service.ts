import { Injectable, InternalServerErrorException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

@Injectable()
export class S3Service {
  private s3Client: S3Client;
  private bucketName: string;

  constructor(private configService: ConfigService) {
    const region = this.configService.get<string>('Amazon_REGION');
    const accessKeyId = this.configService.get<string>('Amazon_ACCESS_KEY_ID');
    const secretAccessKey = this.configService.get<string>(
      'Amazon_SECRET_ACCESS_KEY',
    );

    const bucketName = this.configService.get<string>('Amazon_S3_BUCKET_NAME');

    if (!region || !accessKeyId || !secretAccessKey || !bucketName) {
      throw new Error('AWS configuration is missing. Please check your .env file.');
    }

    this.bucketName = bucketName;

    this.s3Client = new S3Client({
      region,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
    });
  }

  /**
   * Upload a file to S3 bucket
   * @param file The file buffer to upload
   * @param fileName The name/key for the file in S3
   * @param contentType MIME type of the file
   * @returns The S3 URL of the uploaded file
   */
  async uploadFile(
    file: Buffer,
    fileName: string,
    contentType: string,
  ): Promise<{ s3Url: string; s3Key: string }> {
    try {
      // Generate unique key with timestamp
      const timestamp = Date.now();
      const s3Key = `segments/${timestamp}-${fileName}`;

      const command = new PutObjectCommand({
        Bucket: this.bucketName,
        Key: s3Key,
        Body: file,
        ContentType: contentType,
      });

      await this.s3Client.send(command);

      // Construct S3 URL
      const region = this.configService.get<string>('Amazon_REGION');
      const s3Url = `https://${this.bucketName}.s3.${region}.amazonaws.com/${s3Key}`;

      return { s3Url, s3Key };
    } catch (error) {
      console.error('S3 upload error:', error);
      throw new InternalServerErrorException('Failed to upload file to S3');
    }
  }

  /**
   * Generate a presigned URL for temporary file access
   * @param s3Key The S3 object key
   * @param expiresIn Expiration time in seconds (default: 1 hour)
   * @returns Presigned URL
   */
  async getPresignedUrl(s3Key: string, expiresIn: number = 3600): Promise<string> {
    try {
      const command = new GetObjectCommand({
        Bucket: this.bucketName,
        Key: s3Key,
      });

      const presignedUrl = await getSignedUrl(this.s3Client, command, {
        expiresIn,
      });

      return presignedUrl;
    } catch (error) {
      console.error('Presigned URL generation error:', error);
      throw new InternalServerErrorException('Failed to generate download URL');
    }
  }

  /**
   * Delete a file from S3 bucket
   * @param s3Key The S3 object key to delete
   */
  async deleteFile(s3Key: string): Promise<void> {
    try {
      const command = new DeleteObjectCommand({
        Bucket: this.bucketName,
        Key: s3Key,
      });

      await this.s3Client.send(command);
    } catch (error) {
      console.error('S3 delete error:', error);
      throw new InternalServerErrorException('Failed to delete file from S3');
    }
  }

  /**
   * Get file URL (direct S3 URL - only works if bucket is public)
   * @param s3Key The S3 object key
   * @returns Direct S3 URL
   */
  getFileUrl(s3Key: string): string {
    const region = this.configService.get<string>('Amazon_REGION');
    return `https://${this.bucketName}.s3.${region}.amazonaws.com/${s3Key}`;
  }
}
