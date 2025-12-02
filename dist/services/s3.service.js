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
Object.defineProperty(exports, "__esModule", { value: true });
exports.S3Service = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
const client_s3_1 = require("@aws-sdk/client-s3");
const s3_request_presigner_1 = require("@aws-sdk/s3-request-presigner");
let S3Service = class S3Service {
    constructor(configService) {
        this.configService = configService;
        const region = this.configService.get('Amazon_REGION');
        const accessKeyId = this.configService.get('Amazon_ACCESS_KEY_ID');
        const secretAccessKey = this.configService.get('Amazon_SECRET_ACCESS_KEY');
        const bucketName = this.configService.get('Amazon_S3_BUCKET_NAME');
        if (!region || !accessKeyId || !secretAccessKey || !bucketName) {
            throw new Error('AWS configuration is missing. Please check your .env file.');
        }
        this.bucketName = bucketName;
        this.s3Client = new client_s3_1.S3Client({
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
    async uploadFile(file, fileName, contentType) {
        try {
            // Generate unique key with timestamp
            const timestamp = Date.now();
            const s3Key = `segments/${timestamp}-${fileName}`;
            const command = new client_s3_1.PutObjectCommand({
                Bucket: this.bucketName,
                Key: s3Key,
                Body: file,
                ContentType: contentType,
            });
            await this.s3Client.send(command);
            // Construct S3 URL
            const region = this.configService.get('Amazon_REGION');
            const s3Url = `https://${this.bucketName}.s3.${region}.amazonaws.com/${s3Key}`;
            return { s3Url, s3Key };
        }
        catch (error) {
            console.error('S3 upload error:', error);
            throw new common_1.InternalServerErrorException('Failed to upload file to S3');
        }
    }
    /**
     * Generate a presigned URL for temporary file access
     * @param s3Key The S3 object key
     * @param expiresIn Expiration time in seconds (default: 1 hour)
     * @returns Presigned URL
     */
    async getPresignedUrl(s3Key, expiresIn = 3600) {
        try {
            const command = new client_s3_1.GetObjectCommand({
                Bucket: this.bucketName,
                Key: s3Key,
            });
            const presignedUrl = await (0, s3_request_presigner_1.getSignedUrl)(this.s3Client, command, {
                expiresIn,
            });
            return presignedUrl;
        }
        catch (error) {
            console.error('Presigned URL generation error:', error);
            throw new common_1.InternalServerErrorException('Failed to generate download URL');
        }
    }
    /**
     * Delete a file from S3 bucket
     * @param s3Key The S3 object key to delete
     */
    async deleteFile(s3Key) {
        try {
            const command = new client_s3_1.DeleteObjectCommand({
                Bucket: this.bucketName,
                Key: s3Key,
            });
            await this.s3Client.send(command);
        }
        catch (error) {
            console.error('S3 delete error:', error);
            throw new common_1.InternalServerErrorException('Failed to delete file from S3');
        }
    }
    /**
     * Get file URL (direct S3 URL - only works if bucket is public)
     * @param s3Key The S3 object key
     * @returns Direct S3 URL
     */
    getFileUrl(s3Key) {
        const region = this.configService.get('Amazon_REGION');
        return `https://${this.bucketName}.s3.${region}.amazonaws.com/${s3Key}`;
    }
};
exports.S3Service = S3Service;
exports.S3Service = S3Service = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_1.ConfigService])
], S3Service);
