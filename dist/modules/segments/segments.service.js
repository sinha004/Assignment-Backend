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
Object.defineProperty(exports, "__esModule", { value: true });
exports.SegmentsService = void 0;
const common_1 = require("@nestjs/common");
const client_1 = require("@prisma/client");
const s3_service_1 = require("../../services/s3.service");
const config_1 = require("@nestjs/config");
const cache_service_1 = require("../../cache/cache.service");
const csv_parser_1 = __importDefault(require("csv-parser"));
const stream_1 = require("stream");
let SegmentsService = class SegmentsService {
    constructor(s3Service, configService, cacheService) {
        this.s3Service = s3Service;
        this.configService = configService;
        this.cacheService = cacheService;
        this.prisma = new client_1.PrismaClient();
    }
    /**
     * Upload a CSV segment file
     */
    async uploadSegment(userId, file, segmentName) {
        // Validate file type
        if (!file.originalname.toLowerCase().endsWith('.csv')) {
            throw new common_1.BadRequestException('Only CSV files are allowed');
        }
        // Validate file size
        const maxSizeMB = this.configService.get('MAX_FILE_SIZE_MB') || 10;
        const maxSizeBytes = maxSizeMB * 1024 * 1024;
        if (file.size > maxSizeBytes) {
            throw new common_1.BadRequestException(`File size exceeds ${maxSizeMB}MB limit`);
        }
        // Parse and validate CSV
        const { isValid, rowCount, errors } = await this.validateCsv(file.buffer);
        if (!isValid) {
            throw new common_1.BadRequestException(`CSV validation failed: ${errors.join(', ')}`);
        }
        // Upload to S3
        const { s3Url, s3Key } = await this.s3Service.uploadFile(file.buffer, file.originalname, 'text/csv');
        // Use custom name if provided, otherwise use filename without extension
        const finalName = (segmentName === null || segmentName === void 0 ? void 0 : segmentName.trim()) || file.originalname.replace('.csv', '');
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
    async validateCsv(buffer) {
        return new Promise((resolve) => {
            const errors = [];
            let rowCount = 0;
            let headerChecked = false;
            const stream = stream_1.Readable.from(buffer);
            stream
                .pipe((0, csv_parser_1.default)())
                .on('headers', (headers) => {
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
                .on('data', (row) => {
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
                .on('error', (error) => {
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
    isValidEmail(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    }
    /**
     * Get all segments for a user
     */
    async findAllByUser(userId) {
        const cacheKey = this.cacheService.getUserKey(userId, 'segments');
        return this.cacheService.wrap(cacheKey, async () => {
            return this.prisma.segment.findMany({
                where: {
                    userId,
                    status: 'active',
                },
                orderBy: {
                    uploadedAt: 'desc',
                },
            });
        }, 300);
    }
    /**
     * Get a single segment by ID
     */
    async findOne(segmentId, userId) {
        const cacheKey = this.cacheService.getResourceKey('segment', segmentId);
        return this.cacheService.wrap(cacheKey, async () => {
            const segment = await this.prisma.segment.findUnique({
                where: { id: segmentId },
            });
            if (!segment) {
                throw new common_1.NotFoundException('Segment not found');
            }
            if (segment.userId !== userId) {
                throw new common_1.ForbiddenException('Access denied to this segment');
            }
            return segment;
        }, 600);
    }
    /**
     * Get presigned download URL for a segment
     */
    async getDownloadUrl(segmentId, userId) {
        const segment = await this.findOne(segmentId, userId);
        return this.s3Service.getPresignedUrl(segment.s3Key);
    }
    /**
     * Delete a segment
     */
    async deleteSegment(segmentId, userId) {
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
};
exports.SegmentsService = SegmentsService;
exports.SegmentsService = SegmentsService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [s3_service_1.S3Service,
        config_1.ConfigService,
        cache_service_1.CacheService])
], SegmentsService);
