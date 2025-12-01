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
exports.CacheService = void 0;
const common_1 = require("@nestjs/common");
const cache_manager_1 = require("@nestjs/cache-manager");
let CacheService = class CacheService {
    constructor(cacheManager) {
        this.cacheManager = cacheManager;
    }
    /**
     * Get a value from cache
     */
    async get(key) {
        try {
            const value = await this.cacheManager.get(key);
            return value || null;
        }
        catch (error) {
            console.error(`Cache get error for key ${key}:`, error.message);
            return null;
        }
    }
    /**
     * Set a value in cache with optional TTL
     */
    async set(key, value, ttl) {
        try {
            if (ttl) {
                await this.cacheManager.set(key, value, ttl * 1000); // Convert to milliseconds
            }
            else {
                await this.cacheManager.set(key, value);
            }
        }
        catch (error) {
            console.error(`Cache set error for key ${key}:`, error.message);
        }
    }
    /**
     * Delete a specific key from cache
     */
    async del(key) {
        try {
            await this.cacheManager.del(key);
        }
        catch (error) {
            console.error(`Cache delete error for key ${key}:`, error.message);
        }
    }
    /**
     * Delete multiple keys matching a pattern
     */
    async delPattern(pattern) {
        var _a, _b, _c, _d;
        try {
            // For cache-manager v6 with ioredis, we need to access the underlying store
            const cacheStore = this.cacheManager;
            // Try different ways to access the Redis client
            let client = null;
            if ((_a = cacheStore.store) === null || _a === void 0 ? void 0 : _a.client) {
                client = cacheStore.store.client;
            }
            else if ((_c = (_b = cacheStore.stores) === null || _b === void 0 ? void 0 : _b[0]) === null || _c === void 0 ? void 0 : _c.client) {
                client = cacheStore.stores[0].client;
            }
            else if ((_d = cacheStore.store) === null || _d === void 0 ? void 0 : _d.getClient) {
                client = cacheStore.store.getClient();
            }
            if (client && typeof client.keys === 'function') {
                const keys = await client.keys(pattern);
                if (keys && keys.length > 0) {
                    console.log(`Deleting ${keys.length} cache keys matching pattern: ${pattern}`);
                    await Promise.all(keys.map((key) => this.cacheManager.del(key)));
                }
            }
            else {
                // Fallback: if we can't access the client for pattern matching,
                // just delete the specific key without the wildcard
                const specificKey = pattern.replace('*', '');
                console.log(`Pattern delete not available, deleting specific key: ${specificKey}`);
                await this.cacheManager.del(specificKey);
            }
        }
        catch (error) {
            console.error(`Cache delete pattern error for ${pattern}:`, error.message);
        }
    }
    /**
     * Wrap a function with caching
     */
    async wrap(key, fn, ttl) {
        try {
            // Check cache first
            const cached = await this.get(key);
            if (cached !== null) {
                return cached;
            }
            // Execute function and cache result
            const result = await fn();
            await this.set(key, result, ttl);
            return result;
        }
        catch (error) {
            console.error(`Cache wrap error for key ${key}:`, error.message);
            // If caching fails, just execute the function
            return await fn();
        }
    }
    /**
     * Generate cache key for user-specific data
     */
    getUserKey(userId, resource, id) {
        if (id) {
            return `user:${userId}:${resource}:${id}`;
        }
        return `user:${userId}:${resource}`;
    }
    /**
     * Generate cache key for specific resource
     */
    getResourceKey(resource, id) {
        return `${resource}:${id}`;
    }
    /**
     * Invalidate all cache for a user
     */
    async invalidateUser(userId) {
        await this.delPattern(`user:${userId}:*`);
    }
    /**
     * Invalidate cache for a specific user resource
     */
    async invalidateUserResource(userId, resource) {
        // First try to delete the exact key (for list endpoints)
        const exactKey = this.getUserKey(userId, resource);
        console.log(`Invalidating cache for user ${userId} resource ${resource}, key: ${exactKey}`);
        await this.del(exactKey);
        // Also try pattern-based deletion for related keys
        await this.delPattern(`user:${userId}:${resource}*`);
    }
};
exports.CacheService = CacheService;
exports.CacheService = CacheService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, common_1.Inject)(cache_manager_1.CACHE_MANAGER)),
    __metadata("design:paramtypes", [Object])
], CacheService);
