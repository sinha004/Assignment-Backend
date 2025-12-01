import { Injectable, Inject } from '@nestjs/common';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { Cache } from 'cache-manager';

@Injectable()
export class CacheService {
  constructor(@Inject(CACHE_MANAGER) private cacheManager: Cache) {}

  /**
   * Get a value from cache
   */
  async get<T>(key: string): Promise<T | null> {
    try {
      const value = await this.cacheManager.get<T>(key);
      return value || null;
    } catch (error) {
      console.error(`Cache get error for key ${key}:`, (error as Error).message);
      return null;
    }
  }

  /**
   * Set a value in cache with optional TTL
   */
  async set<T>(key: string, value: T, ttl?: number): Promise<void> {
    try {
      if (ttl) {
        await this.cacheManager.set(key, value, ttl * 1000); // Convert to milliseconds
      } else {
        await this.cacheManager.set(key, value);
      }
    } catch (error) {
      console.error(`Cache set error for key ${key}:`, (error as Error).message);
    }
  }

  /**
   * Delete a specific key from cache
   */
  async del(key: string): Promise<void> {
    try {
      await this.cacheManager.del(key);
    } catch (error) {
      console.error(`Cache delete error for key ${key}:`, (error as Error).message);
    }
  }

  /**
   * Delete multiple keys matching a pattern
   */
  async delPattern(pattern: string): Promise<void> {
    try {
      // For cache-manager v6 with ioredis, we need to access the underlying store
      const cacheStore = this.cacheManager as any;
      
      // Try different ways to access the Redis client
      let client = null;
      
      if (cacheStore.store?.client) {
        client = cacheStore.store.client;
      } else if (cacheStore.stores?.[0]?.client) {
        client = cacheStore.stores[0].client;
      } else if (cacheStore.store?.getClient) {
        client = cacheStore.store.getClient();
      }
      
      if (client && typeof client.keys === 'function') {
        const keys = await client.keys(pattern);
        if (keys && keys.length > 0) {
          console.log(`Deleting ${keys.length} cache keys matching pattern: ${pattern}`);
          await Promise.all(keys.map((key: string) => this.cacheManager.del(key)));
        }
      } else {
        // Fallback: if we can't access the client for pattern matching,
        // just delete the specific key without the wildcard
        const specificKey = pattern.replace('*', '');
        console.log(`Pattern delete not available, deleting specific key: ${specificKey}`);
        await this.cacheManager.del(specificKey);
      }
    } catch (error) {
      console.error(`Cache delete pattern error for ${pattern}:`, (error as Error).message);
    }
  }

  /**
   * Wrap a function with caching
   */
  async wrap<T>(
    key: string,
    fn: () => Promise<T>,
    ttl?: number,
  ): Promise<T> {
    try {
      // Check cache first
      const cached = await this.get<T>(key);
      if (cached !== null) {
        return cached;
      }

      // Execute function and cache result
      const result = await fn();
      await this.set(key, result, ttl);
      return result;
    } catch (error) {
      console.error(`Cache wrap error for key ${key}:`, (error as Error).message);
      // If caching fails, just execute the function
      return await fn();
    }
  }

  /**
   * Generate cache key for user-specific data
   */
  getUserKey(userId: number, resource: string, id?: string): string {
    if (id) {
      return `user:${userId}:${resource}:${id}`;
    }
    return `user:${userId}:${resource}`;
  }

  /**
   * Generate cache key for specific resource
   */
  getResourceKey(resource: string, id: string): string {
    return `${resource}:${id}`;
  }

  /**
   * Invalidate all cache for a user
   */
  async invalidateUser(userId: number): Promise<void> {
    await this.delPattern(`user:${userId}:*`);
  }

  /**
   * Invalidate cache for a specific user resource
   */
  async invalidateUserResource(userId: number, resource: string): Promise<void> {
    // First try to delete the exact key (for list endpoints)
    const exactKey = this.getUserKey(userId, resource);
    console.log(`Invalidating cache for user ${userId} resource ${resource}, key: ${exactKey}`);
    await this.del(exactKey);
    
    // Also try pattern-based deletion for related keys
    await this.delPattern(`user:${userId}:${resource}*`);
  }
}
