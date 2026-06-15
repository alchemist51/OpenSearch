/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_S3FIFO_GHOST_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_S3FIFO_SMALL_RATIO;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_EVICTION_TYPE;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_S3FIFO_GHOST_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_S3FIFO_SMALL_RATIO;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_SIZE_LIMIT;

/**
 * Utility class for cache initialization and configuration.
 * Contains the CacheType enum and methods for creating cache configurations.
 */
public final class CacheUtils {
    private static final Logger logger = LogManager.getLogger(CacheUtils.class);

    // Private constructor to prevent instantiation
    private CacheUtils() {}

    /**
     * Cache type enumeration with associated settings.
     */
    public enum CacheType {
        METADATA(
            "METADATA",
            METADATA_CACHE_ENABLED,
            METADATA_CACHE_SIZE_LIMIT,
            METADATA_CACHE_EVICTION_TYPE,
            METADATA_CACHE_S3FIFO_SMALL_RATIO,
            METADATA_CACHE_S3FIFO_GHOST_ENABLED
        ),

        STATISTICS(
            "STATISTICS",
            STATISTICS_CACHE_ENABLED,
            STATISTICS_CACHE_SIZE_LIMIT,
            STATISTICS_CACHE_EVICTION_TYPE,
            STATISTICS_CACHE_S3FIFO_SMALL_RATIO,
            STATISTICS_CACHE_S3FIFO_GHOST_ENABLED
        );

        private final String cacheTypeName;
        private final Setting<Boolean> enabledSetting;
        private final Setting<ByteSizeValue> sizeLimitSetting;
        private final Setting<String> evictionTypeSetting;
        private final Setting<Double> s3fifoSmallRatioSetting;
        private final Setting<Boolean> s3fifoGhostEnabledSetting;

        CacheType(
            String cacheTypeName,
            Setting<Boolean> enabledSetting,
            Setting<ByteSizeValue> sizeLimitSetting,
            Setting<String> evictionTypeSetting,
            Setting<Double> s3fifoSmallRatioSetting,
            Setting<Boolean> s3fifoGhostEnabledSetting
        ) {
            this.cacheTypeName = cacheTypeName;
            this.enabledSetting = enabledSetting;
            this.sizeLimitSetting = sizeLimitSetting;
            this.evictionTypeSetting = evictionTypeSetting;
            this.s3fifoSmallRatioSetting = s3fifoSmallRatioSetting;
            this.s3fifoGhostEnabledSetting = s3fifoGhostEnabledSetting;
        }

        public double getS3fifoSmallRatio(ClusterSettings clusterSettings) {
            return clusterSettings.get(s3fifoSmallRatioSetting);
        }

        public boolean getS3fifoGhostEnabled(ClusterSettings clusterSettings) {
            return clusterSettings.get(s3fifoGhostEnabledSetting);
        }

        public boolean isEnabled(ClusterSettings clusterSettings) {
            return clusterSettings.get(enabledSetting);
        }

        public Setting<Boolean> getEnabledSetting() {
            return enabledSetting;
        }

        public Setting<ByteSizeValue> getSizeLimitSetting() {
            return sizeLimitSetting;
        }

        public Setting<String> getEvictionTypeSetting() {
            return evictionTypeSetting;
        }

        public ByteSizeValue getSizeLimit(ClusterSettings clusterSettings) {
            return clusterSettings.get(sizeLimitSetting);
        }

        public String getEvictionType(ClusterSettings clusterSettings) {
            return clusterSettings.get(evictionTypeSetting);
        }

        public String getCacheTypeName() {
            return cacheTypeName;
        }
    }

    /**
     * Creates and configures a CacheManagerConfig pointer with all enabled caches.
     *
     * @param clusterSettings OpenSearch cluster settings containing cache configuration
     */
    public static NativeCacheManagerHandle createCacheConfig(ClusterSettings clusterSettings) {
        logger.info("Initializing cache configuration");

        long cacheManagerPtr = NativeBridge.createCustomCacheManager();
        NativeCacheManagerHandle handle = new NativeCacheManagerHandle(cacheManagerPtr);

        // Always create each cache so it can be toggled on later via the dynamic
        // datafusion.<type>.cache.enabled setting; the initial enabled flag seeds its
        // state (a disabled cache serves misses and drops writes, holding no memory).
        for (CacheType type : CacheType.values()) {
            boolean enabled = type.isEnabled(clusterSettings);
            double smallRatio = type.getS3fifoSmallRatio(clusterSettings);
            boolean ghostEnabled = type.getS3fifoGhostEnabled(clusterSettings);
            logger.info(
                "Configuring {} cache: enabled={}, size={} bytes, eviction={}, s3fifo.small_ratio={}, s3fifo.ghost_enabled={}",
                type.getCacheTypeName(),
                enabled,
                type.getSizeLimit(clusterSettings).getBytes(),
                type.getEvictionType(clusterSettings),
                smallRatio,
                ghostEnabled
            );

            NativeBridge.createCache(
                handle.getPointer(),
                type.cacheTypeName,
                type.getSizeLimit(clusterSettings).getBytes(),
                type.getEvictionType(clusterSettings),
                enabled,
                smallRatio,
                ghostEnabled
            );
        }
        logger.info("Cache configuration completed");
        return handle;
    }
}
