/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest.ClusterDiffManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.UploadedMetadataResults;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.clusterUUIDContainer;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTINGS_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTING_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteIndexMetadataManager.INDEX_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteIndexMetadataManager.INDEX_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteManifestManager.MANIFEST_PATH_TOKEN;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {
    public static final int RETAINED_MANIFESTS = 10;
    public static final int SKIP_CLEANUP_STATE_CHANGES = 10;

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );

    /**
     * Setting to specify the interval to do run stale file cleanup job
     */
    public static final Setting<TimeValue> REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.cleanup_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMillis(-1),
        Property.NodeScope,
        Property.Dynamic
    );

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private final AtomicBoolean deleteStaleMetadataRunning = new AtomicBoolean(false);
    private final RemotePersistenceStats remoteStateStats;
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private TimeValue staleFileCleanupInterval;
    private AsyncStaleFileDeletion staleFileDeletionTask;
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;
    private String latestClusterName;
    private String latestClusterUUID;
    private long lastCleanupAttemptState;
    private boolean isClusterManagerNode;

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        this.clusterSettings = clusterSettings;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.remoteStateStats = new RemotePersistenceStats();
        this.staleFileCleanupInterval = clusterSettings.get(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING, this::updateCleanupInterval);
        this.lastCleanupAttemptState = 0;
        this.isClusterManagerNode = DiscoveryNode.isClusterManagerNode(settings);
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataManifest writeFullMetadata(ClusterState clusterState, String previousClusterUUID) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        UploadedMetadataResults uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values()),
            clusterState.metadata().customs(),
            true,
            true,
            true,
            true,
            true
        );
        final ClusterMetadataManifest manifest = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults.uploadedIndexMetadata,
            previousClusterUUID,
            uploadedMetadataResults.uploadedCoordinationMetadata,
            uploadedMetadataResults.uploadedSettingsMetadata,
            uploadedMetadataResults.uploadedTemplatesMetadata,
            uploadedMetadataResults.uploadedCustomMetadataMap,
            uploadedMetadataResults.uploadedDiscoveryNodes,
            uploadedMetadataResults.uploadedClusterBlocks,
            new ClusterMetadataManifest.ClusterDiffManifest(clusterState, ClusterState.EMPTY_STATE),
            false
        );
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        } else {
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices and global metadata",
                durationMillis,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        }
        return manifest;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return The uploaded ClusterMetadataManifest file
     */
    @Nullable
    public ClusterMetadataManifest writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();

        // Write Global Metadata
        final boolean updateGlobalMetadata = Metadata.isGlobalStateEquals(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;

        final boolean updateCoordinationMetadata = Metadata.isCoordinationMetadataEqual(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        final boolean updateSettingsMetadata = Metadata.isSettingsMetadataEqual(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        final boolean updateTemplatesMetadata = Metadata.isTemplatesMetadataEqual(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        // ToDo: check if these needs to be updated or not
        final boolean updateDiscoveryNodes = clusterState.getNodes().delta(previousClusterState.getNodes()).hasChanges();
        final boolean updateClusterBlocks = clusterState.blocks().equals(previousClusterState.blocks());

        final Map<String, UploadedMetadataAttribute> previousStateCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        final Map<String, Metadata.Custom> customsToUpload = remoteGlobalMetadataManager.getUpdatedCustoms(
            clusterState,
            previousClusterState
        );
        final Map<String, UploadedMetadataAttribute> allUploadedCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        for (final String custom : clusterState.metadata().customs().keySet()) {
            // remove all the customs which are present currently
            previousStateCustomMap.remove(custom);
        }

        // Write Index Metadata
        final Map<String, Long> previousStateIndexMetadataVersionByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : previousClusterState.metadata().indices().values()) {
            previousStateIndexMetadataVersionByName.put(indexMetadata.getIndex().getName(), indexMetadata.getVersion());
        }

        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();

        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            final Long previousVersion = previousStateIndexMetadataVersionByName.get(indexMetadata.getIndex().getName());
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.debug(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                toUpload.add(indexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            previousStateIndexMetadataVersionByName.remove(indexMetadata.getIndex().getName());
        }
        UploadedMetadataResults uploadedMetadataResults;
        boolean firstUpload = !previousManifest.hasMetadataAttributesFiles();
        // For migration case from codec V0 or V1 to V2, we have added null check on metadata attribute files,
        // If file is empty and codec is 1 then write global metadata.
        if (firstUpload) {
            uploadedMetadataResults = writeMetadataInParallel(
                clusterState,
                toUpload,
                clusterState.metadata().customs(),
                true,
                true,
                true,
                true,
                true
            );
        } else {
            uploadedMetadataResults = writeMetadataInParallel(
                clusterState,
                toUpload,
                customsToUpload,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                updateDiscoveryNodes,
                updateClusterBlocks
            );
        }

        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        // remove the data for removed custom/indices
        previousStateCustomMap.keySet().forEach(allUploadedCustomMap::remove);
        previousStateIndexMetadataVersionByName.keySet().forEach(allUploadedIndexMetadata::remove);
        final ClusterMetadataManifest manifest = remoteManifestManager.uploadManifest(
            clusterState,
            new ArrayList<>(allUploadedIndexMetadata.values()),
            previousManifest.getPreviousClusterUUID(),
            firstUpload || updateCoordinationMetadata
                ? uploadedMetadataResults.uploadedCoordinationMetadata
                : previousManifest.getCoordinationMetadata(),
            firstUpload || updateSettingsMetadata
                ? uploadedMetadataResults.uploadedSettingsMetadata
                : previousManifest.getSettingsMetadata(),
            firstUpload || updateTemplatesMetadata
                ? uploadedMetadataResults.uploadedTemplatesMetadata
                : previousManifest.getTemplatesMetadata(),
            firstUpload || !customsToUpload.isEmpty() ? allUploadedCustomMap : previousManifest.getCustomMetadataMap(),
            firstUpload || updateDiscoveryNodes ? uploadedMetadataResults.uploadedDiscoveryNodes : previousManifest.getDiscoveryNodesMetadata(),
            firstUpload || updateClusterBlocks ? uploadedMetadataResults.uploadedClusterBlocks : previousManifest.getClusterBlocksMetadata(),
            new ClusterMetadataManifest.ClusterDiffManifest(clusterState, previousClusterState),
            false
        );
        this.latestClusterName = clusterState.getClusterName().value();
        this.latestClusterUUID = clusterState.metadata().clusterUUID();

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsToUpload.size()
            );
        } else {
            logger.info(
                "writing cluster state for version [{}] took [{}ms]; "
                    + "wrote metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                manifest.getStateVersion(),
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsToUpload.size()
            );
        }
        return manifest;
    }

    private UploadedMetadataResults writeMetadataInParallel(
        ClusterState clusterState,
        List<IndexMetadata> indexToUpload,
        Map<String, Metadata.Custom> customToUpload,
        boolean uploadCoordinationMetadata,
        boolean uploadSettingsMetadata,
        boolean uploadTemplateMetadata,
        boolean uploadDiscoveryNodes,
        boolean uploadClusterBlock
    ) throws IOException {
        int totalUploadTasks = indexToUpload.size() + customToUpload.size() + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata
            ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0) + (uploadDiscoveryNodes  ? 1 : 0) + (uploadClusterBlock ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        Map<String, CheckedRunnable<IOException>> uploadTasks = new HashMap<>(totalUploadTasks);
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new HashMap<>(totalUploadTasks);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(
            ActionListener.wrap((ClusterMetadataManifest.UploadedMetadata uploadedMetadata) -> {
                logger.trace(String.format(Locale.ROOT, "Metadata component %s uploaded successfully.", uploadedMetadata.getComponent()));
                results.put(uploadedMetadata.getComponent(), uploadedMetadata);
            }, ex -> {
                logger.error(
                    () -> new ParameterizedMessage("Exception during transfer of Metadata Fragment to Remote {}", ex.getMessage()),
                    ex
                );
                exceptionList.add(ex);
            }),
            latch
        );

        if (uploadSettingsMetadata) {
            uploadTasks.put(
                SETTING_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    SETTING_METADATA,
                    SETTINGS_METADATA_FORMAT,
                    clusterState.metadata().persistentSettings(),
                    listener
                )
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.put(
                COORDINATION_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    COORDINATION_METADATA,
                    COORDINATION_METADATA_FORMAT,
                    clusterState.metadata().coordinationMetadata(),
                    listener
                )
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.put(
                TEMPLATES_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    TEMPLATES_METADATA,
                    TEMPLATES_METADATA_FORMAT,
                    clusterState.metadata().templatesMetadata(),
                    listener
                )
            );
        }
        if (uploadDiscoveryNodes) {
            uploadTasks.put(
                DISCOVERY_NODES,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    clusterState,
                    DISCOVERY_NODES,
                    DISCOVERY_NODES_FORMAT,
                    clusterState.nodes(),
                    listener
                )
            );
        }
        if (uploadClusterBlock) {
            uploadTasks.put(
                CLUSTER_BLOCKS,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    clusterState,
                    CLUSTER_BLOCKS,
                    CLUSTER_BLOCKS_FORMAT,
                    clusterState.blocks(),
                    listener
                )
            );
        }
        customToUpload.forEach((key, value) -> {
            String customComponent = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, key);
            uploadTasks.put(
                customComponent,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    customComponent,
                    CUSTOM_METADATA_FORMAT,
                    value,
                    listener
                )
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.put(
                indexMetadata.getIndexName(),
                remoteIndexMetadataManager.getIndexMetadataAsyncAction(clusterState, indexMetadata, listener)
            );
        });

        // start async upload of all required metadata files
        for (CheckedRunnable<IOException> uploadTask : uploadTasks.values()) {
            uploadTask.run();
        }

        try {
            if (latch.await(remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                // TODO: We should add metrics where transfer is timing out. [Issue: #10687]
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of following metadata to complete - %s",
                        String.join(", ", uploadTasks.keySet())
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for transfer of metadata to complete - %s",
                    String.join(", ", uploadTasks.keySet())
                ),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Exception during transfer of following metadata to Remote - %s",
                    String.join(", ", uploadTasks.keySet())
                )
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        UploadedMetadataResults response = new UploadedMetadataResults();
        results.forEach((name, uploadedMetadata) -> {
            if (uploadedMetadata.getClass().equals(UploadedIndexMetadata.class)) {
                response.uploadedIndexMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (uploadedMetadata.getComponent().contains(CUSTOM_METADATA)) {
                // component name for custom metadata will look like custom--<metadata-attribute>
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (COORDINATION_METADATA.equals(uploadedMetadata.getComponent())) {
                response.uploadedCoordinationMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (SETTING_METADATA.equals(uploadedMetadata.getComponent())) {
                response.uploadedSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (TEMPLATES_METADATA.equals(uploadedMetadata.getComponent())) {
                response.uploadedTemplatesMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (DISCOVERY_NODES.equals(uploadedMetadata.getComponent())) {
                response.uploadedDiscoveryNodes = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (CLUSTER_BLOCKS.equals(uploadedMetadata.getComponent())) {
                response.uploadedClusterBlocks = (UploadedMetadataAttribute) uploadedMetadata;
            } else {
                throw new IllegalStateException("Unexpected metadata component " + uploadedMetadata.getComponent());
            }
        });
        return response;
    }

    public TimeValue getStaleFileCleanupInterval() {
        return this.staleFileCleanupInterval;
    }

    AsyncStaleFileDeletion getStaleFileDeletionTask() { // for testing
        return this.staleFileDeletionTask;
    }

    private void updateCleanupInterval(TimeValue updatedInterval) {
        if (!isClusterManagerNode) return;
        this.staleFileCleanupInterval = updatedInterval;
        logger.info("updated remote state cleanup interval to {}", updatedInterval);
        // After updating the interval, we need to close the current task and create a new one which will run with updated interval
        if (!this.staleFileDeletionTask.getInterval().equals(updatedInterval)) {
            this.staleFileDeletionTask.setInterval(updatedInterval);
        }
    }

    private void cleanUpStaleFiles() {
        long cleanUpAttemptState = remoteStateStats.getSuccessCount();
        if (cleanUpAttemptState - lastCleanupAttemptState > SKIP_CLEANUP_STATE_CHANGES
            && this.latestClusterName != null
            && this.latestClusterUUID != null) {
            logger.info(
                "Cleaning up stale remote state files for cluster [{}] with uuid [{}]. Last clean was done before {} updates",
                this.latestClusterName,
                this.latestClusterUUID,
                cleanUpAttemptState - lastCleanupAttemptState
            );
            deleteStaleClusterMetadata(this.latestClusterName, this.latestClusterUUID, RETAINED_MANIFESTS);
            lastCleanupAttemptState = cleanUpAttemptState;
        } else {
            logger.info(
                "Skipping cleanup of stale remote state files for cluster [{}] with uuid [{}]. Last clean was done before {} updates, which is less than threshold {}",
                this.latestClusterName,
                this.latestClusterUUID,
                cleanUpAttemptState - lastCleanupAttemptState,
                SKIP_CLEANUP_STATE_CHANGES
            );
        }
    }

    @Nullable
    public ClusterMetadataManifest markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        assert clusterState != null : "Last accepted cluster state is not set";
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        ClusterMetadataManifest committedManifest = remoteManifestManager.uploadManifest(
            clusterState,
            previousManifest.getIndices(),
            previousManifest.getPreviousClusterUUID(),
            previousManifest.getCoordinationMetadata(),
            previousManifest.getSettingsMetadata(),
            previousManifest.getTemplatesMetadata(),
            previousManifest.getCustomMetadataMap(),
            previousManifest.getDiscoveryNodesMetadata(),
            previousManifest.getClusterBlocksMetadata(),
            previousManifest.getDiffManifest(),
            true
        );
        deleteStaleClusterUUIDs(clusterState, committedManifest);
        return committedManifest;
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        return remoteManifestManager.getLatestClusterMetadataManifest(clusterName, clusterUUID);
    }

    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(String clusterName, String clusterUUID, long term, long version) {
        return remoteManifestManager.getClusterMetadataManifestByTermVersion(clusterName, clusterUUID, term, version);
    }

    @Override
    public void close() throws IOException {
        if (staleFileDeletionTask != null) {
            staleFileDeletionTask.close();
        }
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(blobStoreRepository, clusterSettings);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(blobStoreRepository, clusterSettings);
        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(blobStoreRepository);
        remoteManifestManager = new RemoteManifestManager(blobStoreRepository, clusterSettings, nodeId);
        if (isClusterManagerNode) {
            staleFileDeletionTask = new AsyncStaleFileDeletion(this);
        }
    }

    private String fetchPreviousClusterUUID(String clusterName, String clusterUUID) {
        final Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (!latestManifest.isPresent()) {
            final String previousClusterUUID = getLastKnownUUIDFromRemote(clusterName);
            assert !clusterUUID.equals(previousClusterUUID) : "Last cluster UUID is same current cluster UUID";
            return previousClusterUUID;
        }
        return latestManifest.get().getPreviousClusterUUID();
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    /**
     * Fetch latest ClusterState from remote, including global metadata, index metadata and cluster state version
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@link IndexMetadata}
     */
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID) {
        start();
        Optional<ClusterMetadataManifest> clusterMetadataManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (clusterMetadataManifest.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Latest cluster metadata manifest is not present for the provided clusterUUID: %s", clusterUUID)
            );
        }

        return getClusterStateForManifest(clusterName, clusterMetadataManifest.get());
    }

    public ClusterState getClusterStateForManifest(String clusterName, ClusterMetadataManifest manifest) {
        // todo make this async
        // Fetch Global Metadata
        Metadata globalMetadata = remoteGlobalMetadataManager.getGlobalMetadata(clusterName, manifest.getClusterUUID(), manifest);

        // Fetch Index Metadata
        Map<String, IndexMetadata> indices = remoteIndexMetadataManager.getIndexMetadataMap(
            clusterName,
            manifest.getClusterUUID(),
            manifest
        );

        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        indices.values().forEach(indexMetadata -> { indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata); });

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .version(manifest.getStateVersion())
            .metadata(Metadata.builder(globalMetadata).indices(indexMetadataMap).build())
            .build();
    }

    public ClusterState getClusterStateUsingDiff(String clusterName, ClusterMetadataManifest manifest, ClusterState previousState) {
        assert manifest.getDiffManifest() != null;
        ClusterDiffManifest diff = manifest.getDiffManifest();
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(previousState);
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());

        for (String index:diff.getIndicesUpdated()) {
            //todo optimize below iteration
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndices().stream().filter(idx -> idx.getIndexName().equals(index)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            IndexMetadata indexMetadata = remoteIndexMetadataManager.getIndexMetadata(clusterName, manifest.getClusterUUID(), uploadedIndexMetadataOptional.get());
            metadataBuilder.put(indexMetadata, false);
        }
        for (String index:diff.getIndicesDeleted()) {
            metadataBuilder.remove(index);
        }
        if (diff.isCoordinationMetadataUpdated()) {
            CoordinationMetadata coordinationMetadata = remoteGlobalMetadataManager.getCoordinationMetadata(clusterName, manifest.getClusterUUID(), manifest.getCoordinationMetadata().getUploadedFilename());
            metadataBuilder.coordinationMetadata(coordinationMetadata);
        }
        if (diff.isSettingsMetadataUpdated()) {
            Settings settings = remoteGlobalMetadataManager.getSettingsMetadata(clusterName, manifest.getClusterUUID(), manifest.getSettingsMetadata().getUploadedFilename());
            metadataBuilder.persistentSettings(settings);
        }
        if (diff.isTemplatesMetadataUpdated()) {
            TemplatesMetadata templatesMetadata = remoteGlobalMetadataManager.getTemplatesMetadata(clusterName, manifest.getClusterUUID(), manifest.getTemplatesMetadata().getUploadedFilename());
            metadataBuilder.templates(templatesMetadata);
        }
        for (String customType : diff.getCustomMetadataUpdated().keySet()) {
            Metadata.Custom custom = remoteGlobalMetadataManager.getCustomsMetadata(clusterName, manifest.getClusterUUID(), manifest.getCustomMetadataMap().get(customType).getUploadedFilename(), customType);
            metadataBuilder.putCustom(customType, custom);
        }
        return clusterStateBuilder.metadata(metadataBuilder).build();
    }

    /**
     * Fetch the previous cluster UUIDs from remote state store and return the most recent valid cluster UUID
     *
     * @param clusterName The cluster name for which previous cluster UUID is to be fetched
     * @return Last valid cluster UUID
     */
    public String getLastKnownUUIDFromRemote(String clusterName) {
        try {
            Set<String> clusterUUIDs = getAllClusterUUIDs(clusterName);
            Map<String, ClusterMetadataManifest> latestManifests = remoteManifestManager.getLatestManifestForAllClusterUUIDs(
                clusterName,
                clusterUUIDs
            );
            List<String> validChain = createClusterChain(latestManifests, clusterName);
            if (validChain.isEmpty()) {
                return ClusterState.UNKNOWN_UUID;
            }
            return validChain.get(0);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while fetching previous UUIDs from remote store for cluster name: %s", clusterName),
                e
            );
        }
    }

    private Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(blobStoreRepository, clusterName).children();
        if (clusterUUIDMetadata == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(clusterUUIDMetadata.keySet());
    }

    /**
     * This method creates a valid cluster UUID chain.
     *
     * @param manifestsByClusterUUID Map of latest ClusterMetadataManifest for every cluster UUID
     * @return List of cluster UUIDs. The first element is the most recent cluster UUID in the chain
     */
    private List<String> createClusterChain(final Map<String, ClusterMetadataManifest> manifestsByClusterUUID, final String clusterName) {
        final List<ClusterMetadataManifest> validClusterManifests = manifestsByClusterUUID.values()
            .stream()
            .filter(this::isValidClusterUUID)
            .collect(Collectors.toList());
        final Map<String, String> clusterUUIDGraph = validClusterManifests.stream()
            .collect(Collectors.toMap(ClusterMetadataManifest::getClusterUUID, ClusterMetadataManifest::getPreviousClusterUUID));
        final List<String> topLevelClusterUUIDs = validClusterManifests.stream()
            .map(ClusterMetadataManifest::getClusterUUID)
            .filter(clusterUUID -> !clusterUUIDGraph.containsValue(clusterUUID))
            .collect(Collectors.toList());

        if (topLevelClusterUUIDs.isEmpty()) {
            // This can occur only when there are no valid cluster UUIDs
            assert validClusterManifests.isEmpty() : "There are no top level cluster UUIDs even when there are valid cluster UUIDs";
            logger.info("There is no valid previous cluster UUID. All cluster UUIDs evaluated are: {}", manifestsByClusterUUID.keySet());
            return Collections.emptyList();
        }
        if (topLevelClusterUUIDs.size() > 1) {
            logger.info("Top level cluster UUIDs: {}", topLevelClusterUUIDs);
            // If the valid cluster UUIDs are more that 1, it means there was some race condition where
            // more then 2 cluster manager nodes tried to become active cluster manager and published
            // 2 cluster UUIDs which followed the same previous UUID.
            final Map<String, ClusterMetadataManifest> manifestsByClusterUUIDTrimmed = trimClusterUUIDs(
                manifestsByClusterUUID,
                topLevelClusterUUIDs,
                clusterName
            );
            if (manifestsByClusterUUID.size() == manifestsByClusterUUIDTrimmed.size()) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "The system has ended into multiple valid cluster states in the remote store. "
                            + "Please check their latest manifest to decide which one you want to keep. Valid Cluster UUIDs: - %s",
                        topLevelClusterUUIDs
                    )
                );
            }
            return createClusterChain(manifestsByClusterUUIDTrimmed, clusterName);
        }
        final List<String> validChain = new ArrayList<>();
        String currentUUID = topLevelClusterUUIDs.get(0);
        while (currentUUID != null && !ClusterState.UNKNOWN_UUID.equals(currentUUID)) {
            validChain.add(currentUUID);
            // Getting the previous cluster UUID of a cluster UUID from the clusterUUID Graph
            currentUUID = clusterUUIDGraph.get(currentUUID);
        }
        logger.info("Known UUIDs found in remote store : [{}]", validChain);
        return validChain;
    }

    /**
     * This method take a map of manifests for different cluster UUIDs and removes the
     * manifest of a cluster UUID if the latest metadata for that cluster UUID is equivalent
     * to the latest metadata of its previous UUID.
     * @return Trimmed map of manifests
     */
    private Map<String, ClusterMetadataManifest> trimClusterUUIDs(
        final Map<String, ClusterMetadataManifest> latestManifestsByClusterUUID,
        final List<String> validClusterUUIDs,
        final String clusterName
    ) {
        final Map<String, ClusterMetadataManifest> trimmedUUIDs = new HashMap<>(latestManifestsByClusterUUID);
        for (String clusterUUID : validClusterUUIDs) {
            ClusterMetadataManifest currentManifest = trimmedUUIDs.get(clusterUUID);
            // Here we compare the manifest of current UUID to that of previous UUID
            // In case currentUUID's latest manifest is same as previous UUIDs latest manifest,
            // that means it was restored from previousUUID and no IndexMetadata update was performed on it.
            if (!ClusterState.UNKNOWN_UUID.equals(currentManifest.getPreviousClusterUUID())) {
                ClusterMetadataManifest previousManifest = trimmedUUIDs.get(currentManifest.getPreviousClusterUUID());
                if (isMetadataEqual(currentManifest, previousManifest, clusterName)
                    && remoteGlobalMetadataManager.isGlobalMetadataEqual(currentManifest, previousManifest, clusterName)) {
                    trimmedUUIDs.remove(clusterUUID);
                }
            }
        }
        return trimmedUUIDs;
    }

    private boolean isMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        // todo clusterName can be set as final in the constructor
        if (first.getIndices().size() != second.getIndices().size()) {
            return false;
        }
        final Map<String, UploadedIndexMetadata> secondIndices = second.getIndices()
            .stream()
            .collect(Collectors.toMap(md -> md.getIndexName(), Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                clusterName,
                first.getClusterUUID(),
                uploadedIndexMetadata
            );
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                clusterName,
                second.getClusterUUID(),
                secondUploadedIndexMetadata
            );
            if (firstIndexMetadata.equals(secondIndexMetadata) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidClusterUUID(ClusterMetadataManifest manifest) {
        return manifest.isClusterUUIDCommitted();
    }

    public void writeMetadataFailed() {
        getStats().stateFailed();
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     *
     * @param clusterName name of the cluster
     * @param clusterUUIDs clusteUUIDs for which the remote state needs to be purged
     */
    void deleteStaleUUIDsClusterMetadata(String clusterName, List<String> clusterUUIDs) {
        clusterUUIDs.forEach(clusterUUID -> {
            getBlobStoreTransferService().deleteAsync(
                ThreadPool.Names.REMOTE_PURGE,
                getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.info("Deleted all remote cluster metadata for cluster UUID - {}", clusterUUID);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting all remote cluster metadata for cluster UUID {}",
                                clusterUUID
                            ),
                            e
                        );
                        remoteStateStats.cleanUpAttemptFailed();
                    }
                }
            );
        });
    }

    /**
     * Deletes older than last {@code versionsToRetain} manifests. Also cleans up unreferenced IndexMetadata associated with older manifests
     *
     * @param clusterName name of the cluster
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param manifestsToRetain no of latest manifest files to keep in remote
     */
    // package private for testing
    void deleteStaleClusterMetadata(String clusterName, String clusterUUID, int manifestsToRetain) {
        if (deleteStaleMetadataRunning.compareAndSet(false, true) == false) {
            logger.info("Delete stale cluster metadata task is already in progress.");
            return;
        }
        try {
            getBlobStoreTransferService().listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteManifestManager.getManifestFolderPath(clusterName, clusterUUID),
                "manifest",
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        if (blobMetadata.size() > manifestsToRetain) {
                            deleteClusterMetadata(
                                clusterName,
                                clusterUUID,
                                blobMetadata.subList(0, manifestsToRetain - 1),
                                blobMetadata.subList(manifestsToRetain - 1, blobMetadata.size())
                            );
                        }
                        deleteStaleMetadataRunning.set(false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting Remote Cluster Metadata for clusterUUIDs {}",
                                clusterUUID
                            )
                        );
                        deleteStaleMetadataRunning.set(false);
                    }
                }
            );
        } catch (Exception e) {
            deleteStaleMetadataRunning.set(false);
            throw e;
        }
    }

    private void deleteClusterMetadata(
        String clusterName,
        String clusterUUID,
        List<BlobMetadata> activeManifestBlobMetadata,
        List<BlobMetadata> staleManifestBlobMetadata
    ) {
        try {
            Set<String> filesToKeep = new HashSet<>();
            Set<String> staleManifestPaths = new HashSet<>();
            Set<String> staleIndexMetadataPaths = new HashSet<>();
            Set<String> staleGlobalMetadataPaths = new HashSet<>();
            activeManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = remoteManifestManager.fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                clusterMetadataManifest.getIndices()
                    .forEach(uploadedIndexMetadata -> filesToKeep.add(uploadedIndexMetadata.getUploadedFilename()));
                if (clusterMetadataManifest.getGlobalMetadataFileName() != null) {
                    filesToKeep.add(clusterMetadataManifest.getGlobalMetadataFileName());
                } else {
                    filesToKeep.add(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
                    clusterMetadataManifest.getCustomMetadataMap()
                        .forEach((key, value) -> { filesToKeep.add(value.getUploadedFilename()); });
                }
            });
            staleManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = remoteManifestManager.fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                staleManifestPaths.add(new BlobPath().add(MANIFEST_PATH_TOKEN).buildAsString() + blobMetadata.name());
                if (clusterMetadataManifest.getGlobalMetadataFileName() != null) {
                    if (filesToKeep.contains(clusterMetadataManifest.getGlobalMetadataFileName()) == false) {
                        String[] globalMetadataSplitPath = clusterMetadataManifest.getGlobalMetadataFileName().split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                globalMetadataSplitPath[globalMetadataSplitPath.length - 1]
                            )
                        );
                    }
                } else {
                    if (filesToKeep.contains(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()) == false) {
                        String[] coordinationMetadataSplitPath = clusterMetadataManifest.getCoordinationMetadata()
                            .getUploadedFilename()
                            .split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                coordinationMetadataSplitPath[coordinationMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()) == false) {
                        String[] templatesMetadataSplitPath = clusterMetadataManifest.getTemplatesMetadata()
                            .getUploadedFilename()
                            .split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                templatesMetadataSplitPath[templatesMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()) == false) {
                        String[] settingsMetadataSplitPath = clusterMetadataManifest.getSettingsMetadata().getUploadedFilename().split("/");
                        staleGlobalMetadataPaths.add(
                            new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                settingsMetadataSplitPath[settingsMetadataSplitPath.length - 1]
                            )
                        );
                    }
                    clusterMetadataManifest.getCustomMetadataMap().forEach((key, value) -> {
                        if (filesToKeep.contains(value.getUploadedFilename()) == false) {
                            String[] customMetadataSplitPath = value.getUploadedFilename().split("/");
                            staleGlobalMetadataPaths.add(
                                new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                                    customMetadataSplitPath[customMetadataSplitPath.length - 1]
                                )
                            );
                        }
                    });
                }

                clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
                    if (filesToKeep.contains(uploadedIndexMetadata.getUploadedFilename()) == false) {
                        staleIndexMetadataPaths.add(
                            new BlobPath().add(INDEX_PATH_TOKEN).add(uploadedIndexMetadata.getIndexUUID()).buildAsString()
                                + INDEX_METADATA_FORMAT.blobName(uploadedIndexMetadata.getUploadedFilename())
                        );
                    }
                });
            });

            if (staleManifestPaths.isEmpty()) {
                logger.debug("No stale Remote Cluster Metadata files found");
                return;
            }

            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleGlobalMetadataPaths));
            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleIndexMetadataPaths));
            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleManifestPaths));
        } catch (IllegalStateException e) {
            logger.error("Error while fetching Remote Cluster Metadata manifests", e);
        } catch (IOException e) {
            logger.error("Error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        } catch (Exception e) {
            logger.error("Unexpected error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        }
    }

    private void deleteStalePaths(String clusterName, String clusterUUID, List<String> stalePaths) throws IOException {
        logger.debug(String.format(Locale.ROOT, "Deleting stale files from remote - %s", stalePaths));
        getBlobStoreTransferService().deleteBlobs(getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID), stalePaths);
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     * @param clusterState current state of the cluster
     * @param committedManifest last committed ClusterMetadataManifest
     */
    public void deleteStaleClusterUUIDs(ClusterState clusterState, ClusterMetadataManifest committedManifest) {
        threadpool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
            String clusterName = clusterState.getClusterName().value();
            logger.debug("Deleting stale cluster UUIDs data from remote [{}]", clusterName);
            Set<String> allClustersUUIDsInRemote;
            try {
                allClustersUUIDsInRemote = new HashSet<>(getAllClusterUUIDs(clusterState.getClusterName().value()));
            } catch (IOException e) {
                logger.info(String.format(Locale.ROOT, "Error while fetching all cluster UUIDs for [%s]", clusterName));
                return;
            }
            // Retain last 2 cluster uuids data
            allClustersUUIDsInRemote.remove(committedManifest.getClusterUUID());
            allClustersUUIDsInRemote.remove(committedManifest.getPreviousClusterUUID());
            deleteStaleUUIDsClusterMetadata(clusterName, new ArrayList<>(allClustersUUIDsInRemote));
        });
    }

    public RemotePersistenceStats getStats() {
        return remoteStateStats;
    }

    static final class AsyncStaleFileDeletion extends AbstractAsyncTask {
        private final RemoteClusterStateService remoteClusterStateService;

        AsyncStaleFileDeletion(RemoteClusterStateService remoteClusterStateService) {
            super(logger, remoteClusterStateService.threadpool, remoteClusterStateService.getStaleFileCleanupInterval(), true);
            this.remoteClusterStateService = remoteClusterStateService;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            remoteClusterStateService.cleanUpStaleFiles();
        }
    }
}
