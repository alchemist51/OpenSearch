/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.Locale;

/**
 * Settings for remote cluster state
 *
 * @opensearch.api
 */
public class RemoteClusterStateSettings {

    /**
     * Gates the functionality of remote publication.
     */
    public static final String REMOTE_PUBLICATION_SETTING_KEY = "cluster.remote_store.publication.enabled";

    public static final Setting<Boolean> REMOTE_PUBLICATION_SETTING = Setting.boolSetting(
        REMOTE_PUBLICATION_SETTING_KEY,
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final TimeValue REMOTE_STATE_READ_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> REMOTE_STATE_READ_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.read_timeout",
        REMOTE_STATE_READ_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RemoteClusterStateValidationMode> REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING = new Setting<>(
        "cluster.remote_store.state.checksum_validation.mode",
        RemoteClusterStateValidationMode.NONE.name(),
        RemoteClusterStateValidationMode::parseString,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls the fixed prefix for the cluster state path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.state.path.prefix",
        "",
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final TimeValue REMOTE_STATE_READ_MAX_JITTER_DEFAULT = TimeValue.timeValueMillis(500);

    public static final Setting<TimeValue> REMOTE_STATE_READ_MAX_JITTER = Setting.timeSetting(
        "cluster.remote_store.state.read.max_jitter",
        REMOTE_STATE_READ_MAX_JITTER_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue remoteStateReadTimeout;
    private RemoteClusterStateValidationMode remoteClusterStateValidationMode;
    private final String remotePathPrefix;
    private TimeValue remoteStateReadMaxJitter;
    private boolean remotePublicationSetting;

    public RemoteClusterStateSettings(Settings settings, ClusterSettings clusterSettings) {
        this.remoteStateReadTimeout = clusterSettings.get(REMOTE_STATE_READ_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STATE_READ_TIMEOUT_SETTING, this::setRemoteStateReadTimeout);
        this.remoteClusterStateValidationMode = REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING, this::setChecksumValidationMode);
        this.remotePathPrefix = CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX.get(settings);
        this.remoteStateReadMaxJitter = REMOTE_STATE_READ_MAX_JITTER.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STATE_READ_MAX_JITTER, this::setRemoteStateReadMaxJitter);
        this.remotePublicationSetting = REMOTE_PUBLICATION_SETTING.get(settings);
    }

    public boolean getRemotePublicationSetting() {
        return remotePublicationSetting;
    }

    private void setRemoteStateReadTimeout(TimeValue remoteStateReadTimeout) {
        this.remoteStateReadTimeout = remoteStateReadTimeout;
    }

    private void setChecksumValidationMode(RemoteClusterStateValidationMode remoteClusterStateValidationMode) {
        this.remoteClusterStateValidationMode = remoteClusterStateValidationMode;
    }

    private void setRemoteStateReadMaxJitter(TimeValue remoteStateReadMaxJitter) {
        this.remoteStateReadMaxJitter = remoteStateReadMaxJitter;
    }

    public TimeValue getRemoteStateReadTimeout() {
        return remoteStateReadTimeout;
    }

    public RemoteClusterStateValidationMode getRemoteClusterStateValidationMode() {
        return remoteClusterStateValidationMode;
    }

    public String getRemotePathPrefix() {
        return remotePathPrefix;
    }

    public TimeValue getRemoteStateReadMaxJitter() {
        return remoteStateReadMaxJitter;
    }

    /**
     * Validation mode for cluster state checksum.
     *  None: Validation will be disabled.
     *  Debug: Validation enabled but only matches checksum and logs failing entities.
     *  Trace: Matches checksum and downloads full cluster state to find diff in failing entities. Only logs failures.
     *  Failure: Throws exception on failing validation.
     */
    public enum RemoteClusterStateValidationMode {
        DEBUG("debug"),
        TRACE("trace"),
        FAILURE("failure"),
        NONE("none");

        public final String mode;

        RemoteClusterStateValidationMode(String mode) {
            this.mode = mode;
        }

        public static RemoteClusterStateValidationMode parseString(String mode) {
            try {
                return RemoteClusterStateValidationMode.valueOf(mode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + mode
                        + "] mode is not supported. "
                        + "supported modes are ["
                        + Arrays.toString(RemoteClusterStateValidationMode.values())
                        + "]"
                );
            }
        }
    }
}
