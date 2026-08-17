/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.state;

import org.opensearch.mv.MVStateDataFormatPlugin;

/**
 * Descriptor shim: real nodes load one plugin class per installed plugin, and
 * {@code MVStateDataFormatPlugin} lives inside the mv-data-format jar (the
 * IT's {@code nodePlugins()} hid this — it loads classes directly). All
 * behavior is inherited; this class only gives the mv_state format its own
 * plugin descriptor and classloader edge onto mv-data-format.
 */
public class MVStateFormatShimPlugin extends MVStateDataFormatPlugin {
    public MVStateFormatShimPlugin() {}
}
