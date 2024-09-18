/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.Randomness;

import java.util.Random;

/**
 * Utility to provide a {@link Random} static instance
 *
 * @opensearch.internal
 */
public class DefaultRandomObject {
    public static final Random INSTANCE = new Random(Randomness.get().nextLong());
}
