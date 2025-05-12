/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import java.util.HashMap;
import java.util.Map;

public class PathTransformer {
    private final Map<String, String> pathMappings;

    public PathTransformer() {
        pathMappings = new HashMap<>();
        pathMappings.put(
            "/home/ec2-user/workplace/OpenSearch/build/distribution/local/opensearch-3.0.0-SNAPSHOT",
            "/Users/abandeji/Public/workplace/OpenSearch/build/distribution/local/opensearch-3.0.0-SNAPSHOT"
        );
    }

    public String transformPath(String originalPath) {
        for (Map.Entry<String, String> mapping : pathMappings.entrySet()) {
            if (originalPath.startsWith(mapping.getKey())) {
                return originalPath.replace(mapping.getKey(), mapping.getValue());
            }
        }
        return originalPath;
    }
}
