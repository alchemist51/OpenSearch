/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.mv.pull.MVBuilderOutbox;
import org.opensearch.test.OpenSearchTestCase;

public class MVBuilderPublishActionTests extends OpenSearchTestCase {

    public void testRequestWireRoundTrip() throws Exception {
        MVBuilderOutbox.Publication pub = new MVBuilderOutbox.Publication(
            10L,
            20L,
            2L,
            5L,
            1234L,
            9876L,
            10L,
            1_700_000_000_123L,
            "cb_mv_mv1",
            42L
        );
        MVBuilderPublishAction.Request req = new MVBuilderPublishAction.Request("cb_mv_mv2", 0, "src-uuid", 0, pub);
        assertNull(req.validate());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            req.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MVBuilderPublishAction.Request copy = new MVBuilderPublishAction.Request(in);
                assertEquals("cb_mv_mv2", copy.followerIndex());
                assertEquals(0, copy.followerShard());
                assertEquals("src-uuid", copy.sourceIndexUuid());
                assertEquals(0, copy.sourceShard());
                assertEquals(pub, copy.publication());
            }
        }
    }

    public void testResponseWireRoundTrip() throws Exception {
        for (MVBuilderPublishAction.Response r : new MVBuilderPublishAction.Response[] {
            new MVBuilderPublishAction.Response(true, 20L, 441L, "published"),
            new MVBuilderPublishAction.Response(false, -1L, 0L, "gap"),
            new MVBuilderPublishAction.Response(true, 20L, 0L, null) }) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                r.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    MVBuilderPublishAction.Response copy = new MVBuilderPublishAction.Response(in);
                    assertEquals(r.applied(), copy.applied());
                    assertEquals(r.appliedWatermark(), copy.appliedWatermark());
                    assertEquals(r.publishMillis(), copy.publishMillis());
                    assertEquals(r.detail(), copy.detail());
                }
            }
        }
    }

    public void testActionNameIsWriteScoped() {
        assertEquals("indices:data/write/derived_state/builder_publish", MVBuilderPublishAction.NAME);
    }
}
