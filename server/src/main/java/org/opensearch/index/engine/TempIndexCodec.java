/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class TempIndexCodec extends FilterCodec {

    private final String parquetPath;

    public TempIndexCodec() {
        super("ParquetCodec", new Lucene912Codec());
        parquetPath = null;
    }

    public TempIndexCodec(Codec delegate, String parquetPath) {
        super("ParquetCodec", delegate);
        this.parquetPath = parquetPath;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                info.putAttribute("parquet_file", parquetPath);
                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }
}
