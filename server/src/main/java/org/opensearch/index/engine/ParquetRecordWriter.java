/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.PathUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ParquetRecordWriter {

    private static final String schemaString = "message ELBLog {\n" +
        "  required binary backend_ip;\n" +
        "  required int32 backend_port;\n" +
        "  required float backend_processing_time;\n" +
        "  required int32 backend_status_code;\n" +
        "  required binary client_ip;\n" +
        "  required int32 client_port;\n" +
        "  required float connection_time;\n" +
        "  required binary destination_ip;\n" +
        "  required int32 destination_port;\n" +
        "  required int32 elb_status_code;\n" +
        "  required int32 http_port;\n" +
        "  required binary http_version;\n" +
        "  required int32 matched_rule_priority;\n" +
        "  required int32 received_bytes;\n" +
        "  required int64 request_creation_time;\n" +
        "  required float request_processing_time;\n" +
        "  required float response_processing_time;\n" +
        "  required int32 sent_bytes;\n" +
        "  required binary target_ip;\n" +
        "  required int32 target_port;\n" +
        "  required float target_processing_time;\n" +
        "  required int32 target_status_code;\n" +
        "  required int64 timestamp;\n" +
        "  required int64 ___row_id;\n" +
        "  required binary _id;\n" +
        "}";

    public static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    private final AtomicLong counter;

    private final CheckedSupplier<Tuple<String, WriterWrapper>, IOException> writerSupplier;

    private ConcurrentLinkedQueue<Tuple<String, WriterWrapper>> pool = new ConcurrentLinkedQueue<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public ParquetRecordWriter(String dataPath, EngineConfig config) {
        try {
            Path path = Files.createDirectory(PathUtils.get(dataPath));
            latestFlushPoint.addAll(Files.list(path).map(p -> p.getFileName().toString()).collect(Collectors.toList()));
            if (!latestFlushPoint.isEmpty()) {
                counter = new AtomicLong(latestFlushPoint.stream().mapToLong(s -> Long.parseLong(s.split(".")[0].split("-")[1])).max().getAsLong());
            } else {
                counter = new AtomicLong();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writerSupplier = () -> {
            long generation = counter.incrementAndGet();
            String fileName = dataPath + "generation-" + generation + ".parquet";

            return Tuple.tuple(fileName, new WriterWrapper(ExampleParquetWriter.builder(new LocalOutputFile(PathUtils.get(fileName)))
                .withConf(new Configuration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(true)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build(), fileName, config));
        };
    }

    public void write(Group document, Engine.Index index) throws IOException {
        try {
            lock.readLock().lock();
            Tuple<String, WriterWrapper> tuple = pool.poll();
            if (tuple == null) {
                tuple = writerSupplier.get();
            }
            ParquetWriter<Group> writer = tuple.v2().writer;
            long rowId = tuple.v2().rowIdGenerator.getAndIncrement();
            writer.write(parquetRow(document, rowId));
            index.docs().get(0).add(new NumericDocValuesField(ROW_ID_FIELD, rowId));
            tuple.v2().indexWriter.addDocument(index.docs().get(0));
            pool.offer(tuple);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasPendingWriters() {
        return !pool.isEmpty();
    }

    private final List<String> latestFlushPoint = new CopyOnWriteArrayList<>();

    public  List<String> flushPoint() {
        return latestFlushPoint;
    }

    public List<IndexWriter> flush() throws IOException {
        final List<Tuple<String, WriterWrapper>> writers = new ArrayList<>();
        try {
            lock.writeLock().lock();
            while (!pool.isEmpty()) {
                writers.add(pool.poll());
            }
        } finally {
            lock.writeLock().unlock();
        }
        for (Tuple<String, WriterWrapper> writer : writers) {
            writer.v2().writer.close();
        }
        List<String> newPaths = writers.stream().map(Tuple::v1).collect(Collectors.toList());
        latestFlushPoint.addAll(newPaths);
        return writers.stream().map(Tuple::v2).map(WriterWrapper::getIndexWriter).collect(Collectors.toList());
    }

    private Group parquetRow(Group doc, long rowId) {
        doc.add(ROW_ID_FIELD, rowId);
        return doc;
    }

    public static final String ROW_ID_FIELD = "___row_id";

    static class WriterWrapper {
        final ParquetWriter<Group> writer;
        final AtomicLong rowIdGenerator = new AtomicLong();
        final IndexWriter indexWriter;
        final Directory directory;
        public WriterWrapper(ParquetWriter<Group> writer, String fileName, EngineConfig config) throws IOException {
            this.writer = writer;
            directory = FSDirectory.open(Files.createTempDirectory(Long.toString(System.nanoTime())));
            indexWriter = new IndexWriter(directory, new IndexWriterConfig()
                .setMergePolicy(new LogByteSizeMergePolicy())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setMergeScheduler(new OpenSearchConcurrentMergeScheduler(config.getShardId(), config.getIndexSettings()))
                .setCodec(new TempIndexCodec(config.getCodec(), fileName))
                .setIndexSort(new Sort(new SortField(ROW_ID_FIELD, SortField.Type.LONG))));
        }

        public IndexWriter getIndexWriter() {
            return indexWriter;
        }
    }
}
