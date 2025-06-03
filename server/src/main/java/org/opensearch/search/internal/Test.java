/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.lucene.Lucene;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class Test {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java MyMain <source> <dest>");
            System.exit(1);
        }

        String sourcePath = args[0];
        String destPath = args[1];


        System.out.println("Starting segment multiplication:");
        System.out.println("Source: " + sourcePath);
        System.out.println("Dest: " + destPath);

        try(Directory dir = FSDirectory.open(Path.of(sourcePath))) { // source file
            IndexWriter writer2 = new IndexWriter(dir, new IndexWriterConfig());

            Directory destDir = FSDirectory.open(Path.of(destPath));
            for (int i = 0; i < 10; i ++) {
                writer2.addIndexes(destDir); // destination file
            }

            SegmentInfos lastCommittedSegmentInfos = Lucene.readSegmentInfos(destDir);
            final Map<String, String> commitData = new HashMap<>(1);
            commitData.putAll(lastCommittedSegmentInfos.getUserData());

            writer2.setLiveCommitData(commitData.entrySet());
            writer2.commit();

            writer2.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
