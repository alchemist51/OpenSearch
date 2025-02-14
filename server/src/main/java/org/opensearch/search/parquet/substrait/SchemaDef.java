/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet.substrait;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

// TODO : doesn't work currently
public class SchemaDef {
    public static String createSchemaDefinitionFromParquet(String parquetPath) {
        if (parquetPath == null) {
            parquetPath = "file://"
                + "/Users/gbh/Documents/BKD/opensearch-3.0.0-SNAPSHOT/data/nodes/0/indices/"
                + "setEruFZTR68hF1MH51mNQ/0/index/1728892707_zstd_32mb_rg_v2.parquet";
        }
        try (BufferAllocator allocator = new RootAllocator()) {
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                parquetPath
            );
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = factory.inspect();

            return convertArrowSchemaToSqlSchema(arrowSchema);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create schema definition from Parquet file", e);
        }
    }

    private static String convertArrowSchemaToSqlSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        StringBuilder sqlSchema = new StringBuilder("CREATE TABLE LOGS (");
        List<Field> fields = arrowSchema.getFields();

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sqlSchema.append("\n  ").append(field.getName()).append(" ").append(convertArrowTypeToSqlType(field.getType()));

            if (i < fields.size() - 1) {
                sqlSchema.append(",");
            }
        }

        sqlSchema.append("\n)");
        return sqlSchema.toString();
    }

    private static String convertArrowTypeToSqlType(org.apache.arrow.vector.types.pojo.ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 64) {
                return "BIGINT";
            } else {
                return "INTEGER";
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
            switch (floatType.getPrecision()) {
                case SINGLE:
                    return "FLOAT";
                case DOUBLE:
                    return "DOUBLE";
                default:
                    return "DOUBLE";
            }
        } else if (arrowType instanceof ArrowType.Utf8) {
            return "VARCHAR";
        } else if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (arrowType instanceof ArrowType.Date) {
            return "DATE";
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return "TIMESTAMP";
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            return String.format("DECIMAL(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
        } else if (arrowType instanceof ArrowType.Binary) {
            return "VARBINARY";
        }

        // Default to VARCHAR for unknown types
        return "VARCHAR";
    }
}
