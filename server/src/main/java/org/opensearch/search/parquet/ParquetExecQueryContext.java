package org.opensearch.search.parquet;

import org.apache.arrow.datafusion.ParquetExec;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.SearchContext;

@ExperimentalApi
public class ParquetExecQueryContext implements AutoCloseable {
    private final SessionContext sessionContext;
    private final ParquetExec parquetExec;
    private final BufferAllocator allocator;
    private final String parquetPath;

    public ParquetExecQueryContext(SearchContext context, String parquetPath) {
        try {
            this.sessionContext = SessionContexts.create();
            this.parquetExec = new ParquetExec(sessionContext, sessionContext.getPointer());
            this.allocator = new RootAllocator();
            this.parquetPath = parquetPath;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ParquetExec context", e);
        }
    }

    public ParquetExec getParquetExec() {
        return parquetExec;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    @Override
    public void close() {
        try {
            if (allocator != null) allocator.close();
            if (sessionContext != null) sessionContext.close();
        } catch (Exception e) {
            System.out.println("Exception in parquet exec query context" + e);
        }
    }
}
