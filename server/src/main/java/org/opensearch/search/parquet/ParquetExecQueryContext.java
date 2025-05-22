package org.opensearch.search.parquet;

import org.apache.arrow.datafusion.ExecutionOptions;
import org.apache.arrow.datafusion.ParquetExec;
import org.apache.arrow.datafusion.SessionConfig;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.SearchContext;

@ExperimentalApi
public class ParquetExecQueryContext implements AutoCloseable {
    private final SessionContext sessionContext;
    private final ParquetExec parquetExec;
    private BufferAllocator allocator;
    private final String parquetPath;
    private final Boolean cacheEnabled;
    public ParquetExecQueryContext(SearchContext context, String parquetPath, ClusterService clusterService) {
        try {

            SessionConfig sessionConfig = new SessionConfig();
            ExecutionOptions executionOptions = new ExecutionOptions(sessionConfig);
            executionOptions.withTargetPartitions(1);
            this.cacheEnabled = clusterService.getClusterSettings().get(SearchService.CacheEnabled);
            if(cacheEnabled) {
                this.sessionContext = SessionContexts.withConfigRunTime(executionOptions.getConfig(), 10);
            } else {
                this.sessionContext = SessionContexts.withConfig(executionOptions.getConfig());
            }
            this.parquetExec = new ParquetExec(sessionContext, sessionContext.getPointer());
            this.allocator = new RootAllocator();
            this.parquetPath = parquetPath;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ParquetExec context", e);
        }
    }

    public ParquetExecQueryContext(ParquetExecQueryContext parquetExecQueryContext) {
        try {
            this.sessionContext = parquetExecQueryContext.sessionContext;
            this.parquetExec = new ParquetExec(parquetExecQueryContext.sessionContext, parquetExecQueryContext.sessionContext.getPointer());
            this.allocator = new RootAllocator();
            this.parquetPath = parquetExecQueryContext.parquetPath;
            this.cacheEnabled = parquetExecQueryContext.cacheEnabled;
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

    public SessionContext getSessionContext() {
        return sessionContext;
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
