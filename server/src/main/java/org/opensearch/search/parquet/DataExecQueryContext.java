package org.opensearch.search.parquet;

import org.apache.arrow.datafusion.DataExec;
import org.apache.arrow.datafusion.ExecutionEngine;
import org.apache.arrow.datafusion.ExecutionOptions;
import org.apache.arrow.datafusion.ParquetExec;
import org.apache.arrow.datafusion.SessionConfig;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.datafusion.VortexExec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.SearchContext;

@ExperimentalApi
public class DataExecQueryContext implements AutoCloseable {
    private final SessionContext sessionContext;
    private final DataExec dataExec;
    private final ParquetExec parquetExec; // Keep for backward compatibility
    private final VortexExec vortexExec;
    private BufferAllocator allocator;
    private final String filePath;
    private final ExecutionEngine engine;
    private final Boolean cacheEnabled;
    private static volatile SessionContext sharedSessionContext;
    private static final Object LOCK = new Object();

    private static SessionContext createSessionContext(Boolean cacheEnabled) {
        SessionConfig sessionConfig = new SessionConfig();
        ExecutionOptions executionOptions = new ExecutionOptions(sessionConfig);
        executionOptions.withTargetPartitions(1);

        if (cacheEnabled) {
            return SessionContexts.withConfigRunTime(executionOptions.getConfig(), 10);
        } else {
            return SessionContexts.withConfig(executionOptions.getConfig());
        }
    }

    private static SessionContext getOrCreateSessionContext(Boolean cacheEnabled) {
        SessionContext result = sharedSessionContext;
        if (result == null) {
            synchronized (LOCK) {
                result = sharedSessionContext;
                if (result == null) {
                    sharedSessionContext = result = createSessionContext(cacheEnabled);
                }
            }
        }
        return result;
    }

    public DataExecQueryContext(SearchContext context, String filePath, ClusterService clusterService) {
        try {
            this.filePath = filePath;

            this.engine = clusterService.getClusterSettings().get(SearchService.DataEngine);

            SessionConfig sessionConfig = new SessionConfig();
            ExecutionOptions executionOptions = new ExecutionOptions(sessionConfig);
            executionOptions.withTargetPartitions(1);
            this.cacheEnabled = clusterService.getClusterSettings().get(SearchService.CacheEnabled);

            if (cacheEnabled) {
                this.sessionContext = getOrCreateSessionContext(this.cacheEnabled);
            } else {
                this.sessionContext = SessionContexts.withConfig(executionOptions.getConfig());
            }

            // Create the unified DataExec
            this.dataExec = new DataExec(sessionContext, sessionContext.getPointer(), engine);

            // Create specific executors for backward compatibility and direct access
            switch (engine) {
                case PARQUET:
                    this.parquetExec = new ParquetExec(sessionContext, sessionContext.getPointer());
                    this.vortexExec = null;
                    break;
                case VORTEX:
                    this.vortexExec = new VortexExec(sessionContext, sessionContext.getPointer());
                    this.parquetExec = null;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported execution engine: " + engine);
            }

            this.allocator = new RootAllocator();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DataExec context", e);
        }
    }

    // Copy constructor
    public DataExecQueryContext(DataExecQueryContext other) {
        try {
            this.engine = other.engine;
            this.filePath = other.filePath;
            this.sessionContext = other.sessionContext;
            this.cacheEnabled = other.cacheEnabled;

            // Create new DataExec instance
            this.dataExec = new DataExec(sessionContext, sessionContext.getPointer(), engine);

            // Create specific executors
            switch (engine) {
                case PARQUET:
                    this.parquetExec = new ParquetExec(sessionContext, sessionContext.getPointer());
                    this.vortexExec = null;
                    break;
                case VORTEX:
                    this.vortexExec = new VortexExec(sessionContext, sessionContext.getPointer());
                    this.parquetExec = null;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported execution engine: " + engine);
            }

            this.allocator = new RootAllocator();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DataExec context", e);
        }
    }

    // Unified access methods
    public DataExec getDataExec() {
        return dataExec;
    }

    public ExecutionEngine getEngine() {
        return engine;
    }

    // Backward compatibility methods
    public ParquetExec getParquetExec() {
        if (parquetExec == null) {
            throw new UnsupportedOperationException("ParquetExec not available for engine: " + engine);
        }
        return parquetExec;
    }

    public VortexExec getVortexExec() {
        if (vortexExec == null) {
            throw new UnsupportedOperationException("VortexExec not available for engine: " + engine);
        }
        return vortexExec;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public String getFilePath() {
        return filePath;
    }

    // Backward compatibility
    public String getParquetPath() {
        return filePath;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    // Add a method to explicitly close the shared session context when needed
    public static void closeSharedSessionContext() {
        synchronized (LOCK) {
            if (sharedSessionContext != null) {
                try {
                    sharedSessionContext.close();
                } catch (Exception e) {
                    System.out.println("Exception closing shared session context: " + e);
                } finally {
                    sharedSessionContext = null;
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            if (allocator != null) allocator.close();
            if (dataExec != null) dataExec.close();
            if (this.cacheEnabled == false) {
                if (sessionContext != null) sessionContext.close();
            }
        } catch (Exception e) {
            System.out.println("Exception in data exec query context: " + e);
        }
    }
}
