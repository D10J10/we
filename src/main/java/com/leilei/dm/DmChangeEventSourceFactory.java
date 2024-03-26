/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.*;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class DmChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final DmConnectorConfig configuration;
    private final DmConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final DmDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final DmTaskContext taskContext;
    private final DmStreamingChangeEventSourceMetrics streamingMetrics;

    public DmChangeEventSourceFactory(DmConnectorConfig configuration, DmConnection jdbcConnection,
                                      ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, DmDatabaseSchema schema,
                                      Configuration jdbcConfig, DmTaskContext taskContext,
                                      DmStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new DmSnapshotChangeEventSource(configuration, (OracleOffsetContext) offsetContext, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        DmConnectorConfig.ConnectorAdapter adapter = configuration.getAdapter();
        if (adapter == DmConnectorConfig.ConnectorAdapter.XSTREAM) {
            return new DmXstreamStreamingChangeEventSource(
                    configuration,
                    (DmOffsetContext) offsetContext,
                    jdbcConnection,
                    dispatcher,
                    errorHandler,
                    clock,
                    schema);
        }
        return new DmLogMinerStreamingChangeEventSource(
                configuration,
                (DmOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }
}
