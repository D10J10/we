/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class DmChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory {

    private final DmStreamingChangeEventSourceMetrics streamingMetrics;

    public DmChangeEventSourceMetricsFactory(DmStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics getStreamingMetrics(T taskContext,
                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                  EventMetadataProvider eventMetadataProvider) {
        return streamingMetrics;
    }
}
