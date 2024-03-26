/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.HistoryRecorder;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;

import java.sql.Timestamp;

/**
 * A history recorder implementation that does not do any recording.
 *
 * @author Chris Cranford
 */
public class DmNeverHistoryRecorder implements DmHistoryRecorder {
    @Override
    public void prepare(StreamingChangeEventSourceMetrics streamingMetrics, JdbcConfiguration jdbcConfiguration, long retentionHours) {
    }

    @Override
    public void record(Scn scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
                       String transactionId, int csf, String redoSql) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
