/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.relational.TableId;

import java.time.Instant;

@NotThreadSafe
public class DmSourceInfo extends BaseSourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";

    private Scn scn;
    private Scn commitScn;
    private LcrPosition lcrPosition;
    private String transactionId;
    private Instant sourceTime;
    private TableId tableId;

    protected DmSourceInfo(DmConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public Scn getScn() {
        return scn;
    }

    public Scn getCommitScn() {
        return commitScn;
    }

    public void setScn(Scn scn) {
        this.scn = scn;
    }

    public void setCommitScn(Scn commitScn) {
        this.commitScn = commitScn;
    }

    public LcrPosition getLcrPosition() {
        return lcrPosition;
    }

    public void setLcrPosition(LcrPosition lcrPosition) {
        this.lcrPosition = lcrPosition;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return tableId.catalog();
    }
}
