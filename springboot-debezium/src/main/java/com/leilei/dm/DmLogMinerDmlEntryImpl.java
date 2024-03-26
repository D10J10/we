/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

/**
 * This class holds one parsed DML LogMiner record details
 *
 */
public class DmLogMinerDmlEntryImpl implements DmLogMinerDmlEntry {

    private Envelope.Operation commandType;
    private List<DmLogMinerColumnValue> newLmColumnValues;
    private List<DmLogMinerColumnValue> oldLmColumnValues;
    private String objectOwner;
    private String objectName;
    private Timestamp sourceTime;
    private String transactionId;
    private Scn scn;
    private String rowId;

    public DmLogMinerDmlEntryImpl(Envelope.Operation commandType, List<DmLogMinerColumnValue> newLmColumnValues, List<DmLogMinerColumnValue> oldLmColumnValues) {
        this.commandType = commandType;
        this.newLmColumnValues = newLmColumnValues;
        this.oldLmColumnValues = oldLmColumnValues;
    }

    @Override
    public Envelope.Operation getCommandType() {
        return commandType;
    }

    @Override
    public List<DmLogMinerColumnValue> getOldValues() {
        return oldLmColumnValues;
    }

    @Override
    public List<DmLogMinerColumnValue> getNewValues() {
        return newLmColumnValues;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public Timestamp getSourceTime() {
        return sourceTime;
    }

    @Override
    public String getRowId() {
        return rowId;
    }

    @Override
    public void setObjectName(String name) {
        this.objectName = name;
    }

    @Override
    public void setObjectOwner(String name) {
        this.objectOwner = name;
    }

    @Override
    public void setSourceTime(Timestamp changeTime) {
        this.sourceTime = changeTime;
    }

    @Override
    public void setTransactionId(String id) {
        this.transactionId = id;
    }

    @Override
    public Scn getScn() {
        return scn;
    }

    @Override
    public void setScn(Scn scn) {
        this.scn = scn;
    }

    @Override
    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DmLogMinerDmlEntryImpl that = (DmLogMinerDmlEntryImpl) o;
        return commandType == that.commandType &&
                Objects.equals(newLmColumnValues, that.newLmColumnValues) &&
                Objects.equals(oldLmColumnValues, that.oldLmColumnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandType, newLmColumnValues, oldLmColumnValues);
    }
}
