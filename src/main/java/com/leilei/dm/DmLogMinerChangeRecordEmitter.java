/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;


import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DmLogMinerChangeRecordEmitter extends DmBaseChangeRecordEmitter<DmLogMinerColumnValue> {

    private DmLogMinerDmlEntry dmlEntry;
    protected final Table table;

    public DmLogMinerChangeRecordEmitter(OffsetContext offset, DmLogMinerDmlEntry dmlEntry, Table table, Clock clock) {
        super(offset, table, clock);
        this.dmlEntry = dmlEntry;
        this.table = table;
    }

    @Override
    protected Operation getOperation() {
        return dmlEntry.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues() {
        List<DmLogMinerColumnValue> valueList = dmlEntry.getOldValues();
        DmLogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), DmLogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected Object[] getNewColumnValues() {
        List<DmLogMinerColumnValue> valueList = dmlEntry.getNewValues();
        DmLogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), DmLogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected String getColumnName(DmLogMinerColumnValue columnValue) {
        return columnValue.getColumnName();
    }

    protected Object getColumnData(DmLogMinerColumnValue columnValue) {
        return columnValue.getColumnData();
    }
}
