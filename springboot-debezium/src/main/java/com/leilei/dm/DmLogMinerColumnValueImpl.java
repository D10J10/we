/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.oracle.antlr.listener.ParserUtils;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;

import java.util.Objects;

/**
 * This class stores parsed column info
 *
 */
public class DmLogMinerColumnValueImpl implements DmLogMinerColumnValue {

    private String columnName;
    private Object columnData;
    private int columnType;

    public DmLogMinerColumnValueImpl(String columnName, int columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    @Override
    public Object getColumnData() {
        return columnData;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnData(Object columnData) {
        if (columnData instanceof String) {
            this.columnData = DmParserUtils.replaceDoubleBackSlashes((String) columnData);
        }
        else {
            this.columnData = columnData;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DmLogMinerColumnValueImpl that = (DmLogMinerColumnValueImpl) o;
        return columnType == that.columnType &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnData, that.columnData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnData, columnType);
    }
}
