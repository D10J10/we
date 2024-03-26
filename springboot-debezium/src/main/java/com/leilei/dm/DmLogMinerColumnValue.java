/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

public interface DmLogMinerColumnValue {

    /**
     * @return value of the database record
     * with exception of LOB types
     */
    Object getColumnData();

    /**
     * @return column name
     */
    String getColumnName();

    /**
     * This sets the database record value with the exception of LOBs
     * @param columnData data
     */
    void setColumnData(Object columnData);
}
