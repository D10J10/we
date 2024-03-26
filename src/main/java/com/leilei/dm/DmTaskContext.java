/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;

public class DmTaskContext extends CdcSourceTaskContext {

    public DmTaskContext(DmConnectorConfig config,DmDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
