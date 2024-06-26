/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.oracle.BaseOracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;
import oracle.streams.DDLLCR;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle.
 *
 * @author Gunnar Morling
 */
public class DmXStreamSchemaChangeEventEmitter extends BaseDmSchemaChangeEventEmitter {

    public DmXStreamSchemaChangeEventEmitter(DmOffsetContext offsetContext, TableId tableId, DDLLCR ddlLcr) {
        super(offsetContext,
                tableId,
                ddlLcr.getSourceDatabaseName(),
                ddlLcr.getObjectOwner(),
                ddlLcr.getDDLText(),
                ddlLcr.getCommandType());
    }
}
