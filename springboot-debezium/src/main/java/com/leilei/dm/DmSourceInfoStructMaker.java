/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;


public class DmSourceInfoStructMaker extends AbstractSourceInfoStructMaker<DmSourceInfo> {

    private final Schema schema;

    public DmSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.oracle.Source")
                .field(DmSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(DmSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(DmSourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DmSourceInfo.SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DmSourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DmSourceInfo.LCR_POSITION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(DmSourceInfo sourceInfo) {
        final String scn = sourceInfo.getScn() == null ? null : sourceInfo.getScn().toString();
        final String commitScn = sourceInfo.getCommitScn() == null ? null : sourceInfo.getCommitScn().toString();

        final Struct ret = super.commonStruct(sourceInfo)
                .put(DmSourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
                .put(DmSourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table())
                .put(DmSourceInfo.TXID_KEY, sourceInfo.getTransactionId())
                .put(DmSourceInfo.SCN_KEY, scn);

        if (sourceInfo.getLcrPosition() != null) {
            ret.put(DmSourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition().toString());
        }
        if (commitScn != null) {
            ret.put(DmSourceInfo.COMMIT_SCN_KEY, commitScn);
        }
        return ret;
    }
}
