/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class DmTopicSelector {

    public static TopicSelector<TableId> defaultSelector(DmConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
