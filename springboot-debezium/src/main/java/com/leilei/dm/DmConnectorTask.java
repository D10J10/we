/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.*;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class DmConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(DmConnectorTask.class);
    private static final String CONTEXT_NAME = "dm-connector-task";

    private volatile DmTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile DmConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile DmDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        DmConnectorConfig connectorConfig = new DmConnectorConfig(config);
        TopicSelector<TableId> topicSelector = DmTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.jdbcConfig();
        //dm 的连接
        String jdbcString = "dm.jdbc.driver.DmDriver";
        try {
            Class.forName(jdbcString);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.jdbcConnection = new DmConnection(jdbcConfig, () -> getClass().getClassLoader());
        jdbcConnection = new DmConnection(jdbcConfig,
                () -> getClass().getClassLoader()
        );
        this.schema = new DmDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);
        this.schema.initializeStorage();

        String adapterString = config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER);
        DmConnectorConfig.ConnectorAdapter adapter = DmConnectorConfig.ConnectorAdapter.parse(adapterString);
        OffsetContext previousOffset = getPreviousOffset(new DmOffsetContext.Loader(connectorConfig, adapter));

        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        taskContext = new DmTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);

        final DmEventMetadataProvider metadataProvider = new DmEventMetadataProvider();

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final DmStreamingChangeEventSourceMetrics streamingMetrics = new DmStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new DmChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext, streamingMetrics),
                new DmChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return DmConnectorConfig.ALL_FIELDS;
    }
}
