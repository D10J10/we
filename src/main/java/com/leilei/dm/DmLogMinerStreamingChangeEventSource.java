/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.leilei.dm;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.*;
import io.debezium.connector.oracle.logminer.HistoryRecorder;



import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.leilei.dm.DmLogMinerHelper.*;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class DmLogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DmLogMinerStreamingChangeEventSource.class);

    private final DmConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final DmDatabaseSchema schema;
    private final DmOffsetContext offsetContext;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final DmConnectorConfig.LogMiningStrategy strategy;
    private final DmTaskContext taskContext;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;
    private final DmStreamingChangeEventSourceMetrics streamingMetrics;
    private final DmConnectorConfig connectorConfig;
    private final Duration archiveLogRetention;

    private Scn startScn;
    private Scn endScn;
    private List<BigInteger> currentRedoLogSequences;

    public DmLogMinerStreamingChangeEventSource(DmConnectorConfig connectorConfig, DmOffsetContext offsetContext,
                                                DmConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                                ErrorHandler errorHandler, Clock clock, DmDatabaseSchema schema,
                                                DmTaskContext taskContext, Configuration jdbcConfig,
                                                DmStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context) {
        try (DmTransactionalBuffer transactionalBuffer = new DmTransactionalBuffer(schema, clock, errorHandler, streamingMetrics)) {
            try {
                startScn = offsetContext.getScn();
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn.compareTo(getFirstOnlineLogScn(jdbcConnection, archiveLogRetention)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                initializeRedoLogsForMining(jdbcConnection, false, archiveLogRetention);

                DmHistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(streamingMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final DmLogMinerQueryResultProcessor processor = new DmLogMinerQueryResultProcessor(context, jdbcConnection,
                            connectorConfig, streamingMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            clock, historyRecorder);

                    final String query = DmSqlUtils.logMinerContentsQuery(connectorConfig, jdbcConnection.username());
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                           900000, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {

                        currentRedoLogSequences = getCurrentRedoLogSequences();
                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {
                            // Calculate time difference before each mining session to detect time zone offset changes (e.g. DST) on database server
                            streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));

                            Instant start = Instant.now();
                            endScn = getEndScn(jdbcConnection, startScn, streamingMetrics, connectorConfig.getLogMiningBatchSizeDefault());
                            flushLogWriter(jdbcConnection, jdbcConfiguration, isRac, racHosts);

                            if (hasLogSwitchOccurred()) {
                                // This is the way to mitigate PGA leaks.
                                // With one mining session, it grows and maybe there is another way to flush PGA.
                                // At this point we use a new mining session
                                LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                        startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                endMining(jdbcConnection);

                                initializeRedoLogsForMining(jdbcConnection, true, archiveLogRetention);

                                abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);

                                // This needs to be re-calculated because building the data dictionary will force the
                                // current redo log sequence to be advanced due to a complete log switch of all logs.
                                currentRedoLogSequences = getCurrentRedoLogSequences();
                            }else{

                            }

                            startLogMining(jdbcConnection, startScn, endScn, strategy, isContinuousMining, streamingMetrics);

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setString(1, startScn.toString());
                            miningView.setString(2, endScn.toString());
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                streamingMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                startScn = endScn;

                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Transactional buffer empty, updating offset's SCN {}", startScn);
                                    offsetContext.setScn(startScn);
                                }
                            }

                            streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                            pauseBetweenMiningSessions();
                        }
                    }
                }
                finally {
                    historyRecorder.close();
                }
            }
            catch (Throwable t) {
                logError(streamingMetrics, "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            }
            finally {
                LOGGER.info("startScn={}, endScn={}, offsetContext.getScn()={}", startScn, endScn, offsetContext.getScn());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.toString());
                LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            }
        }
    }

    private void abandonOldTransactionsIfExist(DmConnection connection, DmTransactionalBuffer transactionalBuffer) {
        Duration transactionRetention = connectorConfig.getLogMiningTransactionRetention();
        if (!Duration.ZERO.equals(transactionRetention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(connection, offsetScn, transactionRetention);
            lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
                transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
                offsetContext.setScn(thresholdScn);
                startScn = endScn;
            });
        }
    }

    private void initializeRedoLogsForMining(DmConnection connection, boolean postEndMiningSession, Duration archiveLogRetention) throws SQLException {
        //TODO dm
        setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
//        if (!postEndMiningSession) {
//            if (DmConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
//                buildDataDictionary(connection);
//            }
//            if (!isContinuousMining) {
//                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
//            }
//        }
//        else {
//            if (!isContinuousMining) {
//                if (DmConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
//                    buildDataDictionary(connection);
//                }
//                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
//            }
//        }
    }

    /**
     * Checks whether a database log switch has occurred and updates metrics if so.
     *
     * @return {@code true} if a log switch was detected, otherwise {@code false}
     * @throws SQLException if a database exception occurred
     */
    private boolean hasLogSwitchOccurred() throws SQLException {
        // 判断dm 是否有回话  当前无活动的 LogMiner 会话
        // SELECT * FROM V$LOGMNR_CONTENTS 是否可以执行
        // 如果不可以执行 说明没有回话
        // 如果可以执行 说明有回话
        try{
            jdbcConnection.execute("SELECT * FROM V$LOGMNR_CONTENTS");
        }catch (Exception e){
            return false;
        }




//        final List<BigInteger> newSequences = getCurrentRedoLogSequences();
//        if (!newSequences.equals(currentRedoLogSequences)) {
//            LOGGER.debug("Current log sequence(s) is now {}, was {}", newSequences, currentRedoLogSequences);
//
//            currentRedoLogSequences = newSequences;
//
//            final Map<String, String> logStatuses = jdbcConnection.queryAndMap(DmSqlUtils.redoLogStatusQuery(), rs -> {
//                Map<String, String> results = new LinkedHashMap<>();
//                while (rs.next()) {
//                    results.put(rs.getString(1), rs.getString(2));
//                }
//                return results;
//            });
//
//            final int logSwitchCount = jdbcConnection.queryAndMap(DmSqlUtils.switchHistoryQuery(), rs -> {
//                if (rs.next()) {
//                    return rs.getInt(2);
//                }
//                return 0;
//            });
//
//            final Set<String> fileNames = getCurrentRedoLogFiles(jdbcConnection);
//
//            streamingMetrics.setRedoLogStatus(logStatuses);
//            streamingMetrics.setSwitchCount(logSwitchCount);
//            streamingMetrics.setCurrentLogFileName(fileNames);
//
//            return true;
//        }

        return true;
    }

    /**
     * Get the current redo log sequence(s).
     *
     * In an Oracle RAC environment, there are multiple current redo logs and therefore this method
     * returns multiple values, each relating to a single RAC node in the Oracle cluster.
     *
     * @return list of sequence numbers
     * @throws SQLException if a database exception occurred
     */
    private List<BigInteger> getCurrentRedoLogSequences() throws SQLException {
        return jdbcConnection.queryAndMap(DmSqlUtils.currentRedoLogSequenceQuery(), rs -> {
            List<BigInteger> sequences = new ArrayList<>();
            while (rs.next()) {
                sequences.add(new BigInteger(rs.getString(1)));
            }
            return sequences;
        });
    }

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(streamingMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
