/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc;

import static io.airbyte.cdk.db.DbAnalyticsUtils.cdcCursorInvalidMessage;
import static io.airbyte.integrations.source.postgres.PostgresQueryUtils.streamsUnderVacuum;
import static io.airbyte.integrations.source.postgres.PostgresSpecConstants.FAIL_SYNC_OPTION;
import static io.airbyte.integrations.source.postgres.PostgresSpecConstants.INVALID_CDC_CURSOR_POSITION_PROPERTY;
import static io.airbyte.integrations.source.postgres.PostgresUtils.isDebugMode;
import static io.airbyte.integrations.source.postgres.PostgresUtils.prettyPrintConfiguredAirbyteStreamList;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.components.ComponentRunner;
import io.airbyte.cdk.components.debezium.DebeziumAirbyteMessageFactory;
import io.airbyte.cdk.components.debezium.DebeziumRecord;
import io.airbyte.cdk.components.debezium.DebeziumState;
import io.airbyte.cdk.db.PgLsn;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcUtils;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.cdk.integrations.source.relationaldb.TableInfo;
import io.airbyte.cdk.integrations.source.relationaldb.models.CdcState;
import io.airbyte.cdk.integrations.source.relationaldb.state.StateManager;
import io.airbyte.commons.exceptions.ConfigErrorException;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.postgres.PostgresQueryUtils;
import io.airbyte.integrations.source.postgres.PostgresQueryUtils.TableBlockSize;
import io.airbyte.integrations.source.postgres.PostgresType;
import io.airbyte.integrations.source.postgres.PostgresUtils;
import io.airbyte.integrations.source.postgres.ctid.CtidGlobalStateManager;
import io.airbyte.integrations.source.postgres.ctid.CtidPostgresSourceOperations;
import io.airbyte.integrations.source.postgres.ctid.CtidPostgresSourceOperations.CdcMetadataInjector;
import io.airbyte.integrations.source.postgres.ctid.CtidStateManager;
import io.airbyte.integrations.source.postgres.ctid.CtidUtils;
import io.airbyte.integrations.source.postgres.ctid.FileNodeHandler;
import io.airbyte.integrations.source.postgres.ctid.PostgresCtidHandler;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcCtidInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcCtidInitializer.class);

  public static List<AutoCloseableIterator<AirbyteMessage>> cdcCtidIteratorsCombined(final JdbcDatabase database,
                                                                                     final ConfiguredAirbyteCatalog catalog,
                                                                                     final Map<String, TableInfo<CommonField<PostgresType>>> tableNameToTable,
                                                                                     final StateManager stateManager,
                                                                                     final Instant emittedAt,
                                                                                     final String quoteString,
                                                                                     final JsonNode replicationSlot) {
    try {
      final JsonNode sourceConfig = database.getSourceConfig();

      if (isDebugMode(sourceConfig) && !PostgresUtils.shouldFlushAfterSync(sourceConfig)) {
        throw new ConfigErrorException("WARNING: The config indicates that we are clearing the WAL while reading data. This will mutate the WAL" +
            " associated with the source being debugged and is not advised.");
      }

      final var messageFactory = new DebeziumAirbyteMessageFactory(stateManager, emittedAt, PostgresDebeziumComponentUtils::toAirbyteRecordMessage);
      final DebeziumState initialDebeziumState =
          PostgresDebeziumComponentUtils.makeSyntheticDebeziumState(database, sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());
      final DebeziumState currentDebeziumState = messageFactory.stateFromManager().orElse(initialDebeziumState);
      final PgLsn currentLsn = new PostgresLsnMapper().get(currentDebeziumState.offset());

      final boolean savedOffsetAfterReplicationSlotLSN = new PostgresDebeziumStateUtil().isSavedOffsetAfterReplicationSlotLSN(
          // We can assume that there will be only 1 replication slot cause before the sync starts for
          // Postgres CDC,
          // we run all the check operations and one of the check validates that the replication slot exists
          // and has only 1 entry
          replicationSlot,
          OptionalLong.of(currentLsn.asLong()));

      if (!savedOffsetAfterReplicationSlotLSN) {
        AirbyteTraceMessageUtility.emitAnalyticsTrace(cdcCursorInvalidMessage());
        if (!sourceConfig.get("replication_method").has(INVALID_CDC_CURSOR_POSITION_PROPERTY) || sourceConfig.get("replication_method").get(
            INVALID_CDC_CURSOR_POSITION_PROPERTY).asText().equals(FAIL_SYNC_OPTION)) {
          throw new ConfigErrorException(
              "Saved offset is before replication slot's confirmed lsn. Please reset the connection, and then increase WAL retention and/or increase sync frequency to prevent this from happening in the future. See https://docs.airbyte.com/integrations/sources/postgres/postgres-troubleshooting#under-cdc-incremental-mode-there-are-still-full-refresh-syncs for more details.");
        }
        LOGGER.warn("Saved offset is before Replication slot's confirmed_flush_lsn, Airbyte will trigger sync from scratch");
      } else if (!isDebugMode(sourceConfig) && PostgresUtils.shouldFlushAfterSync(sourceConfig)) {
        // We do not want to acknowledge the WAL logs in debug mode.
        new PostgresDebeziumStateUtil().commitLSNToPostgresDatabase(
            database.getDatabaseConfig(),
            OptionalLong.of(currentLsn.asLong()),
            sourceConfig.get("replication_method").get("replication_slot").asText(),
            sourceConfig.get("replication_method").get("publication").asText(),
            PostgresUtils.getPluginValue(sourceConfig.get("replication_method")));
      }
      final CdcState stateToBeUsed = (!savedOffsetAfterReplicationSlotLSN
          || stateManager.getCdcStateManager().getCdcState() == null
          || stateManager.getCdcStateManager().getCdcState().getState() == null)
              ? DebeziumAirbyteMessageFactory.toCdcState(initialDebeziumState)
              : stateManager.getCdcStateManager().getCdcState();
      final PostgresCdcCtidUtils.CtidStreams ctidStreams = PostgresCdcCtidUtils.streamsToSyncViaCtid(stateManager.getCdcStateManager(), catalog,
          savedOffsetAfterReplicationSlotLSN);
      final List<AutoCloseableIterator<AirbyteMessage>> initialSyncCtidIterators = new ArrayList<>();
      final List<AirbyteStreamNameNamespacePair> streamsUnderVacuum = new ArrayList<>();
      if (!ctidStreams.streamsForCtidSync().isEmpty()) {
        streamsUnderVacuum.addAll(streamsUnderVacuum(database,
            ctidStreams.streamsForCtidSync(), quoteString).result());

        final List<ConfiguredAirbyteStream> finalListOfStreamsToBeSyncedViaCtid =
            streamsUnderVacuum.isEmpty() ? ctidStreams.streamsForCtidSync()
                : ctidStreams.streamsForCtidSync().stream()
                    .filter(c -> !streamsUnderVacuum.contains(AirbyteStreamNameNamespacePair.fromConfiguredAirbyteSteam(c)))
                    .toList();
        LOGGER.info("Streams to be synced via ctid : {}", finalListOfStreamsToBeSyncedViaCtid.size());
        LOGGER.info("Streams: {}", prettyPrintConfiguredAirbyteStreamList(finalListOfStreamsToBeSyncedViaCtid));
        final FileNodeHandler fileNodeHandler = PostgresQueryUtils.fileNodeForStreams(
            database,
            finalListOfStreamsToBeSyncedViaCtid,
            quoteString);
        final CtidStateManager ctidStateManager = new CtidGlobalStateManager(ctidStreams, fileNodeHandler, stateToBeUsed, catalog);
        final CtidPostgresSourceOperations ctidPostgresSourceOperations = new CtidPostgresSourceOperations(
            Optional.of(new CdcMetadataInjector(
                emittedAt.toString(), io.airbyte.cdk.db.PostgresUtils.getLsn(database).asLong(), new PostgresCdcConnectorMetadataInjector())));
        final Map<io.airbyte.protocol.models.AirbyteStreamNameNamespacePair, TableBlockSize> tableBlockSizes =
            PostgresQueryUtils.getTableBlockSizeForStreams(
                database,
                finalListOfStreamsToBeSyncedViaCtid,
                quoteString);

        final Map<io.airbyte.protocol.models.AirbyteStreamNameNamespacePair, Integer> tablesMaxTuple =
            CtidUtils.isTidRangeScanCapableDBServer(database) ? null
                : PostgresQueryUtils.getTableMaxTupleForStreams(database, finalListOfStreamsToBeSyncedViaCtid, quoteString);

        final PostgresCtidHandler ctidHandler = new PostgresCtidHandler(sourceConfig, database,
            ctidPostgresSourceOperations,
            quoteString,
            fileNodeHandler,
            tableBlockSizes,
            tablesMaxTuple,
            ctidStateManager,
            namespacePair -> Jsons.emptyObject());

        initialSyncCtidIterators.addAll(ctidHandler.getInitialSyncCtidIterator(
            new ConfiguredAirbyteCatalog().withStreams(finalListOfStreamsToBeSyncedViaCtid), tableNameToTable, emittedAt));
      } else {
        LOGGER.info("No streams will be synced via ctid");
      }

      final ComponentRunner<DebeziumRecord, DebeziumState> debezium = PostgresDebeziumComponentUtils.runner(database, initialDebeziumState);
      // Attempt to advance LSN past the target position. For versions of Postgres before PG15, this
      // ensures that there is an event that debezium will
      // receive that is after the target LSN.
      PostgresUtils.advanceLsn(database);
      final var lazyDebeziumOutput = debezium.collectRepeatedly(currentDebeziumState, initialDebeziumState);
      final Supplier<AutoCloseableIterator<AirbyteMessage>> incrementalIteratorSupplier =
          () -> AutoCloseableIterators.fromIterator(messageFactory.apply(lazyDebeziumOutput).iterator());

      if (initialSyncCtidIterators.isEmpty()) {
        return Collections.singletonList(incrementalIteratorSupplier.get());
      }

      if (streamsUnderVacuum.isEmpty()) {
        // This starts processing the WAL as soon as initial sync is complete, this is a bit different from
        // the current cdc syncs.
        // We finish the current CDC once the initial snapshot is complete and the next sync starts
        // processing the WAL
        return Stream
            .of(initialSyncCtidIterators, Collections.singletonList(AutoCloseableIterators.lazyIterator(incrementalIteratorSupplier, null)))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      } else {
        LOGGER.warn("Streams are under vacuuming, not going to process WAL");
        return initialSyncCtidIterators;
      }
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
