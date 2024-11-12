/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.command.CdcSourceConfiguration
import io.airbyte.cdk.command.FeatureFlag
import io.airbyte.cdk.command.JdbcSourceConfiguration
import io.airbyte.cdk.command.SourceConfigurationFactory
import io.airbyte.cdk.ssh.SshConnectionOptions
import io.airbyte.cdk.ssh.SshNoTunnelMethod
import io.airbyte.cdk.ssh.SshTunnelMethodConfiguration
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerCdcReplicationConfigurationSpecification
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerCursorBasedReplicationConfigurationSpecification
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerSourceConfigurationSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Duration

sealed interface MsSqlServerIncrementalReplicationConfiguration

data object MsSqlServerCursorBasedIncrementalReplicationConfiguration : MsSqlServerIncrementalReplicationConfiguration

data class MsSqlServerCdcIncrementalReplicationConfiguration(
    var initialWaitingSeconds: Int
) : MsSqlServerIncrementalReplicationConfiguration

class MsSqlServerSourceConfiguration(
    override val realHost: String,
    override val realPort: Int,
    override val sshTunnel: SshTunnelMethodConfiguration?,
    override val sshConnectionOptions: SshConnectionOptions,
    override val global: Boolean,
    override val maxSnapshotReadDuration: Duration?,
    override val checkpointTargetInterval: Duration,
    override val maxConcurrency: Int,
    override val resourceAcquisitionHeartbeat: Duration,
    override val debeziumHeartbeatInterval: Duration,
    override val jdbcUrlFmt: String,
    override val jdbcProperties: Map<String, String>,
    override val namespaces: Set<String>,
    val incrementalReplicationConfiguration: MsSqlServerIncrementalReplicationConfiguration,
) : JdbcSourceConfiguration, CdcSourceConfiguration {
    private val log = KotlinLogging.logger {}
    init {
        log.info {"global=$global"}
    }
}

@Singleton
class MsSqlServerSourceConfigurationFactory
@Inject
constructor(val featureFlags: Set<FeatureFlag>) :
    SourceConfigurationFactory<
        MsSqlServerSourceConfigurationSpecification, MsSqlServerSourceConfiguration> {
    private val log = KotlinLogging.logger {}

    constructor() : this(emptySet())

    override fun makeWithoutExceptionHandling(
        pojo: MsSqlServerSourceConfigurationSpecification,
    ): MsSqlServerSourceConfiguration {
        val replicationMethodPojo = pojo.replicationMethodJson
        val incrementalReplicationConfiguration = when (replicationMethodPojo) {
            is MsSqlServerCdcReplicationConfigurationSpecification -> MsSqlServerCdcIncrementalReplicationConfiguration(
                initialWaitingSeconds = replicationMethodPojo.initialWaitingSeconds ?: MsSqlServerCdcReplicationConfigurationSpecification.DEFAULT_INITIAL_WAITING_SECONDS
            )
            is MsSqlServerCursorBasedReplicationConfigurationSpecification -> MsSqlServerCursorBasedIncrementalReplicationConfiguration
            null -> TODO()
        }
        log.info {"replicationMethodPojo = $replicationMethodPojo, incrementalReplicationConfiguration=$incrementalReplicationConfiguration"}
        return MsSqlServerSourceConfiguration(
            realHost = pojo.host,
            realPort = pojo.port,
            sshTunnel = SshNoTunnelMethod,
            sshConnectionOptions = SshConnectionOptions.fromAdditionalProperties(emptyMap()),
            global = incrementalReplicationConfiguration is MsSqlServerCdcIncrementalReplicationConfiguration,
            maxSnapshotReadDuration = null,
            checkpointTargetInterval = Duration.ofHours(1),
            jdbcUrlFmt = "jdbc:sqlserver://%s:%d;databaseName=${pojo.database}",
            namespaces = pojo.schemas?.toSet()?: setOf(),
            jdbcProperties =
                mapOf("encrypt" to "false", "user" to pojo.username, "password" to pojo.password),
            maxConcurrency = 10,
            debeziumHeartbeatInterval = Duration.ofSeconds(15),
            resourceAcquisitionHeartbeat = Duration.ofSeconds(15),
            incrementalReplicationConfiguration = incrementalReplicationConfiguration
        )
    }
}
