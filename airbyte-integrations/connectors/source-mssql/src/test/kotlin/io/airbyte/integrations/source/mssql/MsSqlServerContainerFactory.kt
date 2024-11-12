/* Copyright (c) 2024 Airbyte, Inc., all rights reserved. */
package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.testcontainers.TestContainerFactory
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerCursorBasedReplicationConfigurationSpecification
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerReplicationMethodConfigurationSpecification
import io.airbyte.integrations.source.mssql.config_spec.MsSqlServerSourceConfigurationSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.StringUtils
import org.testcontainers.containers.Container
import org.testcontainers.containers.MSSQLServerContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import java.sql.Statement
import kotlin.random.Random

object MsSqlServerContainerFactory {
    const val COMPATIBLE_NAME = "mcr.microsoft.com/mssql/server"
    const val SQLSERVER_2022 = "mcr.microsoft.com/mssql/server:2022-latest"
    private val log = KotlinLogging.logger {}

    init {
        TestContainerFactory.register(COMPATIBLE_NAME, ::MSSQLServerContainer)
    }

    sealed interface MysqlContainerModifier :
        TestContainerFactory.ContainerModifier<MSSQLServerContainer<*>>

    data object WithNetwork : MysqlContainerModifier {
        override fun modify(container: MSSQLServerContainer<*>) {
            container.withNetwork(Network.newNetwork())
        }
    }

    data object WithCdcOff : MysqlContainerModifier {
        override fun modify(container: MSSQLServerContainer<*>) {
            container.withCommand("--skip-log-bin")
        }
    }

    fun exclusive(
        imageName: String,
        vararg modifiers: MysqlContainerModifier,
    ): MSSQLServerContainer<*> {
        val dockerImageName =
            DockerImageName.parse(imageName).asCompatibleSubstituteFor(COMPATIBLE_NAME)
        return TestContainerFactory.exclusive(dockerImageName, *modifiers)
    }

    fun shared(
        imageName: String,
        vararg modifiers: MysqlContainerModifier,
    ): MSSQLServerContainer<*> {
        val dockerImageName =
            DockerImageName.parse(imageName).asCompatibleSubstituteFor(COMPATIBLE_NAME)
        return TestContainerFactory.shared(dockerImageName, *modifiers)
    }

    @JvmStatic
    fun config(
        msSQLContainer: MSSQLServerContainer<*>
    ): MsSqlServerSourceConfigurationSpecification {
        val schemaName = "schema_"+RandomStringUtils.insecure().nextAlphabetic(16);
        val config = MsSqlServerSourceConfigurationSpecification().apply {
            host = msSQLContainer.host
            port = msSQLContainer.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT)
            username = msSQLContainer.username
            password = msSQLContainer.password
            jdbcUrlParams = ""
            database = "master"
            schemas = arrayOf(schemaName)
            replicationMethodJson = MsSqlServerCursorBasedReplicationConfigurationSpecification()
        }
        JdbcConnectionFactory(MsSqlServerSourceConfigurationFactory().make(config)).get().use {connection ->
            connection.isReadOnly = false
            connection.createStatement().use { stmt: Statement ->
                stmt.execute("CREATE SCHEMA $schemaName")
            }
            connection.createStatement().use { stmt: Statement ->
                stmt.execute("CREATE TABLE $schemaName.name_and_born(name VARCHAR(200), born DATETIMEOFFSET(7));")
                stmt.execute("CREATE TABLE $schemaName.id_name_and_born(id INTEGER PRIMARY KEY, name VARCHAR(200), born DATETIMEOFFSET(7));")
            }
            connection.createStatement().use { stmt: Statement ->
                stmt.execute("INSERT INTO $schemaName.name_and_born (name, born) VALUES ('foo', '2022-03-21 15:43:15.45'), ('bar', '2022-10-22 01:02:03.04')")
                stmt.execute("INSERT INTO $schemaName.id_name_and_born (id, name, born) VALUES (1, 'foo', '2022-03-21 15:43:15.45'), (2, 'bar', '2022-10-22 01:02:03.04')")
            }
        }
        return config
    }

    @JvmStatic
    fun cdcConfig(
        msSQLContainer: MSSQLServerContainer<*>
    ): MsSqlServerSourceConfigurationSpecification =
        MsSqlServerSourceConfigurationSpecification().apply {
            host = msSQLContainer.host
            port = msSQLContainer.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT)
            username = msSQLContainer.username
            password = msSQLContainer.password
            jdbcUrlParams = ""
            database = "dbo"
        }

    fun MSSQLServerContainer<*>.execAsRoot(sql: String) {
        val cleanSql: String = sql.trim().removeSuffix(";") + ";"
        log.info { "Executing SQL as root: $cleanSql" }
        val result: Container.ExecResult =
            execInContainer("mysql", "-u", "root", "-ptest", "-e", cleanSql)
        for (line in (result.stdout ?: "").lines()) {
            log.info { "STDOUT: $line" }
        }
        for (line in (result.stderr ?: "").lines()) {
            log.info { "STDOUT: $line" }
        }
        if (result.exitCode == 0) {
            return
        }
        log.error { "Exit code ${result.exitCode}" }
        throw RuntimeException("Failed to execute query $cleanSql")
    }
}
