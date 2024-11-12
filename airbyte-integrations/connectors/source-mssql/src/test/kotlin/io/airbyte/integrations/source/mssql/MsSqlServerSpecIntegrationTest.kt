/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.command.CliRunner
import io.airbyte.cdk.command.SyncsTestFixture
import io.airbyte.cdk.output.BufferingOutputConsumer
import io.airbyte.cdk.util.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.SyncMode
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class MsSqlServerSpecIntegrationTest {
    @Test
    fun testSpec() {
        SyncsTestFixture.testSpec("expected_spec.json")
    }

    @Test
    fun testCheck() {
        val it = MsSqlServerContainerFactory.shared(MsSqlServerContainerFactory.SQLSERVER_2022)
        SyncsTestFixture.testCheck(MsSqlServerContainerFactory.config(it))
    }

    @Test
    fun testDiscover() {
        val it = MsSqlServerContainerFactory.shared(MsSqlServerContainerFactory.SQLSERVER_2022)
        val config = MsSqlServerContainerFactory.config(it)
        val discoverOutput: BufferingOutputConsumer = CliRunner.source("discover", config).run()
        Assertions.assertEquals(listOf(AirbyteCatalog().withStreams(listOf(
            AirbyteStream()
                .withName("id_name_and_born")
                .withJsonSchema(Jsons.readTree("""{"type":"object","properties":{"born":{"type":"string"},"name":{"type":"string"},"id":{"type":"number","airbyte_type":"integer"}}}"""))
                .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                .withSourceDefinedCursor(false)
                .withNamespace(config.schemas!![0])
                .withSourceDefinedPrimaryKey(listOf(listOf("id")))
                .withIsResumable(true),
            AirbyteStream()
                .withName("name_and_born")
                .withJsonSchema(Jsons.readTree("""{"type":"object","properties":{"born":{"type":"string"},"name":{"type":"string"}}}"""))
                .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
                .withSourceDefinedCursor(false)
                .withNamespace(config.schemas!![0])
        ))), discoverOutput.catalogs())
    }
}
