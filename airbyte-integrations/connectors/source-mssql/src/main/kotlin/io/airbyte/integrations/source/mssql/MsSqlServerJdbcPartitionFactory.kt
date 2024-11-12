package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.output.CatalogValidationFailureHandler
import io.airbyte.cdk.read.*
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

@Singleton
class MsSqlServerJdbcPartitionFactory(sharedState: DefaultJdbcSharedState,
                                      handler: CatalogValidationFailureHandler,
                                      selectQueryGenerator: SelectQueryGenerator,):
    DefaultJdbcPartitionFactory(sharedState,
        handler,
        selectQueryGenerator) {
    private val log = KotlinLogging.logger {}

    override fun streamState(streamFeedBootstrap: StreamFeedBootstrap): DefaultJdbcStreamState {
        val retVal = super.streamState(streamFeedBootstrap)
        log.info { "streamFeedBootstrap=$streamFeedBootstrap, retVal=$retVal" }
        return retVal
    }

    override fun create(streamFeedBootstrap: StreamFeedBootstrap): DefaultJdbcPartition? {
        val retVal = super.create(streamFeedBootstrap)
        log.info { "streamFeedBootStrap=$streamFeedBootstrap, retVal=$retVal" }
        return retVal
    }

    override fun split(
        unsplitPartition: DefaultJdbcPartition,
        opaqueStateValues: List<OpaqueStateValue>
    ): List<DefaultJdbcPartition> {
        val retVal = super.split(unsplitPartition, opaqueStateValues)
        log.info { "unsplitPartition=$unsplitPartition, opaqueStateValues=$opaqueStateValues, retVal=$retVal" }
        return retVal
    }
}
