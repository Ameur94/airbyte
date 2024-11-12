package io.airbyte.integrations.source.mssql

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.discover.JdbcAirbyteStreamFactory
import io.airbyte.cdk.discover.MetaField
import io.airbyte.cdk.read.Stream
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.time.OffsetDateTime

@Singleton
@Primary
class MsSqlServerStreamFactory: JdbcAirbyteStreamFactory {
    override val globalCursor: MetaField? = null
    override val globalMetaFields: Set<MetaField> = emptySet()

    override fun decorateRecordData(timestamp: OffsetDateTime, globalStateValue: OpaqueStateValue?, stream: Stream, recordData: ObjectNode) {
        // do nothing
    }
}
