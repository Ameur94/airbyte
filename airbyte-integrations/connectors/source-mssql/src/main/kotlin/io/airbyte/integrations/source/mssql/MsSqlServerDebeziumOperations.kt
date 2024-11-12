package io.airbyte.integrations.source.mssql

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.cdc.*
import io.airbyte.cdk.util.Jsons
import jakarta.inject.Singleton
import org.apache.kafka.connect.source.SourceRecord

@Singleton
class MsSqlServerDebeziumOperations:DebeziumOperations<MsSqlServerDebeziumPosition> {
    override fun position(offset: DebeziumOffset): MsSqlServerDebeziumPosition {
        return MsSqlServerDebeziumPosition()
    }

    override fun position(recordValue: DebeziumRecordValue): MsSqlServerDebeziumPosition? {
        return MsSqlServerDebeziumPosition()
    }

    override fun position(sourceRecord: SourceRecord): MsSqlServerDebeziumPosition? {
        return MsSqlServerDebeziumPosition()
    }

    override fun synthesize(): DebeziumInput {
        return DebeziumInput(isSynthetic = true, state = DebeziumState(DebeziumOffset(emptyMap()), null), properties = emptyMap())
    }

    override fun deserialize(opaqueStateValue: OpaqueStateValue, streams: List<Stream>): DebeziumInput {
        return DebeziumInput(isSynthetic = true, state = DebeziumState(DebeziumOffset(emptyMap()), null), properties = emptyMap())    }

    override fun deserialize(key: DebeziumRecordKey, value: DebeziumRecordValue): DeserializedRecord? {
        return null
    }

    override fun serialize(debeziumState: DebeziumState): OpaqueStateValue {
        return Jsons.objectNode()
    }
}

class MsSqlServerDebeziumPosition: Comparable<MsSqlServerDebeziumPosition> {
    override fun compareTo(other: MsSqlServerDebeziumPosition): Int {
        return 0
    }

}
