package io.airbyte.integrations.base.destination.typing_deduping

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.integrations.destination.async.deser.StreamAwareDataTransformer
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMeta
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.lang3.tuple.ImmutablePair
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.ArrayDeque
import java.util.stream.Collectors

private val log = KotlinLogging.logger {  }

open class SizeBasedDataTransformer(
    private val parsedCatalog: ParsedCatalog,
    private val defaultNamespace: String,
    private val maxFieldSize: Int = Int.MAX_VALUE,
    private val maxRecordSize: Int = Int.MAX_VALUE
) :
    StreamAwareDataTransformer {
    @JvmRecord
    private data class ScalarNodeModification(val size: Int, val removedSize: Int, val shouldNull: Boolean)

    data class TransformationInfo(val originalBytes: Int, val removedBytes: Int, val node: JsonNode?, val meta: AirbyteRecordMessageMeta)

    /*
   * This method walks the Json tree nodes and does the following
   *
   * 1. Collect the original bytes using UTF-8 charset. This is to avoid double walking the tree if
   * the total size > maxRecordSize This is to optimize for best case (see worst case as 4 below) that most of
   * the data will be <= maxRecordSize and only few offending varchars > maxRecordSize.
   *
   * 2. Replace all TextNodes with Null nodes if they are greater than maxRecordSize.
   *
   * 3. Verify if replacing the varchars with NULLs brought the record size down to < 16MB. This
   * includes verifying the original bytes and transformed bytes are below the record size limit.
   *
   * 4. If 3 is false, this is the worst case scenarios where we try to resurrect PKs and cursors and
   * trash the rest of the record.
   *
   */
    override fun transform(
        streamDescriptor: StreamDescriptor?,
        data: JsonNode?,
        meta: AirbyteRecordMessageMeta?
    ): Pair<JsonNode?, AirbyteRecordMessageMeta?> {
        val startTime = System.currentTimeMillis()
        log.debug{"Traversing the record to NULL fields for redshift size limitations"}
        val namespace =
            if ((streamDescriptor!!.namespace != null && streamDescriptor.namespace.isNotEmpty())) streamDescriptor.namespace else defaultNamespace
        val streamConfig: StreamConfig =
            parsedCatalog.getStream(namespace, streamDescriptor.name)
        val cursorField: Optional<String> =
            streamConfig.cursor.map(ColumnId::originalName)
        // convert List<ColumnId> to Set<ColumnId> for faster lookup
        val primaryKeys: Set<String> =
            streamConfig.primaryKey.stream().map(ColumnId::originalName).collect(
                Collectors.toSet()
            )
        val syncMode: DestinationSyncMode = streamConfig.destinationSyncMode
        val transformationInfo = clearLargeFields(data)
        val originalBytes = transformationInfo.originalBytes
        val transformedBytes = transformationInfo.originalBytes - transformationInfo.removedBytes
        // We check if the transformedBytes has solved the record limit.
        log.debug{"Traversal complete in ${System.currentTimeMillis() - startTime} ms"}
        if (originalBytes > maxRecordSize && transformedBytes > maxRecordSize) {
            // If we have reached here with a bunch of small varchars constituted to becoming a large record,
            // person using Redshift for this data should re-evaluate life choices.
            log.warn {
                "Record size before transformation $originalBytes, after transformation $transformedBytes bytes exceeds 16MB limit"
            }
            val minimalNode = constructMinimalJsonWithPks(data, primaryKeys, cursorField)
            if (minimalNode.isEmpty && syncMode == DestinationSyncMode.APPEND_DEDUP) {
                // Fail the sync if PKs are missing in DEDUPE, no point sending an empty record to destination.
                throw RuntimeException("Record exceeds size limit, cannot transform without PrimaryKeys in DEDUPE sync")
            }
            // Preserve original changes
            val changes: MutableList<AirbyteRecordMessageMetaChange> = ArrayList()
            changes.add(
                AirbyteRecordMessageMetaChange()
                    .withField("all").withChange(AirbyteRecordMessageMetaChange.Change.NULLED)
                    .withReason(AirbyteRecordMessageMetaChange.Reason.DESTINATION_RECORD_SIZE_LIMITATION)
            )
            if (meta != null && meta.changes != null) {
                changes.addAll(meta.changes)
            }
            return Pair(minimalNode, AirbyteRecordMessageMeta().withChanges(changes))
        }
        if (meta != null && meta.changes != null) {
            // The underlying list of AirbyteRecordMessageMeta is mutable
            transformationInfo.meta.changes.addAll(meta.changes)
        }
        // We intentionally don't deep copy for transformation to avoid memory bloat.
        // The caller already has the reference of original jsonNode but returning again in
        // case we choose to deepCopy in future for thread-safety.
        return Pair(data, transformationInfo.meta)
    }

    private fun shouldTransformScalarNode(
        node: JsonNode
    ): ScalarNodeModification {
        val bytes: Int
        if (node.isTextual) {
            val originalBytes = getByteSize(
                node.asText()
            ) + 2 // for quotes
            if (getByteSize(node.asText()) > maxFieldSize) {
                return ScalarNodeModification(
                    originalBytes,  // size before nulling
                    originalBytes - 4,  // account 4 bytes for null string
                    true
                )
            }
            bytes = originalBytes
        } else if (node.isNumber) {
            // Serialize exactly for numbers to account for Scientific notation converted to full value.
            // This is what we send over wire for persistence.
            bytes = getByteSize(Jsons.serialize<JsonNode?>(node))
        } else if (node.isBoolean) {
            bytes = getByteSize(node.toString())
        } else if (node.isNull) {
            bytes = 4 // for "null"
        } else {
            bytes = 0
        }
        return ScalarNodeModification(
            bytes,  // For all other types, just return bytes
            0,
            false
        )
    }

    fun clearLargeFields(
        rootNode: JsonNode?
    ): TransformationInfo {
        // Walk the tree and transform Varchars that exceed the limit
        // We are intentionally not checking the whole size upfront to check if it exceeds record size limit to
        // optimize for best case.

        var originalBytes = 0
        var removedBytes = 0
        // We accumulate nested keys in jsonPath format for adding to airbyte changes.
        val stack: Deque<ImmutablePair<String, JsonNode?>> = ArrayDeque()
        val changes: MutableList<AirbyteRecordMessageMetaChange> = ArrayList()

        // This was intentionally done using Iterative DFS to avoid stack overflow for large records.
        // This will ensure we are allocating on heap and not on stack.
        stack.push(ImmutablePair.of("$", rootNode))
        while (!stack.isEmpty()) {
            val jsonPathNodePair = stack.pop()
            val currentNode = jsonPathNodePair.right
            if (currentNode!!.isObject) {
                originalBytes += CURLY_BRACES_BYTE_SIZE
                val fields = currentNode.fields()
                while (fields.hasNext()) {
                    val field = fields.next()
                    originalBytes += getByteSize(field.key) + OBJECT_COLON_QUOTES_COMMA_BYTE_SIZE // for quotes, colon, comma
                    val jsonPathKey = "${jsonPathNodePair.left}.${field.key}"
                    // TODO: Little difficult to unify this logic in Object & Array, find a way later
                    // Push only non-scalar nodes to stack. For scalar nodes, we need reference of parent to do in-place
                    // update.
                    if (field.value.isContainerNode) {
                        stack.push(ImmutablePair.of(jsonPathKey, field.value))
                    } else {
                        val shouldTransform = shouldTransformScalarNode(field.value)
                        if (shouldTransform.shouldNull) {
                            removedBytes += shouldTransform.removedSize
                            // DO NOT do this if this code every modified to a multithreading call stack
                            field.setValue(Jsons.jsonNode<Any?>(null))
                            changes.add(
                                AirbyteRecordMessageMetaChange()
                                    .withField(jsonPathKey)
                                    .withChange(AirbyteRecordMessageMetaChange.Change.NULLED)
                                    .withReason(AirbyteRecordMessageMetaChange.Reason.DESTINATION_FIELD_SIZE_LIMITATION)
                            )
                        }
                        originalBytes += shouldTransform.size
                    }
                }
                originalBytes -= 1 // remove extra comma from last key-value pair
            } else if (currentNode.isArray) {
                originalBytes += SQUARE_BRACKETS_BYTE_SIZE
                val arrayNode = currentNode as ArrayNode?
                // We cannot use foreach here as we need to update the array in place.
                for (i in 0 until arrayNode!!.size()) {
                    val childNode = arrayNode[i]
                    val jsonPathKey = "${jsonPathNodePair.left}[$i]"
                    if (childNode.isContainerNode) stack.push(ImmutablePair.of(jsonPathKey, childNode))
                    else {
                        val shouldTransform = shouldTransformScalarNode(childNode)
                        if (shouldTransform.shouldNull) {
                            removedBytes += shouldTransform.removedSize
                            // DO NOT do this if this code every modified to a multithreading call stack
                            arrayNode[i] = Jsons.jsonNode<Any?>(null)
                            changes.add(
                                AirbyteRecordMessageMetaChange()
                                    .withField(jsonPathKey)
                                    .withChange(AirbyteRecordMessageMetaChange.Change.NULLED)
                                    .withReason(AirbyteRecordMessageMetaChange.Reason.DESTINATION_FIELD_SIZE_LIMITATION)
                            )
                        }
                        originalBytes += shouldTransform.size
                    }
                }
                originalBytes += if (!currentNode.isEmpty) currentNode.size() - 1 else 0 // for commas
            } else { // Top level scalar node is a valid json
                originalBytes += shouldTransformScalarNode(currentNode).size
            }
        }

        if (removedBytes != 0) {
            log.info {
                "Original record size $originalBytes bytes, Modified record size ${originalBytes - removedBytes} bytes"
            }
        }
        return TransformationInfo(originalBytes, removedBytes, rootNode, AirbyteRecordMessageMeta().withChanges(changes))
    }

    private fun constructMinimalJsonWithPks(rootNode: JsonNode?, primaryKeys: Set<String>, cursorField: Optional<String>): JsonNode {
        val minimalNode = Jsons.emptyObject() as ObjectNode
        // We only iterate for top-level fields in the root object, since we only support PKs and cursor in
        // top level keys.
        if (rootNode!!.isObject) {
            val fields = rootNode.fields()
            while (fields.hasNext()) {
                val field = fields.next()
                if (!field.value.isContainerNode) {
                    if (primaryKeys.contains(field.key) || cursorField.isPresent && cursorField.get() == field.key) {
                        // Make a deepcopy into minimalNode of PKs and cursor fields and values,
                        // without deepcopy, we will re-reference the original Tree's nodes.
                        // god help us if someone set a PK on non-scalar field, and it reached this point, only do at root
                        // level
                        minimalNode.set<JsonNode>(field.key, field.value.deepCopy())
                    }
                }
            }
        } else {
            log.error{"Encountered ${rootNode.nodeType} as top level JSON field, this is not supported"}
            // This should have caught way before it reaches here. Just additional safety.
            throw RuntimeException("Encountered ${rootNode.nodeType} as top level JSON field, this is not supported")
        }
        return minimalNode
    }

    companion object {
        private val CURLY_BRACES_BYTE_SIZE = getByteSize("{}")
        private val SQUARE_BRACKETS_BYTE_SIZE = getByteSize("[]")
        private val OBJECT_COLON_QUOTES_COMMA_BYTE_SIZE = getByteSize("\"\":,")

        private fun getByteSize(value: String): Int {
            return value.toByteArray(StandardCharsets.UTF_8).size
        }
    }
}
