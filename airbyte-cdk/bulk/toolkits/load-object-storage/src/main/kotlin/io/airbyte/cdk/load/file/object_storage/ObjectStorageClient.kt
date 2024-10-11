/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import io.airbyte.cdk.load.file.NoopProcessor
import io.airbyte.cdk.load.file.StreamProcessor
import java.io.InputStream
import java.io.OutputStream
import kotlinx.coroutines.flow.Flow

interface ObjectStorageClient<T : RemoteObject<*>, U : ObjectStorageStreamingUploadWriter> {
    suspend fun list(prefix: String): Flow<T>
    suspend fun move(remoteObject: T, toKey: String): T
    suspend fun <U> get(key: String, block: (InputStream) -> U): U
    suspend fun put(key: String, bytes: ByteArray): T
    suspend fun delete(remoteObject: T)
    suspend fun streamingUpload(key: String, block: suspend (U) -> Unit): T =
        streamingUpload(key, NoopProcessor, block)
    suspend fun <V : OutputStream> streamingUpload(
        key: String,
        streamProcessor: StreamProcessor<V>,
        block: suspend (U) -> Unit
    ): T
}

interface ObjectStorageStreamingUploadWriter {
    suspend fun write(bytes: ByteArray)
    suspend fun write(string: String) = write(string.toByteArray(Charsets.UTF_8))
}
