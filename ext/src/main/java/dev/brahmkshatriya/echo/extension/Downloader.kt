package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.Progress
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.withContext
import okhttp3.Headers.Companion.toHeaders
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.concurrent.TimeUnit

object Downloader {

    private const val BUFFER_SIZE = 512 * 1024

    suspend fun download(
        file: File,
        stream: InputStream,
        totalBytes: Long,
        progressFlow: MutableSharedFlow<Progress>? = null,
        receiveFlow: MutableStateFlow<Long>? = null,
        append: Boolean = false,
        startBytes: Long = 0L,
    ) = withContext(Dispatchers.IO) {
        progressFlow?.tryEmit(Progress(totalBytes, startBytes))

        stream.buffered(BUFFER_SIZE).use { bis ->
            // Append when resuming a partial download, otherwise overwrite.
            val fos = FileOutputStream(file, append)
            fos.buffered(BUFFER_SIZE).use { out ->
                val buffer = ByteArray(BUFFER_SIZE)
                var received = startBytes

                var lastTime = 0L // 0 so the first chunk is reported immediately
                var lastBytes = startBytes

                while (true) {
                    val bytesRead = bis.read(buffer).takeIf { it >= 0 } ?: break
                    out.write(buffer, 0, bytesRead)
                    received += bytesRead
                    receiveFlow?.value = received

                    val now = System.currentTimeMillis()
                    if (now - lastTime >= 1_000) {
                        val speed = if (lastTime == 0L) 0L else received - lastBytes
                        lastBytes = received
                        lastTime = now
                        progressFlow?.tryEmit(Progress(totalBytes, received, speed))
                    }
                }
                progressFlow?.tryEmit(Progress(totalBytes, received, 0))
            }
        }
        file
    }

    val client by lazy {
        OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build()
    }

    suspend fun okHttpDownload(
        file: File,
        req: NetworkRequest,
        progressFlow: MutableSharedFlow<Progress>? = null,
        receiveFlow: MutableStateFlow<Long>? = null,
    ): File {
        val fileLength = if (file.exists()) file.length() else 0L
        val headers = req.headers.toMutableMap().apply {
            if (fileLength > 0) put("Range", "bytes=$fileLength-")
        }.toHeaders()
        val request = Request.Builder().url(req.url).headers(headers).build()
        val response = client.newCall(request).await()

        val resumed = response.code == 206
        if (!response.isSuccessful) {
            response.close()
            if (response.code == 416 && fileLength > 0) {
                // Stale partial file: start over without a Range header.
                file.delete()
                return okHttpDownload(file, req, progressFlow, receiveFlow)
            }
            throw IOException("Download failed: HTTP ${response.code} for ${req.url}")
        }

        val startBytes = if (resumed) fileLength else 0L
        receiveFlow?.value = startBytes
        val contentLength = response.body.contentLength()
        val totalBytes = if (contentLength >= 0) startBytes + contentLength else -1L
        return download(
            file,
            response.body.byteStream(),
            totalBytes,
            progressFlow,
            receiveFlow,
            append = resumed,
            startBytes = startBytes
        )
    }
}
