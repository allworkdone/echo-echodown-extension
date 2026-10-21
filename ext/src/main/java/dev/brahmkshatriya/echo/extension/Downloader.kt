package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.Progress
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.withContext
import okhttp3.Headers.Companion.toHeaders
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.RandomAccessFile
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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

    private const val CHUNK_SIZE = 1L * 1024 * 1024 // 1 MB per request
    private const val PARALLEL = 4 // pieces downloaded at the same time
    private const val RETRIES = 3

    /**
     * Downloads in 1 MB pieces with several requests at once. Servers like
     * YouTube send each new request fast before throttling it, so pieces are
     * much faster than one long request. Falls back to a single request when
     * the server doesn't support ranges.
     */
    suspend fun okHttpDownload(
        file: File,
        req: NetworkRequest,
        progressFlow: MutableSharedFlow<Progress>? = null,
        receiveFlow: MutableStateFlow<Long>? = null,
    ): File = withContext(Dispatchers.IO) {
        if (file.exists()) file.delete()

        val first = retry { openRange(req, 0, CHUNK_SIZE - 1) }
        when (first.code) {
            200 -> {
                // No range support: stream the whole file in one request.
                val len = first.body.contentLength()
                return@withContext download(
                    file, first.body.byteStream(), len, progressFlow, receiveFlow
                )
            }
            416 -> { // empty file
                first.close()
                file.createNewFile()
                return@withContext file
            }
        }

        val total = first.header("Content-Range")?.substringAfterLast('/')?.toLongOrNull()
        val firstBytes = first.use { it.body.bytes() }
        val received = AtomicLong(firstBytes.size.toLong())
        progressFlow?.tryEmit(Progress(total ?: -1L, received.get(), 0))

        RandomAccessFile(file, "rw").use { raf ->
            raf.write(firstBytes)
            coroutineScope {
                val ticker = launch {
                    var last = received.get()
                    while (isActive) {
                        delay(1_000)
                        val now = received.get()
                        receiveFlow?.value = now
                        progressFlow?.tryEmit(Progress(total ?: -1L, now, now - last))
                        last = now
                    }
                }
                if (total != null) {
                    val queue = Channel<Long>(Channel.UNLIMITED)
                    var pos = firstBytes.size.toLong()
                    while (pos < total) {
                        queue.trySend(pos); pos += CHUNK_SIZE
                    }
                    queue.close()
                    List(PARALLEL) {
                        launch {
                            for (start in queue) {
                                val end = minOf(start + CHUNK_SIZE, total) - 1
                                val bytes = fetchChunk(req, start, end, end - start + 1)
                                synchronized(raf) { raf.seek(start); raf.write(bytes) }
                                received.addAndGet(bytes.size.toLong())
                            }
                        }
                    }.joinAll()
                } else {
                    // Total size unknown: fetch pieces one by one until a short one.
                    var pos = firstBytes.size.toLong()
                    var lastSize = firstBytes.size.toLong()
                    while (lastSize >= CHUNK_SIZE) {
                        val bytes = fetchChunk(req, pos, pos + CHUNK_SIZE - 1, null)
                        raf.seek(pos); raf.write(bytes)
                        pos += bytes.size
                        lastSize = bytes.size.toLong()
                        received.addAndGet(lastSize)
                    }
                }
                ticker.cancel()
            }
        }

        if (total != null && file.length() != total) {
            throw IOException("Incomplete download: ${file.length()} of $total bytes")
        }
        receiveFlow?.value = received.get()
        progressFlow?.tryEmit(Progress(total ?: received.get(), received.get(), 0))
        file
    }

    private suspend fun openRange(req: NetworkRequest, start: Long, end: Long): Response {
        val headers = req.headers.toMutableMap().apply {
            put("Range", "bytes=$start-$end")
        }.toHeaders()
        val request = Request.Builder().url(req.url).headers(headers).build()
        val response = client.newCall(request).await()
        if (response.code !in listOf(200, 206, 416)) {
            response.close()
            throw IOException("Download failed: HTTP ${response.code} for ${req.url}")
        }
        return response
    }

    private suspend fun fetchChunk(
        req: NetworkRequest, start: Long, end: Long, expected: Long?
    ): ByteArray = retry {
        openRange(req, start, end).use { r ->
            when (r.code) {
                206 -> r.body.bytes().also {
                    if (expected != null && it.size.toLong() != expected)
                        throw IOException("Short piece: got ${it.size} of $expected bytes")
                }
                416 -> ByteArray(0)
                else -> throw IOException("Server stopped supporting pieces (HTTP ${r.code})")
            }
        }
    }

    private suspend fun <T> retry(block: suspend () -> T): T {
        var last: IOException? = null
        repeat(RETRIES) { attempt ->
            try {
                return block()
            } catch (e: IOException) {
                last = e
                delay(1_000L * (attempt + 1))
            }
        }
        throw last!!
    }
}
