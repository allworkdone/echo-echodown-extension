package dev.brahmkshatriya.echo.extension.downloaders

import dev.brahmkshatriya.echo.common.models.Progress
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.extension.Downloader.okHttpDownload
import dev.brahmkshatriya.echo.extension.FFMpegHelper
import kotlinx.coroutines.flow.MutableStateFlow
import java.io.File

class HttpDownload {

    suspend fun httpDownload(
        file: File,
        progressFlow: MutableStateFlow<Progress>,
        source: Streamable.Source.Http,
    ): File {
        val isFFmpeg = source.type != Streamable.SourceType.Progressive
        try {
            if (isFFmpeg) ffmpegDownload(file, source, progressFlow)
            else okHttpDownload(file, source.request, progressFlow, MutableStateFlow(0L))
        } catch (e: Throwable) {
            file.delete()
            throw e // surface the real error instead of returning a deleted file
        }
        return file
    }

    private suspend fun ffmpegDownload(
        file: File,
        source: Streamable.Source.Http,
        progressFlow: MutableStateFlow<Progress>
    ): File {
        if (file.exists()) file.delete()

        // Each header line must end with CRLF; escape quotes for the command line.
        val headers = source.request.headers.entries.joinToString("") {
            "${it.key}: ${it.value}\r\n"
        }.replace("\"", "\\\"")

        val ffmpegCommand = buildString {
            // Fail after 15 s without data instead of hanging forever.
            append("-rw_timeout 15000000 ")
            if (headers.isNotEmpty()) append("-headers \"$headers\" ")
            append("-extension_picky 0 ")
            append("-i \"${source.request.url}\" ")
            append("-c copy ")
            append("\"${file.absolutePath}\"")
        }
        progressFlow.emit(Progress())
        var nextIsDuration = false
        var totalMs = 0L
        FFMpegHelper.execute(ffmpegCommand, {
            it ?: return@execute
            if (nextIsDuration) {
                totalMs = durationToTime(it) ?: totalMs
                progressFlow.tryEmit(Progress(size = totalMs))
            }
            nextIsDuration = it.contains("Duration:")
        }) {
            // size = total duration, progress = elapsed time, both in ms
            progressFlow.tryEmit(Progress(size = totalMs, progress = it.time.toLong()))
        }
        if (!file.exists() || file.length() == 0L) {
            throw IllegalStateException("FFmpeg produced no output for ${source.request.url}")
        }
        return file
    }

    private fun durationToTime(it: String): Long? {
        val match = DURATION_REGEX.find(it) ?: return null
        val (hours, minutes, seconds, centis) = match.destructured
        return (hours.toLong() * 3600 + minutes.toLong() * 60 + seconds.toLong()) * 1000 + centis.toLong() * 10
    }

    companion object {
        private val DURATION_REGEX = Regex("(\\d{2}):(\\d{2}):(\\d{2})\\.(\\d{2})")
    }
}
