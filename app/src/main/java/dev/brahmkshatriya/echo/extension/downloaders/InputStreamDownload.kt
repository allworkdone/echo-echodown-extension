package dev.brahmkshatriya.echo.extension.downloaders

import dev.brahmkshatriya.echo.common.models.Progress
import dev.brahmkshatriya.echo.extension.Downloader.download
import kotlinx.coroutines.flow.MutableStateFlow
import java.io.File
import java.io.InputStream

class InputStreamDownload {

    suspend fun inputStreamDownload(
        file: File,
        progressFlow: MutableStateFlow<Progress>,
        stream: InputStream,
        totalBytes: Long
    ): File {
        // Per-download flow so concurrent downloads don't overwrite each other.
        val receiveFlow = MutableStateFlow(0L)
        try {
            download(file, stream, totalBytes, progressFlow, receiveFlow)
        } catch (e: Throwable) {
            file.delete()
            throw e // surface the real error instead of returning a deleted file
        }
        return file
    }
}
