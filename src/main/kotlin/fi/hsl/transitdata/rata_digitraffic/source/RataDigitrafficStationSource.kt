package fi.hsl.transitdata.rata_digitraffic.source

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.*
import org.apache.pulsar.shade.org.apache.http.HttpStatus
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class RataDigitrafficStationSource(private val httpClient: OkHttpClient, private val endpoint: String = "https://rata.digitraffic.fi/api/v1") {
    companion object {
        private const val METADATA_STATIONS = "/metadata/stations"

        //https://www.digitraffic.fi/ohjeita/#sovelluksen-yksil%C3%B6iv%C3%A4t-otsikkotiedot
        private val IDENTIFICATION_HEADERS = Headers.headersOf(
            "Digitraffic-User", "HSL",
            "User-Agent", "transitdata-rata-digitraffic-source/${RataDigitrafficStationSource::class.java.`package`.implementationVersion ?: "dev"}"
        )
    }

    suspend fun getStations(): List<Station> = suspendCancellableCoroutine { cancellableContinuation ->
        val request = Request.Builder()
            .url(endpoint + METADATA_STATIONS)
            .headers(IDENTIFICATION_HEADERS)
            .build()

        val call = httpClient.newCall(request)
        cancellableContinuation.invokeOnCancellation { call.cancel() }

        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                cancellableContinuation.resumeWithException(e)
            }

            override fun onResponse(call: Call, response: Response) {
                if (response.code == HttpStatus.SC_OK) {
                    cancellableContinuation.resume(JsonHelper.parseList<Station>(response.body!!.byteStream()))
                } else {
                    cancellableContinuation.resumeWithException(IOException("Unsuccessful HTTP request (${response.code})"))
                }
            }
        })
    }
}