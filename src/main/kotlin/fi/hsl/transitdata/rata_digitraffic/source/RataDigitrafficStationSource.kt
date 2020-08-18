package fi.hsl.transitdata.rata_digitraffic.source

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import okhttp3.Headers
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.pulsar.shade.org.apache.http.HttpStatus
import java.io.IOException

class RataDigitrafficStationSource(private val httpClient: OkHttpClient, private val endpoint: String = "https://rata.digitraffic.fi/api/v1") {
    companion object {
        private const val METADATA_STATIONS = "/metadata/stations"

        //https://www.digitraffic.fi/ohjeita/#sovelluksen-yksil%C3%B6iv%C3%A4t-otsikkotiedot
        private val IDENTIFICATION_HEADERS = Headers.headersOf(
            "Digitraffic-User", "HSL",
            "User-Agent", "transitdata-rata-digitraffic-source/${RataDigitrafficStationSource::class.java.`package`.implementationVersion ?: "dev"}"
        )
    }

    fun getStations(): List<Station> {
        val request = Request.Builder()
                .url(endpoint + METADATA_STATIONS)
                .headers(IDENTIFICATION_HEADERS)
                .build()
        val response = httpClient.newCall(request).execute()

        if (response.code == HttpStatus.SC_OK) {
            return JsonHelper.parseList<Station>(response.body!!.byteStream())
        } else {
            throw IOException("Unsuccessful HTTP request")
        }
    }
}