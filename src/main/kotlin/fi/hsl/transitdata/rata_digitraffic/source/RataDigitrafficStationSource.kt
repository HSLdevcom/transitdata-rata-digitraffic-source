package fi.hsl.transitdata.rata_digitraffic.source

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.pulsar.shade.org.apache.http.HttpStatus
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.zip.GZIPInputStream


class RataDigitrafficStationSource(private val httpClient: OkHttpClient, private val endpoint: String = "https://rata.digitraffic.fi/api/v1") {
    companion object {
        private const val METADATA_STATIONS = "/metadata/stations"
    }

    fun getStations(): List<Station> {
        val request = Request.Builder().url(endpoint + METADATA_STATIONS).build()
        val response = httpClient.newCall(request).execute()

        if (response.code == HttpStatus.SC_OK) {
            return JsonHelper.parseList<Station>(response.body!!.byteStream())
        } else {
            throw IOException("Unsuccessful HTTP request")
        }
    }
}