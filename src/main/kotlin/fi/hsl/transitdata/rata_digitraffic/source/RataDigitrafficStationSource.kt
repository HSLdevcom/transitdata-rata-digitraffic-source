package fi.hsl.transitdata.rata_digitraffic.source

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.zip.GZIPInputStream

class RataDigitrafficStationSource(private val httpClient: HttpClient, private val endpoint: String = "https://rata.digitraffic.fi/api/v1") {
    companion object {
        private const val METADATA_STATIONS = "/metadata/stations"
    }

    suspend fun getStations(): List<Station> {
        //TODO: maybe use OkHttp instead for automatic GZIP?
        val request = HttpRequest.newBuilder(URI.create(endpoint + METADATA_STATIONS)).header("Accept-Encoding", "gzip").build()
        val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).await()

        if (response.statusCode() == 200) {
            return withContext(Dispatchers.IO) {
                GZIPInputStream(response.body()).use { inputStream ->
                    JsonHelper.parseList<Station>(inputStream)
                }
            }
        } else {
            throw IOException("Unsuccessful HTTP request")
        }
    }
}