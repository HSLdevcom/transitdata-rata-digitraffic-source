package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.rata_digitraffic.source.DoiSource
import fi.hsl.transitdata.rata_digitraffic.source.RataDigitrafficStationSource
import mu.KotlinLogging
import okhttp3.OkHttpClient
import java.io.File
import java.sql.DriverManager
import java.time.ZoneId
import java.util.*

fun main(vararg args: String) {
    val log = KotlinLogging.logger {}

    val config = ConfigParser.createConfig()

    val doiTimezone = ZoneId.of(config.getString("doi.timezone"))
    val doiQueryFutureDays = config.getInt("doi.queryFutureDays")

    val client = OkHttpClient()

    //Default path is what works with Docker out-of-the-box. Override with a local file if needed
    val secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string")
    val connectionString = Scanner(File(secretFilePath))
            .useDelimiter("\\Z").next()

    try {
        DriverManager.getConnection(connectionString).use { connection ->
            val doiSource = DoiSource(connection)
            val stationSource = RataDigitrafficStationSource(client)
            val stations = stationSource.getStations()
            val doiStopMatcher = DoiStopMatcher.newInstance(doiSource, stations)
            val doiTripMatcher = DoiTripMatcher.newInstance(doiTimezone, doiQueryFutureDays, doiSource, doiStopMatcher)
            PulsarApplication.newInstance(config).use { app ->
                val context = app.context
                val processor = MessageHandler(context, doiStopMatcher, doiTripMatcher)
                val healthServer = context.healthServer
                app.launchWithHandler(processor)
            }
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}