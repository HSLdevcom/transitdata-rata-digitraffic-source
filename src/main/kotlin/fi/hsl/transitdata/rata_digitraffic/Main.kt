package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.rata_digitraffic.source.DoiSource
import fi.hsl.transitdata.rata_digitraffic.source.RataDigitrafficStationSource
import fi.hsl.transitdata.rata_digitraffic.utils.intervalFlow
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.launch
import mu.KotlinLogging
import okhttp3.OkHttpClient
import java.io.File
import java.sql.DriverManager
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.*

fun main(vararg args: String) {
    val log = KotlinLogging.logger {}

    val config = ConfigParser.createConfig()

    val doiTimezone = ZoneId.of(config.getString("doi.timezone"))
    val doiQueryFutureDays = config.getLong("doi.queryFutureDays")

    val httpClient = OkHttpClient()

    //Default path is what works with Docker out-of-the-box. Override with a local file if needed
    val secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string")
    val connectionString = Scanner(File(secretFilePath))
            .useDelimiter("\\Z").next()

    try {
        DriverManager.getConnection(connectionString).use { connection ->
            val doiSource = DoiSource(connection)
            val stationSource = RataDigitrafficStationSource(httpClient)


            PulsarApplication.newInstance(config).use { app ->
                val context = app.context
                val healthServer = context.healthServer

                var processor: MessageHandler? = null


                GlobalScope.launch {
                    var doiStopMatcher: DoiStopMatcher? = null
                    var doiTripMatcher: DoiTripMatcher? = null

                    intervalFlow(Duration.ofDays(1))
                        //Update metadata daily
                        .mapLatest {
                            val trainTrips = doiSource.getTrainTrips(LocalDate.now(), doiQueryFutureDays)
                            val stops = doiSource.getStopPointsForRailwayStations(LocalDate.now())

                            val stations = stationSource.getStations()

                            return@mapLatest Triple(trainTrips, stops, stations)
                        }
                        .collect { (trainTrips, stops, stations) ->
                            doiStopMatcher = DoiStopMatcher(stops, stations)
                            doiTripMatcher = DoiTripMatcher(doiTimezone, trainTrips, doiStopMatcher!!)

                            //Start message processor after metadata is first available
                            if (processor == null) {
                                processor = MessageHandler(context, doiStopMatcher!!, doiTripMatcher!!)
                                app.launchWithHandler(processor!!)
                            } else {
                                processor!!.updateDoiMatchers(doiStopMatcher!!, doiTripMatcher!!)
                            }
                        }
                }
            }
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}