package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.config.ConfigUtils
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.model.doi.JourneyPatternStop
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import fi.hsl.transitdata.rata_digitraffic.source.DoiSource
import fi.hsl.transitdata.rata_digitraffic.source.RataDigitrafficStationSource
import fi.hsl.transitdata.rata_digitraffic.utils.intervalFlow
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
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

    val platformChangesEnabled = config.getBoolean("application.platformChangesEnabled")

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
                    intervalFlow(Duration.ofDays(1))
                        //Update metadata daily
                        .mapLatest {
                            val today = LocalDate.now()

                            val trainTrips = async { doiSource.getTrainTrips(today, doiQueryFutureDays) }
                            val stops = async { doiSource.getStopPointsForRailwayStations(today) }
                            val journeyPatternStops = async { doiSource.getJourneyPatternStops(today) }

                            val stations = async { stationSource.getStations() }

                            return@mapLatest Metadata(trainTrips.await(), stops.await(), journeyPatternStops.await(), stations.await())
                        }
                        .collect { metadata ->
                            //Start message processor after metadata is first available
                            if (processor == null) {
                                processor = MessageHandler(context, platformChangesEnabled, doiTimezone, metadata)
                                app.launchWithHandler(processor!!)
                            } else {
                                processor!!.updateMetadata(metadata)
                            }
                        }
                }
            }
        }
    } catch (e: Exception) {
        log.error("Exception at main", e)
    }
}

//Container class for metadata that is needed to create trip updates for trains
data class Metadata(val trainTrips: Collection<TripInfo>, val stops: Collection<StopPoint>, val journeyPatternStops: Map<String, List<JourneyPatternStop>>, val stations: Collection<Station>)
