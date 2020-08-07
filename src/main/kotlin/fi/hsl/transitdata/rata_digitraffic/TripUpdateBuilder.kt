package fi.hsl.transitdata.rata_digitraffic

import com.google.transit.realtime.GtfsRealtime
import fi.hsl.common.gtfsrt.FeedMessageFactory
import fi.hsl.common.transitdata.RouteIdUtils
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.TimetableRow
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import mu.KotlinLogging
import java.time.Instant

class TripUpdateBuilder(private val doiStopMatcher: DoiStopMatcher, private val doiTripMatcher: DoiTripMatcher) {
    private val log = KotlinLogging.logger {}

    fun buildTripUpdate(train: Train, timestamp: Instant): GtfsRealtime.FeedMessage? {
        val tripInfo = doiTripMatcher.matchTrainToTrip(train) ?: return null

        val trip = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(RouteIdUtils.normalizeRouteId(tripInfo.routeId))
                .setStartDate(tripInfo.startDate)
                .setStartTime(tripInfo.startTime)
                .setDirectionId(tripInfo.directionId - 1)
                .setScheduleRelationship(
                    if (train.cancelledOrDeleted)
                        GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED
                    else
                        GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED
                )
                .build()

        val tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(trip)
                .setTimestamp(timestamp.epochSecond)

        //Train was cancelled, no need to add stop time updates
        if (trip.scheduleRelationship == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return FeedMessageFactory.createDifferentialFeedMessage(tripInfo.dvjId, tripUpdateBuilder.build(), timestamp.epochSecond)
        }

        val commercialStopRows = train.timeTableRows.filter { timetableRow -> timetableRow.trainStopping && timetableRow.commercialStop == true }

        val stopTimeUpdates = mergeTimetableRows(commercialStopRows).mapNotNull { timetableRow ->
            val stationShortCode = (timetableRow.arrival?.stationShortCode ?: timetableRow.departure?.stationShortCode)
            val track = (timetableRow.arrival?.commercialTrack ?: timetableRow.departure?.commercialTrack)

            //Match station to DOI stop. If the track is unknown, use track 1 to match to any possible stop
            val doiStop = doiStopMatcher.getStopPointForStationAndTrack(stationShortCode!!, track?.toIntOrNull() ?: 1)
            if (doiStop == null) {
                log.warn("No stop found for station {} and track {}", stationShortCode, track)
                return null
            }

            val stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(doiStop.stopNumber)

            if (timetableRow.isCancelled) {
                return@mapNotNull stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build()
            } else if (timetableRow.arrival?.liveEstimateTime == null
                    && timetableRow.arrival?.actualTime == null
                    && timetableRow.departure?.liveEstimateTime == null
                    && timetableRow.departure?.actualTime == null) {
                return@mapNotNull stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA).build()
            } else {
                val arrival = timetableRow.arrival?.let { timetableRowToStopTimeEvent(it) }
                val departure = timetableRow.departure?.let { timetableRowToStopTimeEvent(it) }

                return@mapNotNull stopTimeUpdateBuilder
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .setArrival((arrival ?: departure)!!)
                        .setDeparture((departure ?: arrival)!!)
                        .build()
            }
        }

        tripUpdateBuilder.addAllStopTimeUpdate(stopTimeUpdates)

        return FeedMessageFactory.createDifferentialFeedMessage(tripInfo.dvjId, tripUpdateBuilder.build(), timestamp.epochSecond)
    }

    private fun timetableRowToStopTimeEvent(timetableRow: TimetableRow): GtfsRealtime.TripUpdate.StopTimeEvent {
        val builder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime((timetableRow.actualTime ?: timetableRow.liveEstimateTime)!!.toEpochSecond())

        if (timetableRow.actualTime != null) {
            builder.uncertainty = 0
        }

        return builder.build()
    }

    private fun mergeTimetableRows(timetableRows: Collection<TimetableRow>): Collection<MergedTimetableRow> {
        val mergedTimetableRows = mutableListOf<MergedTimetableRow>()

        var mergedTimetableRow = MergedTimetableRow()
        for (timetableRow in timetableRows) {
            if (timetableRow.type == TimetableRow.TimetableRowType.DEPARTURE) {
                mergedTimetableRows += mergedTimetableRow.copy(departure = timetableRow)
            } else if (timetableRow.type == TimetableRow.TimetableRowType.ARRIVAL) {
                mergedTimetableRow = mergedTimetableRow.copy(arrival = timetableRow)
            }
        }
        mergedTimetableRows += mergedTimetableRow

        return mergedTimetableRows.toList()
    }

    private data class MergedTimetableRow(val arrival: TimetableRow? = null, val departure: TimetableRow? = null) {
        val hasArrival = arrival != null
        val hasDeparture = departure != null

        //Timetable row is cancelled if both arrival and departure are cancelled or if other is cancelled and other is null (for first and last rows)
        val isCancelled = (arrival?.cancelled ?: true) && (departure?.cancelled ?: true)
    }
}