package fi.hsl.transitdata.rata_digitraffic

import com.google.transit.realtime.GtfsRealtime
import fi.hsl.common.gtfsrt.FeedMessageFactory
import fi.hsl.common.transitdata.JoreDateTime
import fi.hsl.common.transitdata.proto.InternalMessages.TripCancellation
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import mu.KotlinLogging
import java.time.Instant
import java.util.function.Consumer

class TripCancellationBuilder(private val doiTripMatcher: DoiTripMatcher) {

    fun buildTripCancellation(train: Train): TripCancellation? {
        val tripInfo = doiTripMatcher.matchTrainToTrip(train) ?: return null

        val builder = TripCancellation.newBuilder()
                .setTripId(tripInfo.dvjId)
                .setRouteId(tripInfo.routeId)
                .setDirectionId(tripInfo.directionId)
                .setStartDate(tripInfo.startDate)
                .setStartTime(tripInfo.startTime)

        return builder.build()
    }
}