package fi.hsl.transitdata.rata_digitraffic.model.digitraffic

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.ZonedDateTime

@JsonIgnoreProperties(ignoreUnknown = true)
data class TimetableRow(
        val stationShortCode: String,
        val type: TimetableRowType,
        val trainStopping: Boolean,
        val commercialStop: Boolean?,
        val commercialTrack: String?,
        val cancelled: Boolean,
        val scheduledTime: ZonedDateTime,
        val liveEstimateTime: ZonedDateTime?,
        val actualTime: ZonedDateTime?,
        val unknownDelay: Boolean?
) {
    enum class TimetableRowType {
        DEPARTURE, ARRIVAL
    }
}