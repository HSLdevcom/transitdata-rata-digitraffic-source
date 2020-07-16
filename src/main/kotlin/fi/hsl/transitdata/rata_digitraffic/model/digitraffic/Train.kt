package fi.hsl.transitdata.rata_digitraffic.model.digitraffic

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.LocalDate

@JsonIgnoreProperties(ignoreUnknown = true)
data class Train(
        val trainNumber: Int,
        val departureDate: LocalDate,
        val trainType: String,
        val trainCategory: String,
        val commuterLineID: String?, //null if train is not commuter train
        val runningCurrently: Boolean,
        val cancelled: Boolean,
        val deleted: Boolean?,
        val timetableType: String,
        val timeTableRows: List<TimetableRow>
) {
    val cancelledOrDeleted = cancelled || deleted == true
}