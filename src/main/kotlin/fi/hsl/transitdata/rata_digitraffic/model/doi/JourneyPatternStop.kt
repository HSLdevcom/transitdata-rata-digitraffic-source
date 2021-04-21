package fi.hsl.transitdata.rata_digitraffic.model.doi

/**
 * Represents a stop in a journey pattern (i.e. a group of trips sharing the same route)
 */
data class JourneyPatternStop(val journeyPatternId: String, val stopNumber: String, val sequenceNumber: Int) : Comparable<JourneyPatternStop> {
    override fun compareTo(other: JourneyPatternStop): Int {
        return if (journeyPatternId != other.journeyPatternId) {
            journeyPatternId.compareTo(other.journeyPatternId)
        } else {
            sequenceNumber.compareTo(sequenceNumber)
        }
    }
}