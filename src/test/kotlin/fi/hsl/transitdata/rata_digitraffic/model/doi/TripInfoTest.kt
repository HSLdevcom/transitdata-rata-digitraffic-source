package fi.hsl.transitdata.rata_digitraffic.model.doi

import org.junit.Assert.assertEquals
import org.junit.Test
import java.time.LocalDateTime

class TripInfoTest {
    @Test
    fun `Test start time is correct`() {
        val tripInfo = TripInfo("3001L", "20200101", "27:30:00", "28:30:00", 1, "1", "2", "L")

        assertEquals(LocalDateTime.of(2020, 1, 2, 3, 30), tripInfo.startTimeAsLocalDateTime)
    }

    @Test
    fun `Test end time is correct`() {
        val tripInfo = TripInfo("3001L", "20200101", "27:30:00", "28:30:00", 1, "1", "2", "L")

        assertEquals(LocalDateTime.of(2020, 1, 2, 4, 30), tripInfo.endTimeAsLocalDateTime)
    }
}