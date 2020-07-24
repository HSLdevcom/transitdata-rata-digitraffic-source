package fi.hsl.transitdata.rata_digitraffic.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import java.time.Duration

/**
 * Flow that emits incrementing value every time the specified duration has passed. The first value is emitted immediately
 */
fun intervalFlow(interval: Duration) = flow {
    var count = 0L
    while(true) {
        emit(count++)
        delay(interval.toMillis())
    }
}