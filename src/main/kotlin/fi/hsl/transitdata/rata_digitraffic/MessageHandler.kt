package fi.hsl.transitdata.rata_digitraffic

import com.google.transit.realtime.GtfsRealtime
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import mu.KotlinLogging
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import java.time.Instant


class MessageHandler(context: PulsarApplicationContext, var doiStopMatcher: DoiStopMatcher, var doiTripMatcher: DoiTripMatcher) : IMessageHandler {

    private val log = KotlinLogging.logger {}

    private val consumer: Consumer<ByteArray> = context.consumer
    private val producer: Producer<ByteArray> = context.producer

    override fun handleMessage(received: Message<Any>) {
        try {
            if (TransitdataSchema.hasProtobufSchema(received, TransitdataProperties.ProtobufSchema.MqttRawMessage)) {
                val timestamp: Long = received.eventTime
                val data: ByteArray = received.data

                val raw = Mqtt.RawMessage.parseFrom(data)
                val rawPayload = raw.payload.toByteArray()

                val train: Train = JsonHelper.parse(rawPayload)

                val tripUpdate = TripUpdateBuilder(doiStopMatcher, doiTripMatcher).buildTripUpdate(train, Instant.ofEpochMilli(timestamp))
                if (tripUpdate != null) {
                    sendPulsarMessage(received.messageId, tripUpdate, timestamp)
                    //println("Built trip update: $tripUpdate")
                } else {
                    log.warn("No trip update built for train {}", train.trainNumber)
                }
            } else {
                log.warn("Received unexpected schema, ignoring.")
            }
        } catch (e: Exception) {
            log.error("Exception while handling message", e)
        } finally {
            ack(received.messageId) //Ack all messages
        }
    }

    private fun ack(received: MessageId) {
        consumer.acknowledgeAsync(received)
                .exceptionally { throwable ->
                    log.error("Failed to ack Pulsar message", throwable)
                    null
                }
                .thenRun {}
    }

    private fun sendPulsarMessage(received: MessageId, tripUpdate: GtfsRealtime.TripUpdate, timestamp: Long) {
        producer.newMessage() //.key(dvjId) //TODO think about this
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .value(tripUpdate.toByteArray())
                .sendAsync()
                .whenComplete { id: MessageId?, t: Throwable? ->
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t)
                        //Should we abort?
                    } else {
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        ack(received)
                    }
                }
    }
}


