package com.realestate.service

import com.realestate.db.NotificationRepository
import com.realestate.domain.NotificationSubscription
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.{Collections, Properties}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import spray.json._

case class ListingEvent(id: String, address: String, price: Long)

object JsonProtocol extends DefaultJsonProtocol {
  implicit val listingFormat = jsonFormat3(ListingEvent)
}

class NotificationService(repo: NotificationRepository)(implicit ec: ExecutionContext) {
  import JsonProtocol._

  def startConsumer(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "notification-service")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("listings.events"))

    new Thread(() => {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(500))
        for (record <- records.asScala) {
          val event = record.value().parseJson.convertTo[ListingEvent]
          checkSubscriptions(event)
        }
      }
    }).start()
  }

  private def checkSubscriptions(listing: ListingEvent): Unit = {
    repo.allSubscriptions().foreach { subs =>
      subs.foreach { sub =>
        val matchesLocation = listing.address.contains(sub.address)
        val matchesPrice = listing.price <= sub.price

        if (matchesLocation && matchesPrice) {
          println(s"NOTIFICATION: User ${sub.userId} - New listing ${listing.id} at ${listing.address} for ${listing.price}")
        }
      }
    }
  }
}
