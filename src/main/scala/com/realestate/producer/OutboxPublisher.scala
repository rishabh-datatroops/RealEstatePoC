package com.realestate.producer

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import com.realestate.db.OutboxTable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.duration._

class OutboxPublisher(db: Database, topic: String)(implicit system: ActorSystem) {
  private val producerSettings =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

  private val outbox = TableQuery[OutboxTable]

  def start(): Unit = {
    import system.dispatcher

    system.scheduler.scheduleWithFixedDelay(0.seconds, 5.seconds) { () =>
      val rowsF = db.run(outbox.result)
      rowsF.foreach { rows =>
        Source(rows.toList)
          .map { row =>
            new ProducerRecord[String, String](row.topic, row.key, row.payload)
          }
          .runWith(Producer.plainSink(producerSettings))
      }
    }
  }
}
