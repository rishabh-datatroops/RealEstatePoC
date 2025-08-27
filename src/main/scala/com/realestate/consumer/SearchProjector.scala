package com.realestate.consumer

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.concurrent.TrieMap

class SearchProjector(topic: String)(implicit system: ActorSystem) {
  val index: TrieMap[String, String] = TrieMap[String, String]() // id -> address

  def start(): Unit = {
    val settings =
      ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
        .withGroupId("search-projector")

    Consumer
      .plainSource(settings, Subscriptions.topics(topic))
      .map { record =>
        println(s"[SearchProjector] Event: ${record.value()}")
        // simplistic: store raw payload as "indexed"
        index.put(record.key(), record.value())
      }
      .runForeach(_ => ())
  }

  def search(q: String): Seq[String] = {
    index.values.filter(_.toLowerCase.contains(q.toLowerCase)).toSeq
  }
}
