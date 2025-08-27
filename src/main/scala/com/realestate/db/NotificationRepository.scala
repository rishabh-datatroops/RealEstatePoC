package com.realestate.db

import com.realestate.domain.NotificationSubscription
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

class NotificationRepository(implicit ec: ExecutionContext) {
  private val subscriptions = TrieMap.empty[UUID, NotificationSubscription]

  def addSubscription(sub: NotificationSubscription): Future[Unit] = Future {
    subscriptions.put(sub.id, sub)
    ()
  }

  def allSubscriptions(): Future[Seq[NotificationSubscription]] =
    Future.successful(subscriptions.values.toSeq)
}
