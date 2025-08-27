package com.realestate.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import com.realestate.db.NotificationRepository
import com.realestate.domain.NotificationSubscription
import java.util.UUID
import scala.concurrent.ExecutionContext
import spray.json._

case class SubscriptionRequest(userId: String, address: String, price: Long)

object SubscriptionJsonProtocol extends DefaultJsonProtocol {
  implicit val reqFormat = jsonFormat3(SubscriptionRequest)
}

class NotificationRoutes(repo: NotificationRepository)(implicit ec: ExecutionContext) {
  import SubscriptionJsonProtocol._

  val routes: Route =
    pathPrefix("subscriptions") {
      concat(
        pathEndOrSingleSlash {
          post {
            entity(as[String]) { body =>
              val req = body.parseJson.convertTo[SubscriptionRequest]
              val sub = NotificationSubscription(
                id = UUID.randomUUID(),
                userId = req.userId,
                address = req.address,
                price = req.price
              )
              onSuccess(repo.addSubscription(sub)) {
                complete(StatusCodes.Created -> s"Subscription created: ${sub.id}")
              }
            }
          } ~
          get {
            onSuccess(repo.allSubscriptions()) { all =>
              complete(all.toString)
            }
          }
        }
      )
    }
}
