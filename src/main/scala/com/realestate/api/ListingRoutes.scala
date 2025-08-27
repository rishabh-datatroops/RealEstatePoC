package com.realestate.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import com.realestate.db.ListingRepository
import com.realestate.domain.Listing
import java.util.UUID
import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.RequestContext

class ListingRoutes(repo: ListingRepository)(implicit ec: ExecutionContext) {

  val routes: Route =
    pathPrefix("listings") {
      concat(
        pathEndOrSingleSlash {
          concat(
            post {
              entity(as[String]) { addr =>
                val id = UUID.randomUUID()
                val listing = Listing(id, address = addr, price = 100000)
                onSuccess(repo.createListing(listing)) { _ =>
                  complete(StatusCodes.Created -> s"Listing created: $id")
                }
              }
            },
            get {
              onSuccess(repo.allListings()) { all =>
                complete(all.toString)
              }
            }
          )
        },
        path(JavaUUID / "updatePrice" / LongNumber) { (id, newPrice) =>
          put { ctx: RequestContext =>
            repo.updatePrice(id, newPrice).flatMap { _ =>
              complete(StatusCodes.OK -> s"Price updated for $id")(ctx)
            }
          }
        }
      )
    }
}
