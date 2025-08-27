package com.realestate.db

import com.realestate.domain.Listing
import com.realestate.events.EventJsonProtocol._
import com.realestate.events.{ListingCreated, ListingPriceChanged}
import slick.jdbc.PostgresProfile.api._
import spray.json._

import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ListingRepository(db: Database) {
  private val listings = TableQuery[ListingTable]
  private val outbox = TableQuery[OutboxTable]

  def createListing(listing: Listing): Future[Int] = {
    val event = ListingCreated(listing.id, listing.address, listing.price)
    val row = OutboxRow(
      UUID.randomUUID(),
      "listings.events",
      listing.id.toString,
      event.toJson.compactPrint,
      Instant.now
    )
    val action = (listings += listing) andThen (outbox += row)
    db.run(action.transactionally)
  }

  def updatePrice(id: UUID, newPrice: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    val q = listings.filter(_.id === id).result.head
    val action = q.flatMap { listing =>
      val event = ListingPriceChanged(listing.id, listing.price, newPrice)
      val row = OutboxRow(
        UUID.randomUUID(),
        "listings.events",
        listing.id.toString,
        event.toJson.compactPrint,
        Instant.now
      )
      for {
        _ <- listings.filter(_.id === id).map(_.price).update(newPrice)
        _ <- outbox += row
      } yield ()
    }
    db.run(action.transactionally)
  }

  def allListings(): Future[Seq[Listing]] = db.run(listings.result)
}