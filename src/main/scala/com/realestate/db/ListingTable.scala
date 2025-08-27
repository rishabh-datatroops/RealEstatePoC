package com.realestate.db

import slick.jdbc.PostgresProfile.api._

import java.util.UUID
import com.realestate.domain.Listing

class ListingTable(tag: Tag) extends Table[Listing](tag, "listings") {
  def id: Rep[UUID] = column[UUID]("id", O.PrimaryKey)
  def address: Rep[String] = column[String]("address")
  def price: Rep[Long] = column[Long]("price")

  def * = (id, address, price) <> (Listing.tupled, Listing.unapply)
}
