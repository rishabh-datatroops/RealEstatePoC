package com.realestate.events

import java.util.UUID
import java.time.Instant

sealed trait ListingEvent

case class ListingCreated(id: UUID, address: String, price: Long, createdAt: Instant = Instant.now) extends ListingEvent

case class ListingPriceChanged(id: UUID, oldPrice: Long, newPrice: Long, changedAt: Instant = Instant.now) extends ListingEvent
