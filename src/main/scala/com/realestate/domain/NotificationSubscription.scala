package com.realestate.domain

import java.util.UUID

case class NotificationSubscription(
                                     id: UUID,
                                     userId: String,
                                     address: String,
                                     price: Long )
