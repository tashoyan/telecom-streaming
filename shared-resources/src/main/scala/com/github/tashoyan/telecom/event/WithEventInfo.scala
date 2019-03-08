package com.github.tashoyan.telecom.event

trait WithEventInfo {

  val info: String

  def isCommunication: Boolean =
    info != null &&
      info.toLowerCase.contains("communication")

  def isHeat: Boolean =
    info != null &&
      info.toLowerCase.contains("heat")

  def isSmoke: Boolean =
    info != null &&
      info.toLowerCase.contains("smoke")

}
