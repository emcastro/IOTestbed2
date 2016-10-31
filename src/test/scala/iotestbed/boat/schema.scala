package iotestbed.boat

import iotestbed.boat.util.Database.{Key, Row}
import iotestbed.boat.Utils._

case class HullInfo(id: Int, hullCode: Option[Int], length: Option[Int], width: Option[Int]) extends Row {

  def update(that: HullInfo) = {
    assert(id == that.id)

    val hullCode = updateVal(this.hullCode, that.hullCode)
    val length = updateVal(this.length, that.length)
    val width = updateVal(this.width, that.width)

    HullInfo(id, hullCode, length, width)
  }

}

object HullInfo {
  val id = namedAccessor((h: HullInfo) => h.id, "id")
  val hullCode = namedAccessor((h: HullInfo) => h.hullCode, "hullCode")

  def fromEntry(id: Int, e: Map[String, String]) =
    HullInfo(id, someInt(e("hullCode")), someInt(e("length")), someInt(e("width")))
}

case class OwnerInfo(id: Int, boatKey: Key, date: String, radioCode: Option[String], name: Option[String], owner: Option[String]) extends Row {

  /** Mix two OwnerInfo if they're values are compatible */
  def mix(that: OwnerInfo): Option[OwnerInfo] = {
    assert(id == that.id)
    assert(boatKey == that.boatKey)
    assert(date <= that.date)
    for {
      radioCode <- mixVal(this.radioCode, that.radioCode)
      name <- mixVal(this.name, that.name)
      owner <- mixVal(this.owner, that.owner)
    } yield OwnerInfo(id, boatKey, that.date, radioCode, name, owner)
  }

}

object OwnerInfo {
  val boatKey = namedAccessor((o: OwnerInfo) => o.boatKey, "boatKey")
  val radioCode = namedAccessor((o: OwnerInfo) => o.radioCode, "radioCode")
  val name = namedAccessor((o: OwnerInfo) => o.name, "name")

  def fromEntry(id: Int, boatKey: Int, e: Map[String, String]) =
    OwnerInfo(id, boatKey, Some(e("date")).get, someString(e("radioCode")), someString(e("name")), someString(e("owner")))
}

case class Position(id: Int, boatKey: Key, date: String, position: String) extends Row

object Position {
  def fromEntry(id: Int, boatKey: Int, e: Map[String, String]) =
    Position(id, boatKey, Some(e("date")).get, someString(e("position")).get)

  val boatKey = namedAccessor((o: Position) => o.boatKey, "boatKey")
}

object Schema {

  implicit val classOfHullInfo = classOf[HullInfo]
  implicit val classOfOwnerInfo = classOf[OwnerInfo]
  implicit val classOfPosition = classOf[Position]

  def dump(results: Seq[(HullInfo, Vector[OwnerInfo], Vector[Position])]) = {
    for ((hull, owners, positions) <- results) yield {
      s"""hull-code: ${hull.hullCode.getOrElse("?")} - ${hull.length.getOrElse("?")}×${hull.width.getOrElse("?")}
         |
         |  owners:
         |    ${owners.map(o => s"${o.date} ${o.owner.getOrElse("?")} - name: ${o.name.getOrElse("?")} - radio: ${o.radioCode.getOrElse("?")}").mkString("\n    ")}
         |  positions:
         |    ${positions.map(p => s"${p.date} ${p.position}").mkString("\n    ")}
       """.stripMargin
    }
  }
}
