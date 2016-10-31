package iotestbed.boat.classic

import iotestbed.boat.Utils._
import iotestbed.boat._
import iotestbed.boat.util.CSVReader._
import iotestbed.boat.util.{Criteria, Database}

object VesselDatabase_Classic extends App {

  import Database._
  import iotestbed.boat.Schema._

  val t1 = System.currentTimeMillis()

  {

    createTable(classOf[HullInfo])
    createTable(classOf[OwnerInfo])
    createTable(classOf[Position])

    // On a des sources de données qui fournissent des infos

    def entries = read("file:data.csv").toStream // does not store

    Utils.grouped(entries)(_("date")).foreach(
      entryGroup => {
        println(entryGroup.mkString("---", "\n   ", ""))

        entryGroup.foreach(processEntry)
      }
    )

    def ifDefined[K, R](o: Option[K])(value: => Option[R]): Option[R] = if (o.isDefined) value else None

    def processEntry(e: Map[String, String]) = {

      val hullCode = someInt(e("hullCode"))
      val radioCode = someString(e("radioCode"))
      val name = someString(e("name"))

      val hullInfos = {
        if (hullCode.isDefined) selectValues(HullInfo.hullCode === hullCode)
        else Seq.empty
      }
      hullInfos match {
        // Process information fusion based on hullCode, radioCode, and name.
        case Seq(dbHullInfo) => // hullCode found
          val boatKey = dbHullInfo.id

          val dbOwnerInfo = selectValues(OwnerInfo.boatKey === boatKey).sortBy(_.date).last
          val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

          update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = newKey())))

          val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)
          update(classOf[HullInfo], dbHullInfo update csvHullInfo)
          update(classOf[Position], Position.fromEntry(newKey(), boatKey, e))

        case Seq() => // no hull code
          def dbOwnerInfoFromRadioCode = ifDefined(radioCode)(selectValues(OwnerInfo.radioCode === radioCode).sortBy(_.date).lastOption)
          def dbOwnerInfoFromName = ifDefined(name)(selectValues(OwnerInfo.name === name).sortBy(_.date).lastOption)

          dbOwnerInfoFromRadioCode orElse dbOwnerInfoFromName match {
            case Some(dbOwnerInfo) =>
              val boatKey = dbOwnerInfo.boatKey

              val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)
              update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = newKey())))

              val Seq(dbHullInfo) = selectValues(HullInfo.id === boatKey)
              val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)

              update(classOf[HullInfo], dbHullInfo update csvHullInfo)
              update(classOf[Position], Position.fromEntry(newKey(), boatKey, e))

            case None =>
              // Completely new entry
              val boatKey = newKey()
              val csvHullInfo = HullInfo.fromEntry(boatKey, e)
              val csvOwnerInfo = OwnerInfo.fromEntry(newKey(), boatKey, e)

              update(classOf[HullInfo], csvHullInfo)
              update(classOf[OwnerInfo], csvOwnerInfo)
              update(classOf[Position], Position.fromEntry(newKey(), boatKey, e))
          }

        case _ =>
          throw new AssertionError("Multiple entries for hullCode")
      }
    }
  }

  def dump() = {
    val hulls = selectValues(Criteria.all[HullInfo])

    val completeHulls = hulls.map(hull => {
      val owners = selectValues(OwnerInfo.boatKey === hull.id)
      val positions = selectValues(Position.boatKey === hull.id)
      val sortedOwners = owners.sortBy(_.date)
      val sortedPositions = positions.sortBy(_.date)
      (hull, sortedOwners.toVector, sortedPositions.toVector)
    })
    completeHulls
  }

  val results = dump()

  println(Schema.dump(results).sorted.mkString("\n-----\n"))

  val t2 = System.currentTimeMillis()

  println((t2 - t1) / 1000.0)

}



