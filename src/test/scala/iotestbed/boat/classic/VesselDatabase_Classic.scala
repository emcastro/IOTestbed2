package iotestbed.boat.classic

import iotestbed.boat.Utils._
import iotestbed.boat._
import iotestbed.boat.util.CSVReader._
import iotestbed.boat.util.{Criteria, Database}

class VesselDatabase_Classic {

  import iotestbed.boat.Schema._

  val db = new Database()

  def run(url: String): Unit = {

    db.createTable(classOf[HullInfo])
    db.createTable(classOf[OwnerInfo])
    db.createTable(classOf[Position])

    def entries = read(url).toStream // does not store

    Utils.grouped(entries)(_ ("date")).foreach(
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
        if (hullCode.isDefined) db.selectValues(HullInfo.hullCode === hullCode)
        else Seq.empty
      }
      hullInfos match {
        // Process information fusion based on hullCode, radioCode, and name.
        case Seq(dbHullInfo) => // hullCode found
          val boatKey = dbHullInfo.id

          val dbOwnerInfo = db.selectValues(OwnerInfo.boatKey === boatKey).sortBy(_.date).last
          val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

          db.update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = db.newKey())))

          val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)
          db.update(classOf[HullInfo], dbHullInfo update csvHullInfo)
          db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))

        case Seq() => // no hull code
          def dbOwnerInfoFromRadioCode = ifDefined(radioCode)(db.selectValues(OwnerInfo.radioCode === radioCode).sortBy(_.date).lastOption)
          def dbOwnerInfoFromName = ifDefined(name)(db.selectValues(OwnerInfo.name === name).sortBy(_.date).lastOption)

          dbOwnerInfoFromRadioCode orElse dbOwnerInfoFromName match {
            case Some(dbOwnerInfo) =>
              val boatKey = dbOwnerInfo.boatKey

              val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)
              db.update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = db.newKey())))

              val Seq(dbHullInfo) = db.selectValues(HullInfo.id === boatKey)
              val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)

              db.update(classOf[HullInfo], dbHullInfo update csvHullInfo)
              db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))

            case None =>
              // Completely new entry
              val boatKey = db.newKey()
              val csvHullInfo = HullInfo.fromEntry(boatKey, e)
              val csvOwnerInfo = OwnerInfo.fromEntry(db.newKey(), boatKey, e)

              db.update(classOf[HullInfo], csvHullInfo)
              db.update(classOf[OwnerInfo], csvOwnerInfo)
              db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))
          }

        case _ =>
          throw new AssertionError("Multiple entries for hullCode")
      }
    }
  }

  def dump = {
    val hulls = db.selectValues(Criteria.all[HullInfo])

    val completeHulls = hulls.map(hull => {
      val owners = db.selectValues(OwnerInfo.boatKey === hull.id)
      val positions = db.selectValues(Position.boatKey === hull.id)
      val sortedOwners = owners.sortBy(_.date)
      val sortedPositions = positions.sortBy(_.date)
      (hull, sortedOwners.toVector, sortedPositions.toVector)
    })
    completeHulls
  }

}

object VesselDatabase_Classic_App extends App {

  val vd = new VesselDatabase_Classic()
  import vd._

  val t1 = System.currentTimeMillis()

  run("file:data.csv")

  val results = dump

  println(Schema.formatDump(results).sorted.mkString("\n-----\n"))

  val t2 = System.currentTimeMillis()

  db.dumpStats()

  println((t2 - t1) / 1000.0)

}