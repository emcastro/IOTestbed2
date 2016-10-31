package iotestbed.boat.monadic

import iotestbed.Vectorizable
import iotestbed.boat.util.CSVReader
import CSVReader._
import iotestbed.boat.Utils._
import iotestbed.boat._

import iotestbed.Vectorizable._

class VesselDatabase_Each {

  import iotestbed.boat.Schema._

  val db = new VecDatabase()

  def run(url: String): Unit = {

    db.createTable(classOf[HullInfo])
    db.createTable(classOf[OwnerInfo])
    db.createTable(classOf[Position])

    def entries = read(url).toStream // does not store

    Utils.grouped(entries)(_ ("date")).foreach {
      entryGroup => {
        println(entryGroup.mkString("---", "\n   ", ""))

        go {
          unordered(entryGroup.toVector.map(processEntry))
        }
      }
    }

    def ifDefined[K, R](o: Option[K])(value: => Script[Option[R]]): Script[Option[R]] = {
      if (o.isDefined) value else done(None)
    }

    def orElse[T](mainClause: Script[Option[T]])(alternativeClause: Script[Option[T]]): Script[Option[T]] = {
      mainClause >>= {
        case s: Some[T] => done(s)
        case None => alternativeClause
      }
    }

    def processEntry(e: Map[String, String]): Script[Unit] = {

      import com.thoughtworks.each.Monadic._

      monadic[Script] {

        val hullCode = someInt(e("hullCode"))
        val radioCode = someString(e("radioCode"))
        val name = someString(e("name"))

        val hullInfos = ({
          if (hullCode.isDefined) db.selectValues(HullInfo.hullCode === hullCode)
          else done(Seq.empty[HullInfo])
        }).each

        hullInfos match {
          // Process information fusion based on hullCode, radioCode, and name.
          case Seq(dbHullInfo) => // hullCode found
            val boatKey = dbHullInfo.id

            val dbOwnerInfo = db.selectValues(OwnerInfo.boatKey === boatKey).each.sortBy(_.date).last
            val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

            val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)
            unordered(
              db.update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = db.newKey()))),
              db.update(classOf[HullInfo], dbHullInfo update csvHullInfo),
              db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))
            ).each
            ()

          case Seq() => // no hull code
            def dbOwnerInfoFromRadioCode = ifDefined(radioCode)(db.selectValues(OwnerInfo.radioCode === radioCode).map(_.sortBy(_.date).lastOption))
            def dbOwnerInfoFromName = ifDefined(name)(db.selectValues(OwnerInfo.name === name).map(_.sortBy(_.date).lastOption))

            orElse(dbOwnerInfoFromRadioCode)(dbOwnerInfoFromName).each match {
              case Some(dbOwnerInfo) =>
                val boatKey = dbOwnerInfo.boatKey

                val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

                val dbHullInfos = db.selectValues(HullInfo.id === boatKey).each
                val Seq(dbHullInfo) = dbHullInfos
                val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)

                unordered(
                  db.update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = db.newKey()))),
                  db.update(classOf[HullInfo], dbHullInfo update csvHullInfo),
                  db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))
                ).each

              case None =>
                // Completely new entry
                val boatKey = db.newKey()
                val csvHullInfo = HullInfo.fromEntry(boatKey, e)
                val csvOwnerInfo = OwnerInfo.fromEntry(db.newKey(), boatKey, e)

                unordered(
                  db.update(classOf[HullInfo], csvHullInfo),
                  db.update(classOf[OwnerInfo], csvOwnerInfo),
                  db.update(classOf[Position], Position.fromEntry(db.newKey(), boatKey, e))
                ).each
            }

          case _ =>
            throw new AssertionError("Multiple entries for hullCode")

        }
      }
    }
  }
}

object VesselDatabase_Each_App extends App {

  val vd = new VesselDatabase_Each()
  import vd._

  val t1 = System.currentTimeMillis()

  run("file:data.csv")

  val results = Vectorizable.go(db.dump)

  println(Schema.formatDump(results).sorted.mkString("\n-----\n"))

  val t2 = System.currentTimeMillis()

  db.dumpStats()

  println((t2 - t1) / 1000.0)

}


