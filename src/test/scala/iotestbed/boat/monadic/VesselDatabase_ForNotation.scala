package iotestbed.boat.monadic

import iotestbed.Vectorizable
import iotestbed.boat.util.{Criteria, CSVReader}
import CSVReader._
import iotestbed.boat.Utils._
import iotestbed.boat._

import iotestbed.Vectorizable._

object VesselDatabase_ForNotation extends App {

  import iotestbed.boat.Schema._
  import iotestbed.boat.monadic.Database._

  createTable(classOf[HullInfo])
  createTable(classOf[OwnerInfo])
  createTable(classOf[Position])


  def entries = read("file:data.csv").toStream // does not store

  val t1 = System.currentTimeMillis()

  Utils.grouped(entries)(_("date")).foreach {
    entryGroup => {
      println(entryGroup.mkString("---", "\n   ", ""))

      go {
        unordered(entryGroup.toVector.map(processEntry))
      }
    }
  }

  def ifDefined[K, R](o: Option[K])(value: => Script[Option[R]])
  : Script[Option[R]] = {
    if (o.isDefined) value else done(None)
  }

  def orElse[T](mainClause: Script[Option[T]])(alternativeClause: Script[Option[T]]): Script[Option[T]] = {
    mainClause >>= {
      case s: Some[T] => done(s)
      case None => alternativeClause
    }
  }

  def processEntry(e: Map[String, String]): Script[Unit] = {

    val hullCode = someInt(e("hullCode"))
    val radioCode = someString(e("radioCode"))
    val name = someString(e("name"))

    for {
      hullInfos <- if (hullCode.isDefined) selectValues(HullInfo.hullCode === hullCode) else done(Seq.empty[HullInfo])

      _ <- hullInfos match {
        // Process information fusion based on hullCode, radioCode, and name.
        case Seq(dbHullInfo) => // hullCode found
          val boatKey = dbHullInfo.id
          val csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)

          for {
            dbOwnerInfo <- selectValues(OwnerInfo.boatKey === boatKey).map(e => e.sortBy(_.date).last)

            csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

            _ <- unordered(
              update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = newKey()))),
              update(classOf[HullInfo], dbHullInfo update csvHullInfo),
              update(classOf[Position], Position.fromEntry(newKey(), boatKey, e)))
          } yield ()


        case Seq() => // no hull code
          def dbOwnerInfoFromRadioCode = ifDefined(radioCode)(selectValues(OwnerInfo.radioCode === radioCode).map(_.sortBy(_.date).lastOption))
          def dbOwnerInfoFromName = ifDefined(name)(selectValues(OwnerInfo.name === name).map(_.sortBy(_.date).lastOption))

          orElse(dbOwnerInfoFromRadioCode)(dbOwnerInfoFromName) >>= {
            dbOwnerInfoOption =>

              dbOwnerInfoOption match {
                case Some(dbOwnerInfo) =>
                  val boatKey = dbOwnerInfo.boatKey

                  val csvOwnerInfo = OwnerInfo.fromEntry(dbOwnerInfo.id, boatKey, e)

                  for {
                    dbHullInfos <- selectValues(HullInfo.id === boatKey)

                    Seq(dbHullInfo) = dbHullInfos
                    csvHullInfo = HullInfo.fromEntry(dbHullInfo.id, e)

                    _ <- unordered(
                      update(classOf[OwnerInfo], (dbOwnerInfo mix csvOwnerInfo).getOrElse(csvOwnerInfo.copy(id = newKey()))),
                      update(classOf[HullInfo], dbHullInfo update csvHullInfo),
                      update(classOf[Position], Position.fromEntry(newKey(), boatKey, e)))
                  } yield ()


                case None =>
                  // Completely new entry
                  val boatKey = newKey()
                  val csvHullInfo = HullInfo.fromEntry(boatKey, e)
                  val csvOwnerInfo = OwnerInfo.fromEntry(newKey(), boatKey, e)

                  for {
                    _ <- unordered(
                      update(classOf[HullInfo], csvHullInfo),
                      update(classOf[OwnerInfo], csvOwnerInfo),
                      update(classOf[Position], Position.fromEntry(newKey(), boatKey, e)))
                  } yield ()
              }
          }

        case _ =>
          throw new AssertionError("Multiple entries for hullCode")

      }
    }

      yield ()
  }

  val results = Vectorizable.go(Database.dump())

  println(Schema.dump(results).sorted.mkString("\n-----\n"))

  util.Database.dumpStats()
  val t2 = System.currentTimeMillis()

  println((t2 - t1) / 1000.0)

}


