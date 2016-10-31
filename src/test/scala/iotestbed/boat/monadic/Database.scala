package iotestbed.boat.monadic

import iotestbed.CallList.CallList
import iotestbed.Vectorizable._
import iotestbed._
import iotestbed.boat.util.Database.Row
import iotestbed.boat.util.{Criteria, EqualityCriteria, TypeDependentMutableMap}
import iotestbed.boat.{HullInfo, OwnerInfo, Position, util}
import iotestbed.util.EasyNatTrans._

import scala.language.{existentials, higherKinds}

object Database {

  implicit class ImplicitCriteria[A <: Row, B](f: A => B) {
    def ===(b: B) = EqualityCriteria[A, B](f, b)
  }

  def newKey() = util.Database.newKey()

  def createTable(table: Class[_ <: Row]) = util.Database.createTable(table) // no need to be a diehard on such database action

  // selectValues

  trait NRow <: Row with NT

  private val vecSelectValue = new SetMapVectorizableFunctionFamily[Class[NRow], Criteria[NRow], Seq[NRow]](
    table => criteriaSet => {
      val extracted: Class[NRow] = table
      util.Database.selectValues(criteriaSet)(extracted)
    })

  def selectValues[R <: Row](criteria: Criteria[R])(implicit table: Class[R]): Script[Seq[R]] = {
    vecSelectValue(table, criteria)
  }

  // update

  private val vecUpdate = new SeqSeqVectorizableFunctionFamily[Class[NRow], ID[NRow], XUnit[NRow]](
    table => rows => {
      util.Database.update[NRow](table, rows: _*)
      Seq.fill(rows.size)(())
    })

  def update[R <: Row](table: Class[R], row: R): Script[Unit] = {
    vecUpdate(table, row: ID[R]): Script[XUnit[R]]
  }

  // dump

  def dump() = {
    import iotestbed.boat.Schema._

    for {
      hulls <- selectValues(Criteria.all[HullInfo])

      completeHulls <- unordered(hulls.map(hull =>
        for {
          op <- unordered(
            selectValues(OwnerInfo.boatKey === hull.id),
            selectValues(Position.boatKey === hull.id))
          (owners, positions) = op
          sortedOwners = owners.sortBy(_.date)
          sortedPositions = positions.sortBy(_.date)
        } yield (hull, sortedOwners.toVector, sortedPositions.toVector)))
    } yield completeHulls
  }

  // Policy: wait at least 10 similar queries before early evaluation.
  implicit object policy extends EvaluationPolicy {
    override def apply[V](callList: CallList[V]): CallList[V] = {
      val selectedKinds = callList.stats.filter { case (kind, count) => count >= 10 }.keys

      CallList(selectedKinds.flatMap(callList.groups).toList)
    }
  }

}
