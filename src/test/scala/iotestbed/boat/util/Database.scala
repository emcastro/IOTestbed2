package iotestbed.boat.util

import iotestbed.boat.util.Database.Row

/**
 * Let's pretend that we have a database, with high I/O cost.
 * It stores a tree of Nodes.
 */
object Database {

  trait Row {
    def id: Int
  }

  type Key = Int

  implicit class ImplicitCriteria[A <: Row, B](f: A => B) {
    def ===(b: B) = EqualityCriteria[A, B](f, b)
  }

  type Table[R <: Row] = Map[Key, R]

  var tables = Map.empty[Class[_ <: Row], Table[Row]]

  var ioCount = 0
  var selectCount = 0

  var keySequence = 0

  def newKey() = {
    // Free, it either is included in insertion statement, or can be generated as an UUID
    keySequence += 1
    keySequence
  }

  private val LATENCY = 10
  private val PROCESSING_TYPE = 1

  def pause(duration: Long): Unit = {
    Thread.sleep(duration)
  }

  def createTable(table: Class[_ <: Row]): Unit = synchronized {
    ioCount += 1
    pause(LATENCY)
    tables += table -> Map.empty
  }


  def update[R <: Row](table: Class[R], entries: R*) = synchronized {
    println("update: " + table.getSimpleName + " × " + entries.size)
    ioCount += 1
    pause(LATENCY + PROCESSING_TYPE * entries.size)
    val tableMap = tables(table)
    val updatedMap = tableMap ++ entries.map(e => e.id -> e)
    tables += table -> updatedMap
  }

  def selectValues[R <: Row](criteria: Criteria[R])(implicit table: Class[R]): Seq[R] = synchronized {
    ioCount += 1
    pause(LATENCY + 1)
    selectCount += 1
    val tableSeq = tables(table).values.toSeq
    println(s"SelectValue(${table.getSimpleName}, ${criteria})")
    tableSeq.filter { case (row: R@unchecked) => criteria.check(row) }.asInstanceOf[Seq[R]]
  }

  def selectValues[R <: Row](criteriaList: Seq[Criteria[R]])(implicit table: Class[R]): Seq[Seq[R]] = synchronized {
    ioCount += 1
    pause(LATENCY + PROCESSING_TYPE * criteriaList.size)
    selectCount += 1
    val tableSeq = tables(table).values.toSeq
    println(s"SelectValue ${criteriaList.size}(${table.getSimpleName}, ${criteriaList.mkString(" / ")})")
    criteriaList.map(criteria => tableSeq.filter { case (row: R@unchecked) => criteria.check(row) }.asInstanceOf[Seq[R]])
  }

  def selectValues[R <: Row](criteriaSet: Set[Criteria[R]])(implicit table: Class[R]): Map[Criteria[R], Seq[R]] = {
    val cirteriaList = criteriaSet.toList.sortBy(_.toString)
    (cirteriaList zip selectValues(cirteriaList)).toMap
  }


    def dump() = synchronized {
    println("=======================================")
    Database.tables.values.foreach(
      table => {
        println("====")
        table.values.toSeq.sortBy(_.id).foreach(
          row =>
            println(row)
        )
      }
    )

    dumpStats()
  }

  def dumpStats(): Unit = {
    println("io count: " + Database.ioCount)
    println("select count: " + Database.selectCount)
  }
}

trait Criteria[R] {
  def check(value: R): Boolean
}

object Criteria {
  def all[R <: Row]: Criteria[R] = new Criteria[R] {
    override def check(value: R): Boolean = true

    override def toString = "ALL"
  }
}

case class EqualityCriteria[R <: Row, V](extractor: R => V, value: V) extends Criteria[R] {
  def check(row: R) = extractor(row) == value

  override def toString = s"${extractor} = $value"
}