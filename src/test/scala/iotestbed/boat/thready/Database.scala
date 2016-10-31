package iotestbed.boat.thready

import iotestbed.Vectorizable._
import iotestbed.boat.util
import iotestbed.boat.util.Database.Row
import iotestbed.boat.util.{Criteria, EqualityCriteria}

import collection.mutable

object Database {

  implicit class ImplicitCriteria[A <: Row, B](f: A => B) {
    def ===(b: B) = EqualityCriteria[A, B](f, b)
  }

  def newKey() = util.Database.newKey()

  def createTable(table: Class[_ <: Row]) = util.Database.createTable(table) // no need to be a diehard on such database action

  // ------------------------------------------
  def threadVectorizable[X, Y](f: Seq[X] => Seq[Y], name: String = "<function>"): X => Y = ???

  // ------------------------------------------

  def selectValues[R <: Row](criteria: Criteria[R])(implicit table: Class[R]): Seq[R] = ???

  def update[R <: Row](table: Class[R], entries: R*) : Unit = ???

  def fork[A, B](a: A, b: B): (A, B) = ???

  def fork[A, B, C](a: A, b: B, c: C): (A, B, C) = ???

  def fork[A, B, C, D](a: A, b: B, c: C, d: D): (A, B, C, D) = ???

  def fork[A](scripts: Seq[A]): Seq[A] = ???

}
