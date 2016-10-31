package iotestbed.util

import scala.language.higherKinds
import scala.language.implicitConversions
import scalaz.{NaturalTransformation, ~>}

object EasyNatTrans {

  type ID[+T] = T
  
  type XUnit[T] = Unit

  /**
    * Build a natural transformation from an anonymous function.
    *
    * {{{
    * val x : Seq ~> Set = nt(s -> s.toSet); // s is implicitly Seq[NT]
    * }}}
    *
    * The type parameter must be the pseudo type NT.
    * No check is done that it is really a natural transformation. It should be used
    * only for trivial cases, in order to improve readability.
    */
  implicit def nt[B[_], C[_]](f: B[NT] => C[NT]): B ~> C = nt_[NT, B, C](f)

  def nt_[Z, B[_], C[_]](f: B[Z] => C[Z]): B ~> C = {
    new NaturalTransformation[B, C] {
      def apply[Q](v1: B[Q]): C[Q] = f(v1.asInstanceOf[B[Z]]).asInstanceOf[C[Q]]

      override def toString = "nt_" + f.toString()
    }
  }

  /** Pseudo type used to mark type dependant function lambda. It is erased by `nt` */
  trait NT
}