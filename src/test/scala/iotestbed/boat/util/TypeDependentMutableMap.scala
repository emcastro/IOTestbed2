package iotestbed.boat.util

import scala.collection.mutable
import scala.language.higherKinds

/**
  * @tparam K Type of the keys
  * @tparam V Type of the values
  * @tparam B Base type for the type parameters of the values. If you don't know, use `Any`
  */
class TypeDependentMutableMap[K[_], V[_ <: B], B]() {

  val backend = mutable.Map.empty[Any, Any]

  def getOrElseUpdate[T <: B](key: K[T], op: => V[T]): V[T] = backend.getOrElseUpdate(key, op).asInstanceOf[V[T]]
}

object TypeDependentMutableMap {
  /** Identity function at Type level.
    *
    * Useful is the following example, when K or V are not parametrised types:
    * {{{
    * val x = new TypeDependantMutableMap[ID, List, Any]()
    * x.getOrElseUpdate(arg, List(arg))
    * }}}
    * */
  type ID[T] = T
}
