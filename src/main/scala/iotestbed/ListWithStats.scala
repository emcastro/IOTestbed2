package iotestbed

/**
 * A list with hold values V with a _type_ T. The list gives stats about the number of values of each type.
 */
case class ListWithStats[T, V](typer: V => T, list: List[V]) {
  private var _stats: Map[T, Int] = _

  private def _statsInited = _stats != null

  def size = list.size

  def isEmpty = list.isEmpty

  def nonEmpty = list.nonEmpty

  def stats: Map[T, Int] = {
    // We would have used a lazy, if it was possible to know if a lazy is evaluated or not (in method `+`)
    if (!_statsInited) {
      // countBy
      _stats = list.foldLeft(Map.empty[T, Int].withDefaultValue(0))(
        (currentStats, v) => {
          val typ = typer(v)
          currentStats.updated(typ, currentStats(typ) + 1)
        }
      )
    }

    _stats
  }

  def +(v: V): ListWithStats[T, V] = {
    // Compute the stats only when stats has been called once
    val res = ListWithStats(typer, v :: list)
    if (_statsInited) {
      val typ = typer(v)
      res._stats = stats.updated(typ, stats(typ) + 1)
    }
    res
  }

  def -(that: ListWithStats[T, V]): ListWithStats[T, V] = {
    if (this eq that) ListWithStats(typer, Nil) // fast track
    else if (that.list eq Nil) this
    else {
      ListWithStats(typer, list.filterNot(that.list.contains))
    }
  }

  lazy val groups = list.groupBy(typer).withDefaultValue(Nil)
}

