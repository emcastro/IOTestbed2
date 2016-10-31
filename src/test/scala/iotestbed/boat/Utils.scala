package iotestbed.boat

object Utils {

  def grouped[A, B](i: Stream[A])(extractor: A => B): Stream[Stream[A]] = {
    i match {
      case first #:: tail =>
        val key = extractor(first)
        val (group, rest) = tail.span(e => extractor(e) == key)
        (first +: group) #:: grouped(rest)(extractor)

      case Stream.Empty => Stream.Empty
    }
  }

  def someInt(s: String) = {
    s match {
      case "" => None
      case v => Some(v.toInt)
    }
  }

  def someString(s: String) = {
    s match {
      case "" => None
      case v => Some(s)
    }
  }

  /** Mix two options if they are compatible */
  def mixVal[X](a: Option[X], b: Option[X]): Option[Option[X]] = {
    (a, b) match {
      case (None, aa) => Some(aa)
      case (aa, None) => Some(aa)
      case (aa, bb) if aa == bb => Some(a)
      case _ => None
    }
  }

  def updateVal[Y, X >: Option[Y]](a: X, b: X): X = {
    (a, b) match {
      case (None, _) => b
      case _ => a
    }
  }

  def updateVal(a: Int, b: Int): Int = {
    (a, b) match {
      case (0, _) => b
      case _ => a
    }
  }

  def namedAccessor[A, B](f: A => B, name: String): A => B = {
    new Function[A, B] {
      def apply(a: A): B = f(a)

      override def toString() = name
    }
  }

}
