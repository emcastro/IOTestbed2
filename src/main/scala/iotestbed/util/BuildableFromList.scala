package iotestbed.util

trait BuildableFromList[T] {
  def fromList(l: Seq[Any]): T
}

object BuildableFromList {

  object tuples {

    private def checkArity(l: Seq[Any], n: Int): Unit = {
      if (l.size != n) throw new IllegalArgumentException(s"Arity should be $n: $l")
    }

    implicit def buildFromList_loneValue[A] = new BuildableFromList[(A)] {
      def fromList(l: Seq[Any]) = {
        checkArity(l, 1)
        val Seq(a) = l
        a.asInstanceOf[A]
      }
    }

    implicit def buildFromList_tuple2[A, B] = new BuildableFromList[(A, B)] {
      def fromList(l: Seq[Any]): (A, B) = {
        checkArity(l, 2)
        val Seq(a, b) = l
        (a.asInstanceOf[A], b.asInstanceOf[B])
      }
    }

    implicit def buildFromList_tuple3[A, B, C] = new BuildableFromList[(A, B, C)] {
      def fromList(l: Seq[Any]): (A, B, C) = {
        checkArity(l, 3)
        val Seq(a, b, c) = l
        (a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C])
      }
    }

    implicit def buildFromList_tuple4[A, B, C, D] = new BuildableFromList[(A, B, C, D)] {
      def fromList(l: Seq[Any]): (A, B, C, D) = {
        checkArity(l, 4)
        val Seq(a, b, c, d) = l
        (a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C], d.asInstanceOf[D])
      }
    }
  }


  def fromList[T: BuildableFromList](data: Seq[Any]): T = {
    val ta = implicitly[BuildableFromList[T]]
    ta.fromList(data)
  }
}
