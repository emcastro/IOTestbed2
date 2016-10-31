package iotestbed

import iotestbed.Vectorizable._
import org.scalatest.FunSpec

import scala.util.Random

class LoopTest extends FunSpec {

  val VERY_LONG_LENGTH = 1000000

  describe("Standard loop style") {
    def classicLoop(i: Int): List[Int] = {
      if (i == 0)
        List.empty
      else
        i :: classicLoop(i - 1)
    }

    it("is not stackless") {
      intercept[StackOverflowError] {
        classicLoop(VERY_LONG_LENGTH)
      }
    }
  }

  describe("Vectorizable loop style") {
    def loop(i: Int): Vectorizable.Script[List[Int]] = {
      if (i == 0)
        done(List.empty)
      else
        done(i - 1) >>= loop _ >>= (x => done(i :: x))
    }

    it("is stackless (using trampoline)") {

      val result = go(loop(VERY_LONG_LENGTH))
      assert(result.size === VERY_LONG_LENGTH)
      assert(result.startsWith(Seq(VERY_LONG_LENGTH, VERY_LONG_LENGTH - 1)))
      assert(result.endsWith(Seq(5, 4, 3, 2, 1)))
    }

  }

  describe("Vectorisable and FoldR") {

    import scalaz.Scalaz._

    val r = new Random(421)

    def newSequence(): Vector[Int] = Iterator.continually(r.nextInt(10)).take(1000).toVector

    val s1 = newSequence()
    val s2 = newSequence()
    val s3 = newSequence()

    var actionCount = 0
    def vec_plusMod10(args: Set[(Int, Int)]) = {
      // let's suppose it is slow and complicated
      actionCount += args.size
      args.map {
        case key@(x, y) => key -> (x + y) % 10
      }.toMap
    }

    val plusMod10: (Int, Int) => Script[Int] = Function.untupled(new SetMapVectorizableFunction(vec_plusMod10, "+%10"))

    val res: Script[(Int, Int, Int)] = unordered(
      s1.foldLeftM(0)(plusMod10),
      s2.foldLeftM(0)(plusMod10),
      s3.foldLeftM(0)(plusMod10)
    )

    it("produces the same as classic FoldLeft") {
      val classisRes = (
        s1.foldLeft(0)((a, b) => (a + b) % 10),
        s2.foldLeft(0)((a, b) => (a + b) % 10),
        s3.foldLeft(0)((a, b) => (a + b) % 10)
        )
      assert(go(res) == classisRes)
    }
  }

}
