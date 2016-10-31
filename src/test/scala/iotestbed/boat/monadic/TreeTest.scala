package iotestbed.boat.monadic

import iotestbed.Vectorizable._
import iotestbed.boat.Utils._
import iotestbed.boat.util
import iotestbed.boat.util.Database.Row
import org.scalatest.FunSpec

class TreeTest extends FunSpec {

  describe("Given a database containing a tree of nodes") {

    util.Database.createTable(classOf[Node])

    val root = Node(1, 0)

    util.Database.update(classOf[Node],
      root,

      Node(11, 1),
      Node(21, 1),
      Node(31, 1),

      Node(111, 11),
      Node(211, 11),
      Node(311, 11),

      Node(121, 21),
      Node(221, 21),
      Node(321, 21),

      Node(131, 31),
      Node(231, 31),
      Node(331, 31),

      Node(1231, 231)
    )

    describe("A recursive function extracting all the leaves of the tree") {
      def leaves(n: Node): Script[Seq[Node]] = {
        import iotestbed.boat.monadic.Database._

        // It looks like a depth first search. However it behaves as de breadth first search.
        selectValues(Node.parent === n.id) >>= {
          subNodes =>
            if (subNodes.isEmpty) done(Seq(n))
            else unordered(subNodes.sortBy(_.id).map(leaves)) >>= {
              x => done(x.flatten)
            }
        }
      }

      it("returns correct leaves") {
        val leaveSeq = go(leaves(root))
        val a: Seq[Int] = leaveSeq.map(_.id)
        val b: Seq[Int] = Seq(111, 211, 311, 121, 221, 321, 131, 1231, 331)
        assert(a === b)
      }

      it("gets the result using a number of queries equal to the depth of the tree") {
        // TODO à coder
      }
    }

    describe("A recursive function extracting all the leaves of the tree (each operator version)") {
      import com.thoughtworks.each.Monadic._

      def leaves(n: Node): Script[Seq[Node]] = {
        import iotestbed.boat.monadic.Database._

        monadic[Script] {
          val subNodes = selectValues(Node.parent === n.id).each
          if (subNodes.isEmpty) Seq(n)
          else {
            val x = unordered(subNodes.sortBy(_.id).map(leaves)).each
            x.flatten
          }
        }
      }

      it("returns correct leaves") {
        val leaveSeq = go(leaves(root))
        val a: Seq[Int] = leaveSeq.map(_.id)
        val b: Seq[Int] = Seq(111, 211, 311, 121, 221, 321, 131, 1231, 331)
        assert(a === b)
      }

    }

  }
}


/** Database class for table Node */
case class Node(id: Int, parent: Int) extends Row

object Node {
  val parent = namedAccessor((o: Node) => o.parent, "parent")

  implicit val nodeClass: Class[Node] = classOf[Node]
}