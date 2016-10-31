package iotestbed

import iotestbed.Vectorizable._
import iotestbed.util.EasyNatTrans._
import org.scalatest.FunSpec
import uk.org.lidalia.slf4jtest.TestLoggerFactory
import iotestbed.util.BuildableFromList.tuples._

import scala.language.existentials

class WorkflowTest extends FunSpec {

  val logger = TestLoggerFactory.getTestLogger(Vectorizable.getClass)

  case class VectorizedCallInfo(op: VectorizableFunction[_, _], arity: Integer)

  def logForOps(ops: VectorizableFunction[_, _]*) =
    TestLogSupport
      .forAllEvents(Vectorizable.getClass)
      .typedLogs(Vectorizable.VectorizedCall)
      .map(VectorizedCallInfo.tupled)
      .filter(ops contains _.op)


  case class TestContext
  (
    OP_A: VectorizableFunction[String, String],
    OP_B: VectorizableFunction[String, String],
    workflow: Vectorizable.Script[(String, String, String)]
  )

  describe("Given a two vectorisable operations OP_A and OP_B") {
    describe("and a workflow with 2 parallel OP_A operations and 1 OP_B operation") {

      /**
        * In order not to mix and mess the logs for multiple use of the tested workflow, we have to
        * instantiate one set of operation OP_A and OP_B at each run.
        */
      def withWorkflow(test: TestContext => Unit) {

        // Vectorised version of OP_A and OP_B
        def vec_OP(name: String)(ins: Seq[String]): Seq[String] = {
          logger.info("vec_IO start: {}", ins)
          Thread.sleep(100) // Simulates slow blocking computation
          val result = for (s <- ins) yield s"$s.($name)"
          logger.info("vec_IO end: {}", ins)
          result
        }

        val OP_A = new SeqSeqVectorizableFunction[String, String](vec_OP("A"), "OP_A")
        val OP_B = new SeqSeqVectorizableFunction[String, String](vec_OP("B"), "OP_B")


        // A task for the workflow
        def task(taskName: String, a: String) = (x: String) => {
          delayed {
            logger.info("Start Task:   {}", taskName)
            Thread.sleep(100) // Simulates CPU intensive computation
            logger.info("Result Ready: {}", taskName)
            x + "." + a
          }
        }

        val workflow =
          done("") >>= task("Task 0", "a") >>= {
            (x) => {
              unordered(
                done(x) >>= task("Task 1", "b1") >>= OP_A >>= task("Task 4", "c1"),
                done(x) >>= task("Task 3", "b2") >>= OP_B >>= task("Task 6", "c2"),
                done(x) >>= task("Task 2", "b3") >>= OP_A >>= task("Task 5", "c3")
              )
            }
          } >>= {
            case ((a, b, c)) => {
              done((a, b, c))
            }
          }

        test(TestContext(OP_A, OP_B, workflow))

      }

      withWorkflow { context =>
        import context._
        describe("The standard behaviour") {

          val result = go(workflow) // Note: we are analysing its logs below

          it("returns result as expected") {
            assert(result === (".a.b1.(A).c1", ".a.b2.(B).c2", ".a.b3.(A).c3"))
          }

          describe("OP_B") {
            it("is call once") {
              assert(logForOps(OP_B).size === 1)
            }
            it("receives 1 argument set") {
              logForOps(OP_B).foreach(call => assert(call.arity === 1))
            }
          }
          describe("OP_A") {
            it("is call once") {
              assert(logForOps(OP_A).size === 1)
            }
            it("receives 2 argument sets") {
              logForOps(OP_A).foreach(call => assert(call.arity === 2))
            }
          }
        }
      }

      describe("We can control vectorisation") {

        withWorkflow { context =>
          import context._

          describe("OP_B") {
            implicit val policy = evaluationPolicy(nt(
              // Trigger evaluation as soon as a OP_B is pending
              cl => if (cl.stats(OP_B) > 0) CallList(cl.groups(OP_B)) else CallList.empty[NT]
            ))

            val result = go(workflow)

            it("can be launched as soon as possible") {
              import org.scalatest.Matchers._
              logForOps(OP_A, OP_B).indexWhere(_.op == OP_B) should be < logForOps(OP_A, OP_B).indexWhere(_.op == OP_A)
            }
            it("still returns the expected result") {
              assert(result === (".a.b1.(A).c1", ".a.b2.(B).c2", ".a.b3.(A).c3"))
            }
          }
        }

        withWorkflow { context =>
          import context._

          describe("OP_A") {
            val policy = evaluationPolicy(nt(
              // Trigger evaluation as soon as a OP_A is pend_ing (ans prevent vectorisation)
              cl => if (cl.stats(OP_A) > 0) CallList(cl.groups(OP_A)) else CallList.empty
            ))

            val result = go(workflow)(policy) // explicit selection of the policy

            it("can be launched as soon as possible and no vectorisation occurs") {
              assert(logForOps(OP_A, OP_B).size === 3)
              logForOps(OP_A, OP_B).foreach(call => assert(call.arity === 1))
            }
            it("still returns the expected result") {
              assert(result === (".a.b1.(A).c1", ".a.b2.(B).c2", ".a.b3.(A).c3"))
            }
          }
        }
      }
    }
  }
}
