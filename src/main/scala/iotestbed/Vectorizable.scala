package iotestbed

import java.util.Comparator
import java.util.concurrent.{Executors, PriorityBlockingQueue, ThreadFactory}

import com.typesafe.scalalogging.LazyLogging
import iotestbed.util.EasyNatTrans.NT
import iotestbed.util.{TypedMessage}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.{existentials, higherKinds}
import scalaz.Free.liftF
import scalaz.{-\/, Free, Functor, NaturalTransformation, \/-, ~>}

import CallList.CallList

object Vectorizable extends LazyLogging {

  def ifDefined[K, R](o: Option[K])(value: => Script[Option[R]]): Script[Option[R]] = {
    if (o.isDefined) value else done(None)
  }

  def orElse[T](mainClause: Script[Option[T]])(alternativeClause: Script[Option[T]]): Script[Option[T]] = {
    mainClause >>= {
      case s: Some[T] => done(s)
      case None => alternativeClause
    }
  }


  val NewThread = TypedMessage[String]("New Thread: [{}]")
  val VectorizedCall = TypedMessage[(VectorizableFunction[_, _], Integer)]("Vectorized call: {}(arg_count={})")

  /** Script is a free monad over computation steps */
  type Script[A] = Free[Step, A] // FIXME it should be covariant, but Free implementation is not

  /** Scalaz stuff light */
  implicit val functor: Functor[Step] = new Functor[Step] {
    def map[A, B](step: Step[A])(f: A => B): Step[B] = step.map(f)
  }

  // Vectorizable scripts API -------------------------

  val init: Script[Unit] = done(())

  def done[V](v: V): Script[V] = liftF(Done[V, V](v, identity))

  def delayed[V](v: => V): Script[V] = done(()) >>= (_ => done(v))

  def unordered[A, B](a: Script[A], b: Script[B]): Script[(A, B)] =
    liftF(ForkN[(A, B)](Seq(a, b), { case Seq(ra, rb) => (ra.asInstanceOf[A], rb.asInstanceOf[B]) }))

  def unordered[A, B, C](a: Script[A], b: Script[B], c: Script[C]): Script[(A, B, C)] =
    liftF(ForkN[(A, B, C)](Seq(a, b, c), { case Seq(ra, rb, rc) => (ra.asInstanceOf[A], rb.asInstanceOf[B], rc.asInstanceOf[C]) }))

  def unordered[A, B, C, D](a: Script[A], b: Script[B], c: Script[C], d: Script[D]): Script[(A, B, C, D)] =
    liftF(ForkN[(A, B, C, D)](Seq(a, b, c, d), { case Seq(ra, rb, rc, rd) => (ra.asInstanceOf[A], rb.asInstanceOf[B], rc.asInstanceOf[C], rd.asInstanceOf[D]) }))

  def unordered[A](scripts: Seq[Script[A]]): Script[Seq[A]] = liftF(ForkN[Seq[A]](scripts, x => x.asInstanceOf[Seq[A]]))


  private def join[T](id: Int, slot: Int, result: Any): Script[T] = liftF(Join[T](id, slot, result))

  // ------------------------------------------

  /** Computation steps */
  sealed trait Step[+T] {
    /** Enrich the current continuation */
    def map[Q](f: T => Q): Step[Q]
  }

  private case class Done[V, T](value: V, k: V => T) extends Step[T] {
    def map[Q](f: T => Q) = copy(k = k andThen f)
  }

  private case class ForkN[T](scripts: Seq[Script[_]], k: Seq[_] => T) extends Step[T] {
    def map[Q](f: T => Q) = copy(k = k andThen f)
  }

  /**
    * @param id Reference to the join state
    */
  private case class Join[T](id: Int, slot: Int, result: Any) extends Step[T] {
    // Mapping has no effect on Join
    def map[Q](f: T => Q) = this.asInstanceOf[Join[Q]]
  }

  /*private*/ def vectorizableCall[X, Y](func: VectorizableFunction[X, Y], arg: X): Script[Y] =
  liftF(VectorizableCall[Y](func.asInstanceOf[VectorizableFunction[Any, Any]], arg, (v) => v.asInstanceOf[Y]))


  /**
    * @param missingResultCount Number of forked script to wait
    * @param results            Storage of forked script results
    * @param k                  Continuation to use when missing = 0
    */
  private case class JoinInfo[T](missingResultCount: Int, results: Vector[Any], k: Seq[_] => Script[T])

  /** Run a script */
  def go[V](script: Script[V])(implicit policy: EvaluationPolicy = StandardEvaluationPolicy): V = {

    val executor = Executors.newCachedThreadPool(

      new ThreadFactory {
        private var runner = 0

        override def newThread(r: Runnable): Thread = {
          val thread: Thread = new Thread(r, "Vectorized call thread " + runner)
          logger.info(NewThread, thread.getName, runner: Integer)
          thread.setDaemon(true) // The thread doesn't prevent the JVM to stop
          runner += 1
          thread
        }
      })

    type VectorizableCallResult = (List[VectorizableCall[Free[Step, V]]], Seq[Any])

    val vectorizableCallResult = new PriorityBlockingQueue[VectorizableCallResult](
      11, // PriorityBlockingQueue class default initial capacity
      new Comparator[VectorizableCallResult] {
        override def compare(o1: VectorizableCallResult, o2: VectorizableCallResult): Int = {
          0 // no priority yet -- should be parameterisable
        }
      }
    )

    def retrieveVectorizedResults(isBlocking: Boolean) = {

      import scala.collection.JavaConversions._

      val result = mutable.ArrayBuffer[VectorizableCallResult]()

      if (isBlocking) {
        result += vectorizableCallResult.take() // blocking
      }
      vectorizableCallResult.drainTo(result)

      result.flatMap {
        case (calls, results) =>
          calls.zip(results).map { case (c, r) => c.k(r) }
      }

    }

    def selectAndExecute(pendingCount: Int, forked: List[Script[V]], allCalls: CallList[Script[V]]) = {
      val preSelection = policy(allCalls)
      val selection: CallList[Script[V]] =
        if (preSelection.isEmpty)
          if (pendingCount == 0 && forked.isEmpty) allCalls
          else CallList.empty
        else preSelection

      selection.groups.foreach {
        case (func, calls) =>
          executor.execute(new Runnable() {
            def run() {
              logger.info(VectorizedCall, func, calls.size: Integer)
              val f = func.asInstanceOf[VectorizableFunction[Any, Any]]
              val results = f.applyVector(calls.map(_.arg))
              vectorizableCallResult.put((calls, results))
            }
          })
      }


      val remainingCallList = allCalls - selection

      val retrievedForked = retrieveVectorizedResults(forked.isEmpty)

      // If nothing more is evaluable (forked is empty), blocks and waits for an IO result
      val newForked = if (retrievedForked.isEmpty) forked else forked ++ retrievedForked

      (newForked, remainingCallList, pendingCount + selection.size - retrievedForked.size)
    }
    /**
      * @param script   Script to run
      * @param callList Scripts calling a vectorisable function, waiting for a result
      * @param forked   List of the forked script waiting to run
      * @param joinMap  Storage of results and other information about join point
      * @return The result
      */
    @tailrec
    def interpret(script: Script[V],
                  callList: CallList[Script[V]],
                  pendingCount: Int,
                  forked: List[Script[V]],
                  joinMap: Map[Int, JoinInfo[_]],
                  idCounter: Int): V = {
      script.resume match {
        case -\/(step) => // new step
          step match {

            case Done(v, k) =>
              // Returning simple value
              interpret(
                script = k(v),
                callList = callList, // no change
                pendingCount = pendingCount, // no change
                forked = forked, // no change
                joinMap = joinMap, // no change
                idCounter = idCounter // no change
              )

            case v@VectorizableCall(_, _, _) =>
              // Calling a vectorisable function
              val allCalls = callList + v

              val (newForked, remainingCallList, newPendingCount) = selectAndExecute(pendingCount, forked, allCalls)

              interpret(
                script = newForked.head,
                callList = remainingCallList,
                pendingCount = newPendingCount,
                forked = newForked.tail,
                joinMap = joinMap, // no change
                idCounter = idCounter // no change
              )

            case ForkN(scripts, k) =>
              // Forking
              val id = idCounter
              // Add a step at the end of the script to store the result (using in Join)
              val scriptsWithEndMap = scripts.zipWithIndex.map { case (s, i) => s.flatMap(r => join[V](id, i, r)) }.toList

              val info =
                JoinInfo(
                  missingResultCount = scripts.size,
                  results = Vector.fill(scripts.size)(null),
                  k = k)

              interpret(
                script = scriptsWithEndMap.head,
                callList = callList, // no change
                pendingCount = pendingCount, // no change
                forked = scriptsWithEndMap.tail ++ forked,
                joinMap = joinMap + (id -> info),
                idCounter = idCounter + 1
              )

            case Join(id, slot, result) =>
              // Joining forked scripts
              val oldInfo = joinMap(id).asInstanceOf[JoinInfo[V]] // There is a theorem about this typecast

              val results = oldInfo.results.updated(slot, result)
              val missingCount = oldInfo.missingResultCount - 1

              // If it comes one day that vectorisable functions can change their results,
              // we'll need to change the way `missingCount` is calculated.
              // `val missingCount = results.count(_ == null)`

              if (missingCount == 0) {
                // All the work is done: Launch fork continuation
                val restartScript = done(results).flatMap(oldInfo.k)

                interpret(
                  script = restartScript,
                  callList = callList, // no change
                  pendingCount = pendingCount, // no change
                  forked = forked, // no change
                  joinMap = joinMap - id,
                  idCounter = idCounter // no change
                )
              }
              else {
                // Results are still missing, update the join map...
                val updatedJoinMap = joinMap.updated(id, JoinInfo(missingCount, results, oldInfo.k))

                val (newForked, remainingCallList, newPendingCount) =
                  if (forked.isEmpty) // if nothing is evaluable, select some and execute them
                    selectAndExecute(pendingCount, forked, callList)
                  else
                    (forked, callList, pendingCount)

                // Find a forked and run
                interpret(
                  script = newForked.head,
                  callList = remainingCallList,
                  pendingCount = newPendingCount,
                  forked = newForked.tail,
                  joinMap = updatedJoinMap,
                  idCounter = idCounter // no change
                )
              }
          }
        case \/-(a) => // no more steps
          assert(forked.isEmpty)
          assert(joinMap.isEmpty)
          a
      }
    }

    interpret(script, CallList.empty[Script[V]], 0, Nil, Map.empty, 0)
  }

  def evaluationPolicy(earlyEvaluationSelector: CallList ~> CallList) =
    new EvaluationPolicy {
      override def apply[V](callList: CallList[V]) = earlyEvaluationSelector(callList)
    }


}

import Vectorizable._

// Vectorizable building blocks -------------------------

class SeqSeqVectorizableFunction[X, Y](f: Seq[X] => Seq[Y], name: String = "<function>")
  extends VectorizableFunction[X, Y] {
  def applyVector(args: Seq[X]) = f(args)

  override def toString = name
}

class SetMapVectorizableFunction[X, Y](f: Set[X] => Map[X, Y], name: String = "<function>")
  extends VectorizableFunction[X, Y] {
  def applyVector(args: Seq[X]) = {
    val resultMap = f(args.toSet)
    args.map(resultMap)
  }

  override def toString = name
}

// Vectorizable function family ------------------------------

class SeqSeqVectorizableFunctionFamily[F, X, Y](f: F => Seq[X] => Seq[Y], name: String = "<function family>")
  extends VectorizableFunctionFamily[F, X, Y] {

  override def initFamily(family: F): VectorizableFunction[X, Y] = new SeqSeqVectorizableFunction(f(family), name + "@" + family)

  override def toString = name
}

class SetMapVectorizableFunctionFamily[F, X, Y](f: F => Set[X] => Map[X, Y], name: String = "<function family>")
  extends VectorizableFunctionFamily[F, X, Y] {

  override def initFamily(family: F): VectorizableFunction[X, Y] = new SetMapVectorizableFunction(f(family), name + "@" + family)

  override def toString = name
}


// Vectorizable scripts API -------------------------

case class VectorizableCall[T](func: VectorizableFunction[Any, Any], arg: Any, k: Any => T) extends Step[T] {
  def map[Q](f: T => Q) = copy(k = k andThen f)
}

trait VectorizableFunction[X, Y] extends ((X) => Script[Y]) {
  // It should be covariant, but Script implementation is not because Free is not.

  /** Apply the vectorized function on a vector of elements */
  def applyVector(args: Seq[X]): Seq[Y]

  /** Apply the vectorized function on one element */
  def apply(x: X) = vectorizableCall(this, x)
}

object SS {
  type ScriptSeq[T] = Script[Seq[T]]
}

trait VectorizableFunctionFamily[F, X, Y] extends (F => X => Script[Y]) {

  @volatile var cache = Map.empty[F, VectorizableFunction[X, Y]]

  def initFamily(f: F): VectorizableFunction[X, Y]

  /** Apply the vectorized(criteriaList: Seq[Criteria[R]]) function on one element */
  def apply(family: F): (X) => Script[Y] = { (x: X) =>
    val cached = cache.get(family) match {
      case Some(vf) => vf
      case None => {
        val vf = initFamily(family)
        cache += (family -> vf)
        vf
      }
    }

    vectorizableCall(cached, x)
  }

  def apply[T, NTT <: NT, FF[_], XX[_], YY[_]](family: FF[T])
                                              (implicit ff: FF[NTT] =:= F, xx: XX[NTT] =:= X, yy: Y =:= YY[NTT]): XX[T] => Script[YY[T]] = {
    apply(family.asInstanceOf[F]).asInstanceOf[XX[T] => Script[YY[T]]]
  }

  def apply[T, NTT <: NT, FF[_], XX[_], YY[_]](family: FF[T], x: XX[T])
                                              (implicit ff: FF[NTT] =:= F, xx: XX[NTT] =:= X, yy: Y =:= YY[NTT]): Script[YY[T]] =
    apply(family).apply(x)

}

object CallList {
  type CallList[V] = ListWithStats[VectorizableFunction[_, _], VectorizableCall[V]]

  private val _vfcTyper = (vfc: VectorizableCall[_]) => vfc.func

  private val _empty: CallList[Any] = ListWithStats(_vfcTyper, Nil)

  def empty[V]: CallList[V] = _empty.asInstanceOf[CallList[V]] // always return the same instance

  def apply[V](funcs: List[VectorizableCall[V]]): CallList[V] = ListWithStats(_vfcTyper, funcs)

}

/**
  * An evaluation policy takes available call lists and select those which will be evaluated earlier.
  * It lets the possibility not to wait until every possible computation be explored
  * before evaluating the waiting vectorisable functions.
  */
trait EvaluationPolicy extends (CallList ~> CallList)

/** The standard evaluation policy does not select any call list for early evaluation */
object StandardEvaluationPolicy extends EvaluationPolicy {

  def apply[V](callList: CallList[V]) = CallList.empty

}

