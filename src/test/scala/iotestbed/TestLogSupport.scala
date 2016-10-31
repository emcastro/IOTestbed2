package iotestbed

import iotestbed.util.{BuildableFromList, TypedMessage}
import uk.org.lidalia.slf4jtest.{LoggingEvent, TestLogger, TestLoggerFactory}

class TestLogSupport(_logs: => Seq[LoggingEvent]) {

  lazy val logs = _logs

  import collection.convert.wrapAsScala._

  lazy val logsByKey: Map[String, Seq[Seq[AnyRef]]] =
    logs
      .groupBy(_.getMessage)
      .mapValues(_.map(_.getArguments.toIndexedSeq))

  def typedLogs[T](typedMessage: TypedMessage[T])(implicit b: BuildableFromList[T]): Seq[T] =
    logsByKey(typedMessage.msg).map(b.fromList)

}

object TestLogSupport {

  import collection.convert.wrapAsScala._

  def forAllEvents(loggerClass: Class[_]) = new TestLogSupport(TestLoggerFactory.getTestLogger(loggerClass).getAllLoggingEvents)

  def forEvents(loggerClass: Class[_]) = new TestLogSupport(TestLoggerFactory.getTestLogger(loggerClass).getLoggingEvents)

}
