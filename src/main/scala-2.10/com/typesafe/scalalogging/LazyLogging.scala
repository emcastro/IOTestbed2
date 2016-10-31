package com.typesafe.scalalogging

import iotestbed.util.TypedMessage
import org.slf4j.{Logger, LoggerFactory}
;

/**
  * LazyLogging is not available in Scala 2.10.
  * This is just a minimal implementation for the cases used in this project
  */
class LazyLogging {

  class Slf4jLogger(slf4jLogger: Logger) {
    def info[T](message: TypedMessage[T], args: AnyRef*) = slf4jLogger.info(message, args: _*)
  }


  lazy val logger = new Slf4jLogger(LoggerFactory.getLogger(getClass))

}
