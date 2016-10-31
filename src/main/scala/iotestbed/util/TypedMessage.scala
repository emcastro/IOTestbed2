package iotestbed.util

import scala.language.implicitConversions

case class TypedMessage[T](msg: String) extends AnyVal {
  def decode(s: Seq[AnyRef])(implicit b: BuildableFromList[T]): T = {
    BuildableFromList.fromList(s)(b)
  }
}

object TypedMessage {
  implicit def typedMessageToString[T](b: TypedMessage[T]):String = b.msg
}
