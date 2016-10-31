package iotestbed.boat.util

import scala.collection.immutable.ListMap
import scala.io.{Codec, Source}

object CSVReader {

  implicit val utf8 = Codec.UTF8

  def read(url: String) = {
    val linesIterator = Source.fromURL(url).getLines()
    val header = linesIterator.next()

    val columnName = header.split(",").map(_.trim)
    linesIterator.flatMap(
      line => {
        if (line.startsWith("#")) None
        else {
          val items = line.split(",").take(columnName.size).padTo(columnName.size, "").map(_.trim).toVector
          Some(ListMap(columnName zip items: _*))
        }
      }
    )
  }

}
