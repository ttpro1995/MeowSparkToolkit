package me.thaithien.sparktoolkit.common.parameter

import me.thaithien.sparktoolkit.common.exception.ParameterException.ArgumentsWrongFormatException

/**
 * @author baopng
 */

object ArgumentsParser {
  val parse: Array[String] => Map[String, String] = (args: Array[String]) => {
    if (args.length % 2 != 0) throw ArgumentsWrongFormatException()
    else args.sliding(2, 2).toList.map {
      case Array(opt, value) =>
        if (opt.startsWith("--")) {
          opt.drop(2).replace("-", "_") -> value
        } else {
          throw ArgumentsWrongFormatException()
        }
    }.toMap
  }
}
