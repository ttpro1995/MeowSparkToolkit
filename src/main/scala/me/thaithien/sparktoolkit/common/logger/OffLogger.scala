package me.thaithien.sparktoolkit.common.logger

import org.apache.log4j.{Level, Logger}

/**
 * @author baopng
 */

trait OffLogger {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
