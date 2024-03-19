package me.thaithien.sparktoolkit.common.utils

import scala.io.Source

object LocalFileReader {
  def read(path: String): String = {
    if (path.startsWith("/")) {
      val source = Source.fromFile(path)
      val content = try source.getLines.mkString("\n") finally source.close()
      content
    } else {
      val source = Source.fromInputStream(LocalFileReader.getClass
        .getResourceAsStream("/" + path))
      val content = try source.getLines.mkString("\n") finally source.close()
      content
    }
  }
}
