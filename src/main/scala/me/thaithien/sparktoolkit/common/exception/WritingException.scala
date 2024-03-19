package me.thaithien.sparktoolkit.common.exception

/**
 * @author baopng
 */

object WritingException {

  case class WrongOutputFormatException() extends Exception {
    override def getMessage = s"Wrong output mode."
  }

  case class UnimplementedWritingCustomFormatException() extends Exception {
    override def getMessage = s"Function for writing custom format file is not implemented."
  }

}
