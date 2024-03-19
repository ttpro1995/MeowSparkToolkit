package me.thaithien.sparktoolkit.common.exception

/**
 * @author baopng
 */

object ReadingException {

  case class WrongInputFormatException() extends Exception {
    override def getMessage = s"Wrong input mode."
  }

  case class UnimplementedReadingCustomFormatException() extends Exception {
    override def getMessage = s"Function for reading custom format file is not implemented."
  }

}
