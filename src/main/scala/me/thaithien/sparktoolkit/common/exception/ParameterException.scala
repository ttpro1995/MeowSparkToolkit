package me.thaithien.sparktoolkit.common.exception

/**
 * @author baopng
 */

object ParameterException {

  case class TypeMismatchException(value: Any, require: String) extends Exception {
    override def getMessage = s"Cannot convert ${value.getClass} to ${require}."
  }

  case class UnknownParamException(value: Iterable[String]) extends Exception {
    override def getMessage = s"Unknown parameters: ${value.mkString(", ")}."
  }

  case class UnassignedParamException(value: String) extends Exception {
    override def getMessage = s"Unassigned parameters: ${value}."
  }

  case class ArgumentsWrongFormatException() extends Exception {
    override def getMessage = "Arguments must be a list of pairs [--param_name param_value]"
  }
}
