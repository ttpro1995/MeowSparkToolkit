package me.thaithien.sparktoolkit.common.parameter

/**
 * @author baopng
 */

trait Parameter[+A, +B] {
  protected val value: Option[A]

  def getValue: B

  def isEmpty: Boolean = value.isEmpty

  def modify(new_value: String): Parameter[A, B] = {
    val field = this.getClass.getDeclaredField("value")
    field.setAccessible(true)
    field.set(this, Some(new_value))
    this
  }
}
