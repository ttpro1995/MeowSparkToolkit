package me.thaithien.sparktoolkit.common.utils

/**
 * @author baopng
 */

object Using {
  def apply[A, B <: {def close(): Unit}](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    } finally {
      closeable.close()
    }
}
