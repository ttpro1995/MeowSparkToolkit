package me.thaithien.sparktoolkit.common.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.text.Normalizer

object TextProcessor {
  val normalize_text: UserDefinedFunction = udf { (text: String) =>
    val name_nor = Normalizer.normalize(text, Normalizer.Form.NFD)
    val name_replace = name_nor.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
      .replaceAll("đ", "d")
      .replaceAll("Đ", "d")
      .toLowerCase
      .replaceAll("[^a-z]+", " ")
      .trim()
    name_replace
  }

  val ngram: UserDefinedFunction = udf((xs: Seq[String], n: Int) =>
    (1 to n).flatMap(i => xs.sliding(i).filter(_.size == i).map("_" + _.mkString("_") + "_")).distinct.mkString(" "))
}
