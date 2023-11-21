package com.verizon.oneparser.utils.dataframe

import org.apache.spark.sql.DataFrame

object ExtraDataFrameOperations {
  object implicits {
    implicit def dFWithExtraOperations(df: DataFrame) = DFWithExtraOperations(df)
  }
}
