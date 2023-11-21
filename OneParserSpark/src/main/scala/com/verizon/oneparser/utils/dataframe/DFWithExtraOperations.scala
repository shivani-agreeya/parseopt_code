package com.verizon.oneparser.utils.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

case class DFWithExtraOperations(df: DataFrame) {

  /**
   * Set nullable property of column.
   *
   * @param cn       is the column name to change
   * @param nullable is the flag to set, such that the column is  either nullable or not
   */
  def setNullableState(cn: String, nullable: Boolean): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if c.equals(cn) => StructField(c, t, nullable = nullable, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  /**
   * Set nullable property of columns to true except cn.
   *
   * @param cn is the column name to change
   */
  def setNullableStateExcept(cn: String): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = !c.equals(cn), m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}
