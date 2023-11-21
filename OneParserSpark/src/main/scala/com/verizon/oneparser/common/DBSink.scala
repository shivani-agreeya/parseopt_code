package com.verizon.oneparser.common

import java.util.Properties

import com.verizon.oneparser.config.CommonConfigParameters
import org.apache.spark.sql.{DataFrame, SparkSession}

case object DBSink {
  def apply(spark: SparkSession, config: CommonConfigParameters, table: String = null): DBSink =
    DBSink(spark, config, table, null)
}

case class DBSink(spark: SparkSession, config: CommonConfigParameters, table: String, df: DataFrame) {

  def getDataFrame = df

  def read: DBSink = {
    read(table)
  }

  def read(table: String): DBSink = {
    val df = spark.read
      .option("driver", config.POSTGRES_DRIVER)
      .jdbc(config.POSTGRES_CONN_URL, table, new Properties())
    DBSink(spark, config, table, df)
  }

  def query(readQuery: String): DBSink = {
    query(readQuery, config.POSTGRES_CONN_URL)
  }

  def query(readQuery: String, url: String): DBSink = {
    val df = spark.read
      .format("jdbc")
      .option("driver", config.POSTGRES_DRIVER)
      .option("url", url)
      .option("query", readQuery)
      .load()
    DBSink(spark, config, table, df)
  }

  def transform(f: DataFrame => DataFrame): DBSink = {
    DBSink(spark, config, table, f(df))
  }

  def debug: Unit = {
    df.write
      .format("console")
      .option("truncate", false)
      .save()
  }

  def write: DBSink = {
    write(table)
  }

  def write(table: String): DBSink = {
    df.write
      .option("driver", config.POSTGRES_DRIVER)
      .jdbc(config.POSTGRES_CONN_URL, table, new Properties())
    DBSink(spark, config, table, df)
  }
}