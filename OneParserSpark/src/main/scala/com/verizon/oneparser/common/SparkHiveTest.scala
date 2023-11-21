package com.verizon.oneparser.common

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHiveTest extends App {
  val spark = SparkSession.builder().master("yarn").config("spark.submit.deployMode", "client").enableHiveSupport().getOrCreate()

  import spark.implicits._
  val someDF = Seq(
//    (8, "bat"),
//    (64, "mouse"),
//    (-27, "horse"),
      //(5, "lion")
//    (12, "null", "zebra", "wild", "test", "test1"),
//    (13, "null", "elephant", "wild", "test", "test1"),
//    (34, "null", "dog", "pet", "test", "test1")
    (12, "zebra", "zebra", "wild", "test", "test1"),
    (13, "elephant", "elephant", "wild", "test", "test1"),
    (34, "dog", "dog", "pet", "test", "test1")
  ).toDF("number", "word","animal", "livingtype", "test", "test1")

//  val logcodes_set2 = spark.sparkContext.broadcast[Array[Int]](Array(45250,45369))
//  println("logcodes_set2 >>>>>>>>>>>>>>> "+logcodes_set2.value)
//  println("logcodes_set2 >>>>>>>>>>>>>>> "+logcodes_set2.value.foreach(x => println(x)))
//  someDF.filter($"word"==="bat").select("number").write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("dmat_logs.sample")

//  someDF.write.partitionBy("number").mode(SaveMode.Overwrite).option("path", "hdfs://10.20.40.150:8020/apps/hive/warehouse/dmat_logs.db/sample").saveAsTable("dmat_logs.sample")

  //    dlfListDF.write.format("parquet").mode(SaveMode.Overwrite).insertInto(Config.HIVE_DB+Constants.TBL_B193_NAME)
  //    spark.sql("insert overwrite table "+Config.HIVE_DB+Constants.TBL_B193_NAME+" PARTITION(filename,inserteddate) select `(filename|inserteddate)?+.+`,fileName as filename,insertedDate as inserteddate from dlfList_DataFrame")

  someDF.createOrReplaceTempView("sample_dataframe")
  spark.sql("SET hive.exec.dynamic.partition = true")
  spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
//  spark.sql("SET hive.support.quoted.identifiers=none")
  spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
  spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
//  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

/*  spark.sql("select * from sample_dataframe").show()
  spark.catalog.refreshTable("numbersample")
  spark.sql("REFRESH TABLE numbersample")*/

//  spark.sql("CREATE TABLE numbersample(word string)\nPARTITIONED BY (number integer) STORED AS PARQUET\nLOCATION '/apps/hive/warehouse'")
//  spark.sql("ALTER TABLE numbersample ADD COLUMNS (animal string, livingtype string)")
//  spark.sql("insert overwrite table numbersample PARTITION(number) select null,animal,livingtype,test,number from sample_dataframe")
//  spark.sql("insert overwrite table numbersample PARTITION(number) select `(number)?+.+`, number from sample_dataframe")
//  someDF.write.format("parquet").partitionBy("number").mode(SaveMode.Overwrite).option("path", "/apps/hive/warehouse").saveAsTable("numbersample")
//  spark.sql("select * from numbersample").show

//  spark.sql("CREATE external TABLE numbersample1(word string)\nPARTITIONED BY (number integer) STORED AS PARQUET\nLOCATION '/apps/hive/warehouse/numbersample1'")
//  spark.sql("CREATE external TABLE numbersample2(word string)\nPARTITIONED BY (number integer) STORED AS PARQUET\nLOCATION '/apps/hive/warehouse/dmat_logs.db/numbersample2'")

//  spark.sql("select `number`,number from sample_dataframe").show(10)
  spark.sql("insert overwrite table numbersample1 PARTITION(number) select `number`,number from sample_dataframe")
  spark.sql("insert overwrite table dmat_logs.numbersample2 PARTITION(number) select `number`,number from sample_dataframe")
}
