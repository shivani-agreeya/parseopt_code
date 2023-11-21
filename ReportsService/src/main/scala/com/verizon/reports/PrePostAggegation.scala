package com.verizon.reports

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.immutable._
import scala.collection.mutable.Map
import org.apache.spark.sql.Row
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import com.verizon.reports.common.{CommonConfigParameters, CommonDaoImpl, Constants}
import org.apache.spark.sql.Dataset

object PrePostAggegation extends Serializable with LazyLogging {
  
  def savePrepostRangeVariableDataToDb(spark: SparkSession, kpiName: String, processType: String, reportId: Long, ranges: Dataset[(Double, Double)], reportprefix: String, commonConfigParams: CommonConfigParameters): Unit = {
    ranges.createOrReplaceTempView("ranges")

    //-99999 to - -100 -100 to --90 -90 - -80     < -80 - 9999999
    val rangeValRdd = spark.sql("select report.fileName,report." + kpiName + ",ranges._1 as rangeMin,ranges._2 as rangeMax" +
      " from  report join ranges on ranges._1 < report." + kpiName + " and ranges._2 >= report." + kpiName + " where report." + kpiName + " > -9999999.0")
    //rsrpRangeValRdd.show
    rangeValRdd.cache()  
    val totalCount = rangeValRdd.count
    //val rangeCounts=rangeValRdd.map(rangeVal => (rangeVal.getDouble(1),rangeVal.getDouble(2).toString+"<>"+rangeVal.getDouble(3).toString)).groupBy("_2").count()
    val rangeCounts = rangeValRdd.groupBy("rangeMin", "rangeMax").count
    //rangeCounts.show()
    val rangeCountArr = rangeCounts.collect
    val sortedRangeCountArr = rangeCountArr.sortBy(row => row.getDouble(0))
    val totalRows = if (rangeCountArr.length > 0) rangeCountArr.map(row => row.getLong(2)).reduce((a, b) => a + b) else 0;
    if (totalRows > 0) {
      val rangeCountTuples = sortedRangeCountArr.map(ele => (ele.getDouble(0) + "<>" + ele.getDouble(1), ele.getLong(2).toDouble / totalRows, 0.0, ele.getLong(2)))
      def cdf(a: (String, Double, Double, Long), b: (String, Double, Double, Long)): (String, Double, Double, Long) = {
        (b._1, b._2, a._3 + b._2, b._4.toLong)
      }
      import java.math._
      val cdfArr = rangeCountTuples.scanLeft(("", 0.0, 0.0, 0L))(cdf).filter(ele => ele._1 != "").map(curRow => (curRow._1, curRow._2, curRow._3, curRow._4))
      def roundOff(someVal: Double): Double = {
        val value = new BigDecimal(someVal)
        val roundedVal = value.setScale(4, RoundingMode.HALF_UP)
        roundedVal.doubleValue
      }
      val roundedCdfArr = cdfArr.map(value => (value._1, roundOff(value._2), roundOff(value._3), value._4))
      logger.info(roundedCdfArr.deep.mkString("\n"))
      CommonDaoImpl.insertPdfCdfReportData(kpiName, roundedCdfArr, processType, reportId, reportprefix, totalRows, commonConfigParams)
    }
    rangeValRdd.unpersist();
  }
  
  def savePrepostFixedRangeDataToDb(spark: SparkSession, kpiName: String, processType: String, reportId: Long, ranges: Dataset[String], reportPrefix: String, commonConfigParams: CommonConfigParameters): Unit = {
    ranges.createOrReplaceTempView("ranges")
    val rangeValRdd = spark.sql("select report.fileName,report." + kpiName + ",ranges.value" +
      " from  report join ranges on  report." + kpiName + " = ranges.value ")

    /* val rangeValRdd=spark.sql("select report.fileName,report.lteRrcState,ranges.value"+
       " from  report join ranges on  report.lteRrcState = ranges.value ")*/

    //rsrpRangeValRdd.show
    // val rangeCounts=rangeValRdd.map(rangeVal => (rangeVal.getDouble(1),rangeVal.getDouble(2).toString+"<>"+rangeVal.getDouble(3).toString)).groupBy("_2").count()
    val rangeCounts = rangeValRdd.groupBy("value").count()

    //rangeCounts.show()
    val rangeCountOrdered = rangeCounts.orderBy("ranges.value")
    
    logger.info("fixed range kpi distribution data >>>>>>>>>>>>>>>>>> Start ")
    rangeCountOrdered.show()
    logger.info("fixed range kpi distribution data >>>>>>>>>>>>>>>>>> end ")
    
    val rangeCountArr=rangeCountOrdered.collect()
    logger.info("")

    val totalRows = if (rangeCountArr.length > 0) rangeCountArr.map(row => row.getLong(1)).reduce((a, b) => a + b) else 0;
    if (totalRows > 0) {
      val rangeCountTuples = rangeCountArr.map(ele => (ele.getString(0), ele.getLong(1).toDouble / totalRows, 0.0, ele.getLong(1)))
      def cdf(a: (String, Double, Double, Long), b: (String, Double, Double, Long)): (String, Double, Double, Long) = {
        (b._1, b._2, a._3 + b._2, b._4)
      }

      val cdfArr = rangeCountTuples.scanLeft(("", 0.0, 0.0, 0L))(cdf).filter(ele => ele._1 != "")
      logger.info(cdfArr.deep.mkString("\n"))
      //  CommonDaoImpl.insertCoverageReportData(kpiName,cdfArr,processType,reportId)
      CommonDaoImpl.insertPdfCdfReportData(kpiName, cdfArr, processType, reportId, reportPrefix, totalRows, commonConfigParams)

    }

  }



  def  SaveUECoverage(spark: SparkSession,jobId : Long,processType: String,commonConfigParams: CommonConfigParameters): Boolean= {

    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]


    val aggUECoverageReportData1= spark.sql("select LTEPccEarfcnDL,count(ltePccEarfcnDL) earnfcn_count," +
      "avg(if(ltePccRsrp="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,ltePccRsrp)) avgrsrp," +
      "avg(if(ltePccRssi="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,ltePccRssi)) avgrssi," +
      "avg(if(ltePccSinr="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,ltePccSinr)) avgsinr," +
      "avg(if(ltePccRsrq="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,ltePccRsrq)) avgrsrq," +
      "count(if(LTEPccRsrp="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,LTEPccRsrp)) countrsrp , count(if(LTEPccRssi="+Constants.DEFAULT_KPI_DOUBLE_VAL+",null,LTEPccRssi)) countrssi ," +
      "min(if(LTEPccRssi="+Constants.DEFAULT_KPI_DOUBLE_VAL+",0.0,LTEPccRssi)) minrssi , max(if(LTEPccRssi="+Constants.DEFAULT_KPI_DOUBLE_VAL+",0.0,LTEPccRssi)) maxrssi  " +
      " from report where LTEPccEarfcnDL!="+Constants.DEFAULT_KPI_DOUBLE_VAL+"  and (ltePccRsrp!="+Constants.DEFAULT_KPI_DOUBLE_VAL+" or ltePccRsrq!="+Constants.DEFAULT_KPI_DOUBLE_VAL+" ) group by LTEPccEarfcnDL")

    import spark.implicits._


   // val reportId =jobId

  //  val reportIdB=spark.sparkContext.broadcast(reportId)
    if(aggUECoverageReportData1.count >0) {

           val earnfcnTotalCount = aggUECoverageReportData1.select("earnfcn_count").rdd.map(_ (0).asInstanceOf[Long]).reduce(_ + _)
      val earnfcnTotalCountBc = spark.sparkContext.broadcast(earnfcnTotalCount)

      val rssiTotalCount = aggUECoverageReportData1.select("countrssi").rdd.map(_ (0).asInstanceOf[Long]).reduce(_ + _)
      val rssiTotalCountBc = spark.sparkContext.broadcast(rssiTotalCount)

      //logger.info("=================================================================fetching UE COVERAGE from below RDD!!!!!!!!!")
      //EARFCN distribution

      //case class LteRaReport(jobid: Long,process_type: String,var_kpi_range: String,fixed_kpi_val: String,kpi_name: String,pdf: Double,cdf: Double,count: Long,bin_average: Double,
      // kpi_min_val: Double,kpi_max_val: Double,kpi_avg_val: Double,groupbyName: String,report_name: String,bin_percent: Double, distribution: Double)
      //aggUECoverageReportData1.repartition(100).flatMap((row) => {
      val lteUECovReport = 
        aggUECoverageReportData1.flatMap((row) => {
        val lteEarnfcn = row.getInt(0)
        val earnfcnCount = row.getLong(1)
        val avgrsrp = row.getDouble(2)
        val avgrssi = row.getDouble(3)
        val avgsinr = row.getDouble(4)
        val avgrsrq = row.getDouble(5)
        val countrsrp = row.getLong(6)
        val countrssi = row.getLong(7)
        val minrssi = row.getDouble(8)
        val maxrssi = row.getDouble(9)

        val binRssiPercent = if (rssiTotalCountBc.value == 0.0) 0.0 else (countrssi * 100.0) / rssiTotalCountBc.value
        val earnfcnDistribution = if (earnfcnTotalCountBc.value == 0.0) 0.0 else (earnfcnCount.toDouble / earnfcnTotalCountBc.value.toDouble)

        // List(LteRaReport(reportIdB.value,processType,null,null,"distribution",0.0,0.0,0,0.0,null,null,null,lteEarnfcn.toString,"EARFCNdistribution",0,earnfcnDistribution))

        List(LteRaReport(jobId, processType, null, null, "LTEPccRsrp", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrsrp, lteEarnfcn.toString, "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccRssi", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrssi, lteEarnfcn.toString, "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccSinr", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgsinr, lteEarnfcn.toString, "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccRsrq", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrsrq, lteEarnfcn.toString, "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "distribution", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, 0.0, lteEarnfcn.toString, "EARFCNdistribution", 0, earnfcnDistribution),
          LteRaReport(jobId, processType, null, null, "LTEPccRssi", 0.0, 0.0, countrssi, 0.0, minrssi, maxrssi, avgrssi, lteEarnfcn.toString, "DominantRSSI", binRssiPercent, 0))

      })


      lteUECovReport.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))


      val aggUECoverageReportData2 = spark.sql("select count(ltePccEarfcnDL) earnfcn_count," +
        "avg(if(ltePccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRsrp)) avgrsrp," +
        "avg(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRsrq)) avgrsrq," +
        "avg(if(ltePccRssi=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRssi)) avgrssi," +
        "avg(if(ltePccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccSinr)) avgsinr" +
        " from report where  LTEPccEarfcnDL!="+Constants.DEFAULT_KPI_DOUBLE_VAL+"  ")
        //aggUECoverageReportData2.repartition(100).flatMap
      val lteUECovReport1 = aggUECoverageReportData2.flatMap((row) => {
        val earnfcnCount = row.getLong(0)
        val avgrsrp = row.getDouble(1)
        val avgrsrq = row.getDouble(2)
        val avgrssi = row.getDouble(3)
        val avgsinr = row.getDouble(4)

        List(LteRaReport(jobId, processType, null, null, "LTEPccRsrp", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrsrp, "All", "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccRssi", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrssi, "All", "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccSinr", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgsinr, "All", "CoverageAverage", 0, 0),
          LteRaReport(jobId, processType, null, null, "LTEPccRsrq", 0.0, 0.0, earnfcnCount, 0.0, 0.0, 0.0, avgrsrq, "All", "CoverageAverage", 0, 0))
      })


      lteUECovReport1.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))
      // logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>END of RDD>>>>>>>>>>>>>>>>>>")
     SaveStDeviatationData(spark, jobId, processType,commonConfigParams)
      true
    }
    false
  }
  
  def generateCongestionChart(spark: SparkSession,xAxisKpiName: String,averagedKpiName: String,
      normalizedToMaxValue: Boolean,grpDefaultValue: String,avgDefaultValue: String,jobId: Long,processType: String,reportName: String,
      commonConfigParams: CommonConfigParameters): Boolean = {
    import spark.implicits._
    logger.info("<<<<<<<CONGESTION DATA FOR KPI"+averagedKpiName+" AGAINST "+xAxisKpiName+">>>>>>>>>>>")
    var dataFlag=false
     val averageDf= spark.sql("select avg("+averagedKpiName+") as avgval,"+xAxisKpiName+" from report  where "+xAxisKpiName+" != "+grpDefaultValue
         +" and "+averagedKpiName +" != "+avgDefaultValue
         +" group by  "+xAxisKpiName +" order by "+xAxisKpiName)
         
     averageDf.show(100)
     if(averageDf.count()>0){
       logger.info("CONGESTION KPI"+averagedKpiName+" AGAINST "+xAxisKpiName +" COUNT >>"+averageDf.count)
       val aggData: Array[Row]=averageDf.collect();
       CommonDaoImpl.saveCongestionData(aggData,xAxisKpiName,averagedKpiName,jobId,processType,reportName,commonConfigParams)
       dataFlag=true
     }
     
     
     dataFlag
  }
  def SaveStDeviatationData(spark: SparkSession,jobId : Long,processType: String,commonConfigParams: CommonConfigParameters) : Unit = {

    import spark.implicits._


    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]

    val aggUECoverageReportData3 = spark.sql("select avg(if(LTEPccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRsrp)) avgrsrp," +
      "avg(if(LTEPccRssi=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRssi)) avgrssi," +
      "avg(if(LTEPccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccSinr)) avgsinr,avg(if(LTEPccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRsrq)) avgrsrq ," +
      "count(if(LTEPccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRsrp)) countrsrp , count(if(LTEPccRssi=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRssi)) countrssi ," +
      "min(if(LTEPccRssi=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRssi)) minrssi , max(if(LTEPccRssi=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRssi)) maxrssi,  " +
      "min(if(LTEPccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRsrp)) minrsrp , max(if(LTEPccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRsrp)) maxrsrp,  " +
      "min(if(LTEPccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRsrq)) minrsrq , max(if(LTEPccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccRsrq)) maxrsrq , " +
      "min(if(LTEPccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccSinr)) minsinr , max(if(LTEPccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccSinr)) maxsinr,  " +
      "min(if(LTEPccPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccPuschTxPwr)) minpcctxpwr , max(if(LTEPccPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPccPuschTxPwr)) maxpcctxpwr,  " +
      "avg(if(LTEPccPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccPuschTxPwr)) avgltePccPuschTxPwr," +
      "min(if(LTEPucchActTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPucchActTxPwr)) minpucchacttxpwr , max(if(LTEPucchActTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPucchActTxPwr)) maxpucchacttxpwr,  " +
      "avg(if(LTEPucchActTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPucchActTxPwr)) avgpucchacttxpwr," +
      "min(if(LTEPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPuschTxPwr)) mintxpwr , max(if(LTEPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPuschTxPwr)) maxtxpwr,  " +
      "avg(if(LTEPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPuschTxPwr)) avgpuschtxpwr," +
      "min(if(LTEMacTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEMacTputDl)) minmacdl , max(if(LTEMacTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEMacTputDl)) maxmacdl,  " +
      "avg(if(LTEMacTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEMacTputDl)) avgmactputdl," +
      "min(if(LTEMacTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEMacTputUl)) minmacul , max(if(LTEMacTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEMacTputUl)) maxmacul , " +
      "avg(if(LTEMacTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEMacTputUl)) avgmacul ," +
      "min(if(LTERlcTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTERlcTputDl)) minrlctputdl , max(if(LTERlcTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTERlcTputDl)) maxrlctputdl,  " +
      "avg(if(LTERlcTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTERlcTputDl)) avgrlctputdl," +
      "min(if(LTERlcTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTERlcTputUl)) minrlctputul, max(if(LTERlcTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTERlcTputUl)) maxrlctputul,  " +
      "avg(if(LTERlcTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTERlcTputUl)) avgrlctputul," +
      "min(if(LTEPdschTput=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPdschTput)) minpdschtput , max(if(LTEPdschTput=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,LTEPdschTput)) maxpdschtput , " +
      "avg(if(LTEPdschTput=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPdschTput)) avgpdschtput," +
      "count(if(LTEPccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRsrq)) countrsrq , count(if(LTEPccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccSinr)) countsinr ," +
      "count(if(LTEPccPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccPuschTxPwr)) countLTEPccPuschTxPwr , count(if(LTEPucchActTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPucchActTxPwr)) countLTEPucchActTxPwr ," +
      "count(if(LTEPuschTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPuschTxPwr)) countLTEPuschTxPwr , count(if(LTEMacTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEMacTputDl)) countLTEMacTputDl ," +
      "count(if(LTEMacTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEMacTputUl)) countLTEMacTputUl , count(if(LTERlcTputDl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTERlcTputDl)) countLTERlcTputDl ," +
      "count(if(LTERlcTputUl=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTERlcTputUl)) countLTERlcTputUl , count(if(LTEPdschTput=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPdschTput)) countLTEPdschTput " +
      " from report where LTEPccEarfcnDL!=" + Constants.DEFAULT_KPI_DOUBLE_VAL + " ")


    val lteUECovReport2 = aggUECoverageReportData3.flatMap((row) => {

      val avgrsrp = toDouble(row,0)
      val avgrssi = toDouble(row,1)
      val avgsinr = toDouble(row,2)
      val avgrsrq = toDouble(row,3)
      val countrsrp = toLong(row,4)
      val countrssi = toLong(row,5)
      val minrssi = toDouble(row,6)
      val maxrssi = toDouble(row,7)
      val minrsrp = toDouble(row,8)
      val maxrsrp = toDouble(row,9)
      val minrsrq = toDouble(row,10)
      val maxrsrq = toDouble(row,11)
      val minsinr = toDouble(row,12)
      val maxsinr = toDouble(row,13)
      val minpcctxpwr= toDouble(row,14)
      val maxpcctxpwr= toDouble(row,15)
      val avgltePccPuschTxPwr= toDouble(row,16)

      val minpucchacttxpwr= toDouble(row,17)
      val maxpucchacttxpwr= toDouble(row,18)
      val avgpucchacttxpwr = toDouble(row,19)
      val mintxpwr= toDouble(row,20)
      val maxtxpwr= toDouble(row,21)
      val avgpuschtxpwr= toDouble(row,22)
      val minmacdl= toDouble(row,23)
      val maxmacdl= toDouble(row,24)
      val avgmactputdl= toDouble(row,25)
      val minmacul = toDouble(row,26)
      val maxmacul = toDouble(row,27)
      val avgmacul = toDouble(row,28)
      val minrlctputdl = toDouble(row,29)
      val maxrlctputdl = toDouble(row,30)
      val avgrlctputdl = toDouble(row,31)
      val minrlctputul = toDouble(row,32)
      val maxrlctputul = toDouble(row,33)
      val avgrlctputul = toDouble(row,34)
      val minpdschtput = toDouble(row,35)
      val maxpdschtput = toDouble(row,36)
      val avgpdschtput = toDouble(row,37)

      val countrsrq = toLong(row,38)
      val countsinr = toLong(row,39)
      val countLTEPccPuschTxPwr = toLong(row,40)
      val countLTEPucchActTxPwr = toLong(row,41)
      val countLTEPuschTxPwr = toLong(row,42)
     val countLTEMacTputDl = toLong(row,43)//if(row.get(43)!=null) row.getLong(43) else 0L
      val countLTEMacTputUl = toLong(row,44)
      val countLTERlcTputDl = toLong(row,45)
      val countLTERlcTputUl = row.getLong(46)
      val countLTEPdschTput = toLong(row,47)

      List(LteRaReport(jobId, processType, null, null, "LTEPccRsrp", 0.0, 0.0, countrsrp, 0.0, minrsrp, maxrsrp, avgrsrp, "Dominant RSRP", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEPccRsrq", 0.0, 0.0, countrsrq, 0.0, minrsrq, maxrsrq, avgrsrq, "Dominant RSRQ", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEPccRssi", 0.0, 0.0, countrssi, 0.0, minrssi, maxrssi, avgrssi, "Dominant RSSI", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEnPccSinr", 0.0, 0.0, countsinr, 0.0, minsinr, maxsinr, avgsinr, "Dominant CINR", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "NCell10RSRP", 0.0, 0.0, countrsrp, 0.0, minrsrp, maxrsrp, avgrsrp, "Serving RSRP", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "NCell10RSRQ", 0.0, 0.0, countrsrq, 0.0, minrsrq, maxrsrq, avgrsrq, "Serving RSRQ", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEPccSinr", 0.0, 0.0, countsinr, 0.0, minsinr, maxsinr, avgsinr, "Serving CINR", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "NCell10RSSI", 0.0, 0.0, countrssi, 0.0, minrssi, maxrssi, avgrssi, "Serving RSSI", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEPuschTxPwr", 0.0, 0.0, countLTEPuschTxPwr, 0.0, mintxpwr, maxtxpwr, avgpuschtxpwr, "Tx Power", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEPdschTput", 0.0, 0.0, countLTEPdschTput, 0.0, minpdschtput, maxpdschtput, avgpdschtput, "PDSCH Total DL Throughput", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEMacTputDl", 0.0, 0.0, countLTEMacTputDl, 0.0, minmacdl, maxmacdl, avgmactputdl, "Mac Uplink Throughput", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTEMacTputUl", 0.0, 0.0, countLTEMacTputUl, 0.0, minmacul, maxmacul, avgmacul, "Mac Downlink Throughput", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTERlcTputDl", 0.0, 0.0, countLTERlcTputDl, 0.0, minrlctputdl, maxrlctputdl, avgrlctputdl, "RLC uplink Thoughput", "KPINameSTDev", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTERlcTputUl", 0.0, 0.0, countLTERlcTputUl, 0.0, minrlctputul, maxrlctputul, maxrlctputul, "RLC Downlink Thoughput", "KPINameSTDev", 0, 0)

      )
    })


    lteUECovReport2.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))
  }

  
  def SaveRCCConnectionData(spark: SparkSession,jobId : Long,processType: String,commonConfigParams: CommonConfigParameters) : Boolean = {

    import spark.implicits._
    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]


    val aggUECoverageReportData2 = spark.sql("select count(if(lteRrcConRej=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConRej)) countlteRrcConRej," +
      "count(if(lteRrcConRestRej=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConRestRej)) countlteRrcConRestRej," +
      "count(if(lteRrcConRelCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConRelCnt)) countlteRrcConRelCnt," +
      "count(if(lteActDefContAcptCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteActDefContAcptCnt)) countlteActDefContAcptCnt," +
      "count(if(lteActDedContAcptCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteActDedContAcptCnt)) countlteActDedContAcptCnt," +
      "count(if(lteBearerModAcptCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteBearerModAcptCnt)) countlteBearerModAcptCnt," +
      "count(if(lteHoFailure=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteHoFailure)) countlteHoFailure," +
      "count(if(lteIntraHoFail=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteIntraHoFail)) countlteIntraHoFail," +
      "count(if(lteAttRej=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteAttRej)) countlteAttRej," +
      "count(if(lteActDefContRejCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteActDefContRejCnt)) countlteActDefContRejCnt," +
      "count(if(lteActDedContRejCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteActDedContRejCnt)) countlteActDedContRejCnt," +
      "count(if(lteBearerModRejCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteBearerModRejCnt)) countlteBearerModRejCnt," +
      "count(if(lteReselFromGsmUmtsFail=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteReselFromGsmUmtsFail)) countlteReselFromGsmUmtsFail," +
      "count(if(lteRrcConSetupCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConSetupCnt)) countlteRrcConSetupCnt," +
      "count(if(lteRrcConReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConReqCnt)) countlteRrcConReqCnt," +
       "count(if(ltePdnConntReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,ltePdnConntReqCnt)) countltePdnConntReqCnt," +
      "count(if(lteRrcConSetupCmpCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConSetupCmpCnt)) countlteRrcConSetupCmpCnt," +
      "count(if(lteAttAcptCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteAttAcptCnt)) countlteAttAcptCnt," +
      "count(if(lteRrcConeconfCmpCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConeconfCmpCnt)) countlteRrcConeconfCmpCnt," +
      "count(if(lteRrcConReconfigCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConReconfigCnt)) countlteRrcConReconfigCnt," +
      "count(if(lteDetachReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteDetachReqCnt)) countlteDetachReqCnt," +
      "count(if(lteRrcConRestReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConRestReqCnt)) countlteRrcConRestReqCnt," +
      "count(if(lteTaAcptCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteTaAcptCnt)) countlteTaAcptCnt," +
      "count(if(lteTaReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteTaReqCnt)) countlteTaReqCnt," +
      "count(if(lteTaRej=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteTaRej)) countlteTaRej, " +
      "count(if(lteAttCmpCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteAttCmpCnt)) countlteAttCmpCnt, " +
      "count(if(lteAttReqCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteAttReqCnt)) countlteAttReqCnt, " +
      "count(if(lteRrcConeconfCmpCnt=" + Constants.DEFAULT_KPI_INTEGER_VAL + ",null,lteRrcConeconfCmpCnt)) countlteRrcConReconfCmpCnt " +
        " from report ")

        if(aggUECoverageReportData2.count> 0)
        {
      val lteUECovReport = aggUECoverageReportData2.flatMap((row) => {
      val countlteRrcConRej = toLong(row,0)
      val countlteRrcConRestRej = toLong(row,1)
      val countlteRrcConRelCnt = toLong(row,2)
      val countlteActDefContAcptCnt = toLong(row,3)
      val countlteActDedContAcptCnt = toLong(row,4)
      val countlteBearerModAcptCnt = toLong(row,5)
      val countlteHoFailure = toLong(row,6)
      val countlteIntraHoFail = toLong(row,7)
      val countlteAttRej = toLong(row,8)
      val countlteActDefContRejCnt = toLong(row,9)
      val countlteActDedContRejCnt = toLong(row,10)
      val countlteBearerModRejCnt = toLong(row,11)
      val countlteReselFromGsmUmtsFail = toLong(row,12)
      val countlteRrcConSetupCnt = toLong(row,13)
      val countlteRrcConReqCnt = toLong(row,14)
      val countltePdnConntReqCnt = toLong(row,15)
      val countlteRrcConSetupCmpCnt = toLong(row,16)
      val countlteAttAcptCnt = toLong(row,17)
      val countlteRrcConeconfCmpCnt = toLong(row,18)
      val countlteRrcConReconfigCnt = toLong(row,19)
      val countlteDetachReqCnt = toLong(row,20)
      val countlteRrcConRestReqCnt = toLong(row,21)
      val countlteTaAcptCnt = toLong(row,22)
      val countlteTaReqCnt = toLong(row,23)
      val countlteTaRej = toLong(row,24)
      val countlteAttCmpCnt = toLong(row,25)
      val countlteAttReqCnt = toLong(row,26)
    val countlteRrcConReconfCmpCnt=toLong(row, 27)


      List(LteRaReport(jobId, processType, null, null, "LTE RRC Connection Release", 0.0, 0.0, countlteRrcConRelCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reject", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE EUTRAN Attach Complete", 0.0, 0.0, countlteAttCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Attach Accept", 0.0, 0.0, countlteAttAcptCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Setup", 0.0, 0.0, countlteRrcConSetupCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Request", 0.0, 0.0, countlteRrcConReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE PDN Connectivity Request", 0.0, 0.0, countltePdnConntReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Attach Request", 0.0, 0.0, countlteAttReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Setup Success", 0.0, 0.0, countlteRrcConSetupCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reestablishment Reject", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE EMM Registered", 0.0, 0.0, countlteAttCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reconfiguration Success", 0.0, 0.0, countlteRrcConReconfCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reconfiguration", 0.0, 0.0, countlteRrcConReconfigCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Attach Complete", 0.0, 0.0, countlteAttCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Dropped", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Setup Failure", 0.0, 0.0, countlteRrcConRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Detach Request", 0.0, 0.0, countlteDetachReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Intra Handover Failure", 0.0, 0.0, countlteIntraHoFail, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reestablishment Request", 0.0, 0.0, countlteRrcConRestReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE RRC Connection Reestablishment Failure", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Tracking Area Update Accept", 0.0, 0.0, countlteTaAcptCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Tracking Area Update Request", 0.0, 0.0, countlteTaReqCnt, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0),
        LteRaReport(jobId, processType, null, null, "Setup Failure", 0.0, 0.0, countlteRrcConRej, 0.0, 0.0, 0.0, 0.0, null, "RRC connections outcomes", 0, 0),
        LteRaReport(jobId, processType, null, null, "Dropped", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "RRC connections outcomes", 0, 0),
        LteRaReport(jobId, processType, null, null, "Normal release", 0.0, 0.0, countlteRrcConRelCnt, 0.0, 0.0, 0.0, 0.0, null, "RRC connections outcomes", 0, 0),
        LteRaReport(jobId, processType, null, null, "Handover (LTE to LTE)", 0.0, 0.0, (countlteHoFailure +countlteIntraHoFail), 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "RRC re-establishment", 0.0, 0.0, countlteRrcConRestRej, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "Attach", 0.0, 0.0, countlteAttCmpCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-OK", 0, 0),
        LteRaReport(jobId, processType, null, null, "Attach", 0.0, 0.0, countlteAttRej, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "Default EPS Bearer Act.", 0.0, 0.0, countlteActDefContAcptCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-OK", 0, 0),
        LteRaReport(jobId, processType, null, null, "Default EPS Bearer Act.", 0.0, 0.0, countlteActDefContRejCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "Dedicated EPS Bearer Act.", 0.0, 0.0, countlteActDedContAcptCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-OK", 0, 0),
        LteRaReport(jobId, processType, null, null, "Dedicated EPS Bearer Act.", 0.0, 0.0, countlteActDedContRejCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "Modify EPS Bearer", 0.0, 0.0, countlteBearerModAcptCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-OK", 0, 0),
        LteRaReport(jobId, processType, null, null, "Modify EPS Bearer", 0.0, 0.0, countlteBearerModRejCnt, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "All reselections from UMTS", 0.0, 0.0, countlteReselFromGsmUmtsFail, 0.0, 0.0, 0.0, 0.0, null, "LTEproceduresstatistics-FAILED", 0, 0),
        LteRaReport(jobId, processType, null, null, "LTE Tracking Area Update Reject", 0.0, 0.0, countlteTaRej, 0.0, 0.0, 0.0, 0.0, null, "EventCount", 0, 0))

     })


    lteUECovReport.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))
true

        }
false
  }
  
  def saveReport(spark: SparkSession,jobId : Long,processType: String,commonConfigParams: CommonConfigParameters) : Boolean= {

   import spark.implicits._

   implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]


   val aggReporttData = spark.sql("select avg(if(ltePccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRsrp)) avgrsrp," +
     "avg(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRsrq)) avgrsrq," +
     "avg(if(LTEPucchActTxPwr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPucchActTxPwr)) avgpucch," +
     "avg(if(ltePccSinr=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccSinr)) avgsinr," +
     "count(if(LTEPccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,LTEPccRsrp)) countrsrp ," +
     "count(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",null,ltePccRsrq)) countrsrq " +
     " from report ")

if(aggReporttData.count> 0) {
   val report1 = aggReporttData.flatMap((row) => {
   val avgrsrp = toDouble(row, 0)
   val avgrsrq = toDouble(row, 1)
   val avgpucch = toDouble(row, 2)
   val avgsinr = toDouble(row, 3)
   val  countrsrp = toLong(row, 4)
   val countrsrq = toLong(row, 5)



   List(LteRaReport(jobId, processType, null, null, "Signal strength", 0.0, 0.0, 0, 0.0, 0.0, 0.0, avgrsrp, null, "Report", 0, 0),
     LteRaReport(jobId, processType, null, null, "Signal quality", 0.0, 0.0, 0, 0.0, 0.0, 0.0, avgrsrq, null, "Report", 0, 0),
     LteRaReport(jobId, processType, null, null, "Uplink coverage", 0.0, 0.0, 0, 0.0, 0.0, 0.0, avgpucch, null, "Report", 0, 0),
     LteRaReport(jobId, processType, null, null, "SINR", 0.0, 0.0, 0, 0.0, 0.0, 0.0, avgsinr, null, "Report", 0, 0))
})

  // val lteRaReportData=aggReporttData.collect()
   report1.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))

  val (sq,retValRSRQ)=getreportThreshold(commonConfigParams)

  val aggReporttDataPer = spark.sql("select count(ltePccRsrp) countTotal from report where  " +
   " (ltePccRsrp!=" + Constants.DEFAULT_KPI_DOUBLE_VAL + " or ltePccRsrq!=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ") and " +
   "(ltePccRsrp > " + -110 + " and ltePccRsrq > " + -13 + ") ")

   val countRs=aggReporttData.select("countrsrp").rdd.collect()
   val countRq=aggReporttData.select("countrsrq").rdd.collect()

   val total=countRs(0).getLong(0) + countRq(0).getLong(0)

  val reportRdd1 = aggReporttDataPer.flatMap((row) => {
   val reportCount = toLong(row, 0)
   val per = if (reportCount == 0.0) 0.0 else (reportCount * 100.0) / total
  List(LteRaReport(jobId, processType, null, null, "Downlink coverage", 0.0, 0.0, 0, 0.0, 0.0, 0.0, per, null, "Report", 0, 0))
  })
  

reportRdd1.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))

true
}
   false

}

  
  def toLong(row: Row,index: Int): Long ={
    if(row.get(index)!=null) row.getLong(index) else 0L
  }
  def toDouble(row: Row,index: Int): Double ={
    if(row.get(index)!=null) row.getDouble(index) else 0.0
  }
  def getreportThreshold(commpnConfigParams: CommonConfigParameters) = {
    var connection: Connection = null
    var pst: PreparedStatement = null
    var retValRSRP: String = null
    var retValRSRQ: String = null

    val query: String = "select distinct kpi_Name,thres_value from report_thresold r"
    try {
      connection = DriverManager.getConnection(commpnConfigParams.POSTGRES_CONN_URL)
      pst = connection.prepareStatement(query)
      val rs = pst.executeQuery()
      while (rs.next()) {
        retValRSRP = rs.getString(0)
        retValRSRQ = rs.getString(1)

      }

    } catch {
      case e: Throwable =>

    } finally {

      pst.close
      connection.close
    }
    (retValRSRP, retValRSRQ)
  }
  def getConnectionProperties(commonConfigParams:CommonConfigParameters)={
    val connectionProperties: Properties=new Properties()
    connectionProperties.put("user", commonConfigParams.POSTGRES_DB_USER)
    connectionProperties.put("password",commonConfigParams.POSTGRES_DB_PWD)
    connectionProperties.put("driver", commonConfigParams.POSTGRES_DRIVER)
    connectionProperties
  }
 

}

