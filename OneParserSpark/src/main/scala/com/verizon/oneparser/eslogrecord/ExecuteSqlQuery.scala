package com.verizon.oneparser.eslogrecord

import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.{CommonDaoImpl, Constants}
import com.verizon.oneparser.common.Constants.{
  LocalTransportAddress,
  PciInfo,
  PciRequestInfo
}
import com.verizon.oneparser.ProcessDMATRecords._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Timestamp

import com.verizon.oneparser.config.CommonConfigParameters

import scala.collection.immutable.{List, Seq}
import scala.collection.mutable.WrappedArray

object ExecuteSqlQuery extends Serializable with LazyLogging {

  def getJoinedTable(
      fileNamesStr: String,
      datesStr: String,
      testIds: String,
      spark: SparkSession,
      logsTblDs: Dataset[Row],
      startTimeRead: Long,
      fileNamesList: List[String],
      reportFlag: Boolean,
      commonConfigParams: CommonConfigParameters): LogRecordJoinedDF = {

    var kpi0xB193ExistsQuery: Boolean = false
    var kpiLteNormalSrvQuery: Boolean = false
    var kpiWcdmaNormalSrvQuery: Boolean = false
    var kpiGsmNormalSrvQuery: Boolean = false
    var kpiCdmaNormalSrvQuery: Boolean = false
    var kpiNoSrvQuery: Boolean = false
    var kpiDmatIPDSSrvQuery: Boolean = false
    var kpiPdcpRlcTputDFQuery: Boolean = false
    var kpiDmatAsnQuery: Boolean = false
    var kpiDmatIpQuery: Boolean = false
    var kpiQualComm5GExistsQuery: Boolean = false
    var kpiRtpDslFieldExistsQuery: Boolean = false
    var kpiDmatRtpGapQuery: Boolean = false
    var kpiNrDlScg5MsTputQuery: Boolean = false
    var kpiNrDlScg1SecTputQuery: Boolean = false
    var kpi5gDlDataBytesQuery: Boolean = false
    var kpiDmatOperatorQuery: Boolean = false
    var kpi4gDlTputQuery: Boolean = false
    var kpiVoLTEcallQuery: Boolean = false
    var kpiGeoDistToCellsQuery: Boolean = false
    var gNbidGeoDistToCellsQuery:Boolean = false
    var kpiisinbuilding: Boolean = false
    var mod4g5gFiles: List[String] = null
    var shelbyFiles: List[String] = null

    var kpiisRachExists:Boolean = false
    var kpiisRlcUlInstTputExists:Boolean = false
    var kpiPdcpUlInstTputExists:Boolean = false
    var kpiRlcInstTputExists:Boolean = false
    var kpiPdcpDlExists:Boolean = false
    var kpiDmatIpExists: Boolean = false
    var kpiRlcDLTputExists:Boolean = false
    var kpiNrDlScg5MsTputExists: Boolean = false
    var kpiNrPhyULThroughputExists:Boolean = false
    var kpi5gDlDataBytesExists: Boolean = false

    logger.info("Get the filenames: " + fileNamesStr)
    val dmatQualCommQuery: String = if (!reportFlag) {
      "select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + QualCommQuery
        .getQCommQuery(false) +
        "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1 "
    } else {
      "select * from (select fileName,dmTimeStamp, " + QualCommQuery
        .getQCommQuery(true) +
        "from (select * " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) "
    }

    val dmatQualCommQueryOperator = spark.sql(
      "select * from (select fileName,dmTimeStamp,dateWithoutMillis, " +
        "last_value(mcc, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mcc, " +
        "last_value(mnc, true) over(partition by filename,dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mnc, " +
        "last_value(lteCellID, true) over(partition by filename,dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteCellID, " +
        "last_value(lteeNBId, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteeNBId, " +
        "last_value(pUSCHTxPower, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower, " +
        "row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
        "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testid in (" + testIds + "))) where rn = 1")

    //logger.info("Persisting both dmatQualCommWithLoc and qualCommDf>>>>>")
    var dmatQualComm1 = spark.sql(dmatQualCommQuery)
    //dmatQualComm1 = dmatQualComm1.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info("dmatQualComm1.explain>>>>>>>>>>>>>>")
    //dmatQualComm1.explain()

    dmatQualComm1.createOrReplaceTempView("dmatQualComm1")

    val dmatQualComm2 = if (!reportFlag) {
      spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + QualComm2Query
        .getQComm2Query(false) +
        " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and (inbldUserMark != 'true' or inbldUserMark != 'false'))) where rn = 1")
    } else {
      spark.sql("select * from (select fileName,dmTimeStamp, " + QualComm2Query
        .getQComm2Query(true) +
        " from (select * " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and (inbldUserMark != 'true' or inbldUserMark != 'false'))) ")
    }
    val dmatAsn = if (!reportFlag) {
      spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + AsnQuery
        .getAsnQuery(false) +
        " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        " from dmat_logs." + commonConfigParams.TBL_ASN_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1")
    } else {
      spark.sql("select * from (select fileName,dmTimeStamp, " + AsnQuery
        .getAsnQuery(true) +
        " from (select * " +
        " from dmat_logs." + commonConfigParams.TBL_ASN_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ")))")
    }

    val dmatNas = if (!reportFlag) {
      spark.sql(
        "select * from (select fileName,dmTimeStamp, dateWithoutMillis, " + NasQuery
          .getNasQuery(false) +
          " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          " from dmat_logs." + commonConfigParams.TBL_NAS_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1")
    } else {
      spark.sql("select * from (select fileName,dmTimeStamp,  " + NasQuery
        .getNasQuery(true) +
        " from (select * " +
        " from dmat_logs." + commonConfigParams.TBL_NAS_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) ")
    }
    val dmatB192 = if (!reportFlag) {
      spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis," + B192Query.query + "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          " from dmat_logs." + commonConfigParams.TBL_B192_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")
    } else {
      spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp," + B192Query.query + "from (select * " +
          " from dmat_logs." + commonConfigParams.TBL_B192_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dmTimeStamp")
    }
    import spark.implicits._

    val mergeList1 = udf { (strings: WrappedArray[WrappedArray[Integer]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    val mergeList2 = udf { (strings: WrappedArray[WrappedArray[Double]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    val mergeList3 = udf { (strings: WrappedArray[WrappedArray[String]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    // val mergeList4 = udf { (strings: WrappedArray[String]) => strings.map(x => x.mkString(",")).mkString(",") }
    val mergeList4 = udf { (strings: WrappedArray[String]) =>
      strings.mkString(",")
    }

    val flatten = udf((xs: WrappedArray[WrappedArray[WrappedArray[Int]]]) =>
      xs.flatten.flatten.distinct.mkString(","))

    var dmatB192_NeighboringPciDist: DataFrame = null
    val dmatB192_merged = dmatB192
      .withColumn("EARFCN_collected", mergeList1($"collected_earfcn"))
      .withColumn("PCI_collected", mergeList1($"collected_pci"))
      .withColumn("Rsrp_collected", mergeList2($"collected_rsrp"))
      .withColumn("RsrpRx0_collected", mergeList2($"collected_rsrp0"))
      .withColumn("RsrpRx1_collected", mergeList2($"collected_rsrp1"))
      .withColumn("Rsrq_collected", mergeList2($"collected_rsrq"))
      .withColumn("RsrqRx0_collected", mergeList2($"collected_rsrq0"))
      .withColumn("RsrqRx1_collected", mergeList2($"collected_rsrq1"))
      .withColumn("Rssi_collected", mergeList2($"collected_rssi"))
      .withColumn("RssiRx0_collected", mergeList2($"collected_rssi0"))
      .withColumn("RssiRx1_collected", mergeList2($"collected_rssi1"))

    dmatB192_NeighboringPciDist = dmatB192_merged
      .select($"fileName", $"collected_pci")
      .groupBy("fileName")
      .agg(flatten(collect_list($"collected_pci")).alias("NeighboringPCI"))

    val dmatB193 = if (!reportFlag) {
      spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis, " + B193Query.query + " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          "from dmat_logs." + commonConfigParams.TBL_B193_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")
    } else {
      spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp, " + B193Query.query + " from (select * " +
          " from dmat_logs." + commonConfigParams.TBL_B193_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dmTimeStamp")
    }
    val dmatB193_merged = dmatB193
      .withColumn("EARFCN", mergeList1($"collectedB193_earfcn"))
      .withColumn("PhysicalCellID_B193", mergeList1($"collectedB193_pci"))
      .withColumn("ServingCellIndex_B193",
                  mergeList3($"collectedB193_servingCell"))
      .withColumn("InstMeasuredRSRP", mergeList2($"collectedB193_rsrp"))
      .withColumn("InstRSRPRx_0", mergeList2($"collectedB193_rsrp0"))
      .withColumn("InstRSRPRx_1", mergeList2($"collectedB193_rsrp1"))
      .withColumn("InstRSRPRx_2", mergeList2($"collectedB193_rsrp2"))
      .withColumn("InstRSRPRx_3", mergeList2($"collectedB193_rsrp3"))
      .withColumn("InstRSRQ", mergeList2($"collectedB193_rsrq"))
      .withColumn("InstRSRQRx_0", mergeList2($"collectedB193_rsrq0"))
      .withColumn("InstRSRQRx_1", mergeList2($"collectedB193_rsrq1"))
      .withColumn("InstRSRQRx_2", mergeList2($"collectedB193_rsrq2"))
      .withColumn("InstRSRQRx_3", mergeList2($"collectedB193_rsrq3"))
      .withColumn("InstRSSI", mergeList2($"collectedB193_rssi"))
      .withColumn("InstRSSIRx_0", mergeList2($"collectedB193_rssi0"))
      .withColumn("InstRSSIRx_1", mergeList2($"collectedB193_rssi1"))
      .withColumn("InstRSSIRx_2", mergeList2($"collectedB193_rssi2"))
      .withColumn("InstRSSIRx_3", mergeList2($"collectedB193_rssi3"))
      .withColumn("FTLSNRRx_0", mergeList2($"collectedB193_snir0"))
      .withColumn("FTLSNRRx_1", mergeList2($"collectedB193_snir1"))
      .withColumn("FTLSNRRx_2", mergeList2($"collectedB193_snir2"))
      .withColumn("FTLSNRRx_3", mergeList2($"collectedB193_snir3"))
      .withColumn("validRx", mergeList3($"collectedB193_validRx"))
      .withColumn("horxd_mode", mergeList1($"collectedB193_horxd_mode"))

    var dmatB193_ServingPciDist: DataFrame = null

    dmatB193_ServingPciDist = dmatB193_merged
      .select($"fileName", $"collectedB193_pci")
      .groupBy("fileName")
      .agg(flatten(collect_list($"collectedB193_pci")).alias("ServingPCI"))

    val lc0x184EDf = spark.sql(
      "select count(*) as totalCount from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and hexlogcode='0x184E'")

    var lteNormalSvcQuery = ""
    var wcdmaNormalSvcQuery = ""
    var gsmNormalSvcQuery = ""
    var cdmaNormalSvcQuery = ""
    var noSvcQuery = ""

    if (lc0x184EDf.head().getLong(0) > 0) {
      lteNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as lteNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 9 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as lteNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 9 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      wcdmaNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as wcdmaNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 5 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as wcdmaNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 5 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      gsmNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as gsmNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 3 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as gsmNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 3 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      cdmaNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as cdmaNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 2 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as cdmaNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 2 and stk_SRV_STATUS0x184E = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      noSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as noSrvCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 0 and stk_SRV_STATUS0x184E = 0 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as noSrvCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE0_0x184E = 0 and stk_SRV_STATUS0x184E = 0 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
    } else {
      lteNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as lteNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 9 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as lteNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 9 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      wcdmaNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as wcdmaNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 5 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as wcdmaNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 5 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      gsmNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as gsmNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 3 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as gsmNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 3 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      cdmaNormalSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as cdmaNormalCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 2 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as cdmaNormalCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 2 and stk_SRV_STATUS = 2 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
      noSvcQuery = if (!reportFlag) {
        "select fileName, dateWithoutMillis, max(rn) as noSrvCount " + "from (select *, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 0 and stk_SRV_STATUS = 0 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dateWithoutMillis"
      } else {
        "select fileName, dmTimeStamp, max(rn) as noSrvCount " + "from (select *, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn " +
          "from (select * " + "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " " +
          "where insertedDate in (" + datesStr + ") and stk_SYS_MODE = 0 and stk_SRV_STATUS = 0 and filename in (" +
          fileNamesStr + ") and testId in (" + testIds + "))) group by fileName, dmTimeStamp"
      }
    }

    val lteNormalSrvCnt =
      if (!lteNormalSvcQuery.isEmpty) spark.sql(lteNormalSvcQuery) else null
    val wcdmaNormalSrvCnt =
      if (!wcdmaNormalSvcQuery.isEmpty) spark.sql(wcdmaNormalSvcQuery) else null
    val gsmNormalSrvCnt =
      if (!gsmNormalSvcQuery.isEmpty) spark.sql(gsmNormalSvcQuery) else null
    val cdmaNormalSrvCnt =
      if (!cdmaNormalSvcQuery.isEmpty) spark.sql(cdmaNormalSvcQuery) else null
    val noSrvCnt = if (!noSvcQuery.isEmpty) spark.sql(noSvcQuery) else null

    val inbuilding = if (!reportFlag) {
      spark.sql(
        "select fileName, dmTimeStamp, inbldImgId, inbldX, inbldY, inbldUserMark, inbldAccuracy, inbldAltitude, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and fileName in (" + fileNamesStr + ") and (inbldUserMark = 'true' or inbldUserMark = 'false')")
    } else {
      spark.sql(
        "select fileName, dmTimeStamp, inbldImgId, inbldX, inbldY, inbldUserMark, inbldAccuracy, inbldAltitude " +
          "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and fileName in (" + fileNamesStr + ") and (inbldUserMark = 'true' or inbldUserMark = 'false')")
    }

    val windInbuilding =
      Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)

    import org.apache.spark.sql.functions._
    val inbuildingAlt = if (!reportFlag) {
      inbuilding
        .join(
          inbuilding
            .groupBy("dateWithoutMillis")
            .agg($"dateWithoutMillis", max("inbldUserMark") as "inbldUserMark"),
          Seq("dateWithoutMillis", "inbldUserMark"))
        .withColumn("rn", row_number.over(windInbuilding))
    } else {
      inbuilding
        .join(inbuilding
                .groupBy("dmTimeStamp")
                .agg($"dmTimeStamp", max("inbldUserMark") as "inbldUserMark"),
              Seq("dmTimeStamp", "inbldUserMark"))
        .withColumn("rn", row_number.over(windInbuilding))
    }
    //val inbuildingAlt = inbuilding.join(inbuilding.groupBy("dateWithoutMillis").agg($"dateWithoutMillis", max("inbldUserMark") as "inbldUserMark"), Seq("dateWithoutMillis", "inbldUserMark")).withColumn("rn", row_number.over(windInbuilding))
    //"select b.dateWithoutMillis,a.timestamp, a.fileName, a.x, a.y, b.userMark from dfAlt1 a join (select dateWithoutMillis, max(userMark) as userMark from dfAlt1 group by dateWithoutMillis) b where a.dateWithoutMillis = b.dateWithoutMillis and a.userMark = b.userMark")

    val inbuildingDer = inbuildingAlt
      .filter($"inbldUserMark" === "true")
      .withColumn("rn1", $"rn")
      .withColumn("inbldX1", lead("inbldX", 1, 0).over(windInbuilding))
      .withColumn("inbldY1", lead("inbldY", 1, 0).over(windInbuilding))
      .withColumn("rn2", lead("rn", 1, 999999).over(windInbuilding))

    inbuildingAlt.createOrReplaceTempView("inbuildingAlt")
    inbuildingDer.createOrReplaceTempView("inbuildingDer")

    var finalInbuildingDS = if (!reportFlag) {
      spark.sql(
        "select a.fileName, a.dmTimeStamp, a.dateWithoutMillis, a.inbldImgId, a.inbldAccuracy, a.inbldAltitude, b.inbldX, b.inbldX1, b.inbldY, b.inbldY1, " +
          "a.inbldUserMark, b.rn1, b.rn2, a.rn, b.inbldX+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldX1 - b.inbldX)) as interpolated_x, " +
          "b.inbldY+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldY1 - b.inbldY)) as interpolated_y  from inbuildingAlt a join inbuildingDer b " +
          "on a.fileName = b.fileName and (a.rn >= b.rn and a.rn < b.rn2)")
    } else {
      spark.sql(
        "select a.fileName, a.dmTimeStamp,  a.inbldImgId, a.inbldAccuracy, a.inbldAltitude, b.inbldX, b.inbldX1, b.inbldY, b.inbldY1, " +
          "a.inbldUserMark, b.rn1, b.rn2, a.rn, b.inbldX+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldX1 - b.inbldX)) as interpolated_x, " +
          "b.inbldY+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldY1 - b.inbldY)) as interpolated_y  from inbuildingAlt a join inbuildingDer b " +
          "on a.fileName = b.fileName and (a.rn >= b.rn and a.rn < b.rn2)")
    }
    if (finalInbuildingDS.count() > 0) {
      logger.info("Inbuilding File(s) Found.. Fetching Image Height from DB...")
      val imgIds = finalInbuildingDS
        .filter($"inbldImgId" =!= "")
        .groupBy("inbldImgId")
        .agg(max("inbldImgId"))
        .select("inbldImgId")
        .collect()
        .map(_(0))
        .toList
      val inbldImgWithHeightDS = CommonDaoImpl.getInbuildingHeight(
        spark,
        imgIds.mkString(","),
        commonConfigParams)
      //Merging/Joining Inbld Height to the Payload
      finalInbuildingDS = finalInbuildingDS.join(inbldImgWithHeightDS,
                                                 Seq("inbldImgId"),
                                                 "left_outer")
      finalInbuildingDS = finalInbuildingDS.withColumn(
        "interpolated_y",
        $"inbldImgHeight" - $"interpolated_y")
    }

    val dmatIPDS = if (!reportFlag) {
      spark.sql(
        "select * from (select fileName, dmTimeStamp, dateWithoutMillis, " + IpQuery
          .getIpQuery(false) + "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          "from dmat_logs." + commonConfigParams.TBL_IP_NAME + " where (voLTEDropEvt=1 or voLTEAbortEvt=1 or voLTETriggerEvt=1 or voLTECallEndEvt=1 or voLTECallNormalRelEvt=1) and insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1")
    } else {
      spark.sql("select * from (select fileName, dmTimeStamp, " + IpQuery
        .getIpQuery(true) + "from (select * " +
        "from dmat_logs." + commonConfigParams.TBL_IP_NAME + " where (voLTEDropEvt=1 or voLTEAbortEvt=1 or voLTETriggerEvt=1 or voLTECallEndEvt=1 or voLTECallNormalRelEvt=1) and insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1")
    }
    val dmatIpQuery = if (!reportFlag) {
      spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp asc) as rn  " +
        "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_IP_NAME + "  where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and (VoLTETriggerEvt=1 or VoLTECallEndEvt=1)))")
    } else {
      spark.sql(
        "select * from (select fileName,dmTimeStamp, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp asc) as rn  " +
          "from (select * " +
          "from dmat_logs." + commonConfigParams.TBL_IP_NAME + "  where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and (VoLTETriggerEvt=1 or VoLTECallEndEvt=1)))")
    }

    val dmatIp = if (!reportFlag) {
      spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp asc) as rn  " +
        "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_IP_NAME + "  where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + "))) where rn=1")
    } else {
      spark.sql(
        "select * from (select fileName,dmTimeStamp, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp asc) as rn  " +
          "from (select * " +
          "from dmat_logs." + commonConfigParams.TBL_IP_NAME + "  where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + "))) where rn=1")
    }
    val windIp = Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)

    var finalRtpGapDf: DataFrame = null
    if (dmatIpQuery.count() > 0) {

      val timeStampLeadDf =
        if (!reportFlag) {
          dmatIpQuery
            .filter($"rn" === 1)
            .withColumn(
              "callEndTimeStamp",
              when(lead("VoLTECallEndEvt", 1, null).over(windIp).isNotNull,
                   lead("dateWithoutMillis", 1, null).over(windIp))
                .otherwise(lit(null)))
        } else {
          dmatIpQuery
            .filter($"rn" === 1)
            .withColumn(
              "callEndTimeStamp",
              when(
                lead("VoLTECallEndEvt", 1, null).over(windIp).isNotNull,
                lead("dmTimeStamp", 1, null).over(windIp)).otherwise(lit(null)))
        }
      // val timeStampLeadDf = dmatIpQuery.filter($"rn"===1).withColumn("callEndTimeStamp", lead("dateWithoutMillis", 1, null).over(w))

      val minDf = if (!reportFlag) {
        timeStampLeadDf
          .select($"dateWithoutMillis" as "rtpTimeStamp",
                  $"callEndTimeStamp",
                  $"fileName")
          .filter($"callEndTimeStamp".isNotNull)
          .groupBy($"fileName", $"rtpTimeStamp")
          .agg(min($"callEndTimeStamp"))
      } else {
        timeStampLeadDf
          .select($"dmTimeStamp" as "rtpTimeStamp",
                  $"callEndTimeStamp",
                  $"fileName")
          .filter($"callEndTimeStamp".isNotNull)
          .groupBy($"fileName", $"rtpTimeStamp")
          .agg(min($"callEndTimeStamp"))
      }
      val rtpTimeStampRange =
        minDf.select($"rtpTimeStamp", $"min(callEndTimeStamp)")

      val FinalRtpLossDf = if (!reportFlag) {
        dmatIp.join(
          rtpTimeStampRange,
          ($"dateWithoutMillis" >= $"rtpTimeStamp" && $"dateWithoutMillis" <= $"min(callEndTimeStamp)"))
      } else {
        dmatIp.join(
          rtpTimeStampRange,
          ($"dmTimeStamp" >= $"rtpTimeStamp" && $"dmTimeStamp" <= $"min(callEndTimeStamp)"))
      }
      val rtpGapJoinDf = if (!reportFlag) {
        FinalRtpLossDf
          .withColumn(
            "rtpGap",
            when($"rtpDlSn".isNotNull, lit("false")).otherwise(lit("true")))
          .orderBy($"dateWithoutMillis")
      } else {
        FinalRtpLossDf
          .withColumn(
            "rtpGap",
            when($"rtpDlSn".isNotNull, lit("false")).otherwise(lit("true")))
          .orderBy($"dmTimeStamp")
      }
      finalRtpGapDf = if (!reportFlag) {
        rtpGapJoinDf.select($"dmTimeStamp",
                            $"fileName",
                            $"dateWithoutMillis",
                            $"VoLTETriggerEvt" as "VoLTETriggerEvtRtpGap",
                            $"VoLTECallEndEvt" as "VoLTECallEndEvtRtpGap",
                            $"rtpGap")
      } else {
        rtpGapJoinDf.select($"dmTimeStamp",
                            $"fileName",
                            $"VoLTETriggerEvt" as "VoLTETriggerEvtRtpGap",
                            $"VoLTECallEndEvt" as "VoLTECallEndEvtRtpGap",
                            $"rtpGap")
      }

    }

    def dateShiftBefore(myDate: java.sql.Timestamp): java.sql.Timestamp = {
      val offset = -3
      val cal = Calendar.getInstance
      cal.setTime(myDate)
      cal.add(Calendar.SECOND, offset)
      new java.sql.Timestamp(cal.getTime.getTime)
    }

    def dateShiftAfter(myDate: java.sql.Timestamp): java.sql.Timestamp = {
      val offset = 3
      val cal = Calendar.getInstance
      cal.setTime(myDate)
      cal.add(Calendar.SECOND, offset)
      new java.sql.Timestamp(cal.getTime.getTime)
    }

    val udfDateShiftBefore =
      udf[java.sql.Timestamp, java.sql.Timestamp](dateShiftBefore)
    val udfDateShiftAfter =
      udf[java.sql.Timestamp, java.sql.Timestamp](dateShiftAfter)

    var rtpPacketsLoss: DataFrame = null
    val dmatIpDf = if (!reportFlag) {
      spark.sql(
        "select dmTimeStamp, index, logCode, hexLogCode, version, rtpDirection, rtpDlSn, rtpRxTimeStamp, exceptionOccured, exceptionCause, fileName, insertedDate, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from dmat_logs." + commonConfigParams.TBL_IP_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and rtpdlsn is not null")
    } else {
      spark.sql(
        "select dmTimeStamp, index, logCode, hexLogCode, version, rtpDirection, rtpDlSn, rtpRxTimeStamp, exceptionOccured, exceptionCause, fileName, insertedDate from dmat_logs." + commonConfigParams.TBL_IP_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and rtpdlsn is not null")
    }
    if (dmatIpDf.count() > 0) {
      kpiRtpDslFieldExistsQuery = true
      //1. For each second find Min SN, Max SN, Total received packets count and, Missing packet count(DeltaCount)
      val rtpPacketCounts1SecDf = if (!reportFlag) {
        dmatIpDf
          .filter($"rtpdlsn".isNotNull)
          .groupBy("fileName", "dateWithoutMillis")
          .agg(
            $"dateWithoutMillis",
            min("rtpdlsn") as "rtpDlsnAggMin",
            max("rtpdlsn") as "rtpDlsnAggMax",
            count($"rtpdlsn") as "rtpDlsnAggCount",
            ((max("rtpdlsn") - min("rtpdlsn")) + 1) - count($"rtpdlsn") as "rtpdlsnCountAgg"
          )
          .orderBy($"dateWithoutMillis".asc)
      } else {
        dmatIpDf
          .filter($"rtpdlsn".isNotNull)
          .groupBy("fileName", "dmTimeStamp")
          .agg(
            $"dmTimeStamp",
            min("rtpdlsn") as "rtpDlsnAggMin",
            max("rtpdlsn") as "rtpDlsnAggMax",
            count($"rtpdlsn") as "rtpDlsnAggCount",
            ((max("rtpdlsn") - min("rtpdlsn")) + 1) - count($"rtpdlsn") as "rtpdlsnCountAgg"
          )
          .orderBy($"dmTimeStamp".asc)
      }
      val dmatIpDfOneSecAgg = if (!reportFlag) {
        dmatIpDf.join(rtpPacketCounts1SecDf,
                      Seq("fileName", "dateWithoutMillis"),
                      "full_outer")
      } else {
        dmatIpDf.join(rtpPacketCounts1SecDf,
                      Seq("fileName", "dmTimeStamp"),
                      "full_outer")
      }
      //2. Collect all the SN which is between Min SN and Max SN where 1SecDeltaSN > 0 from the SNs fpr the period of + or - 3 Seconds
      val rtpPacketCounts3SecDf = if (!reportFlag) {
        rtpPacketCounts1SecDf
          .filter($"rtpdlsnCountAgg" > 0)
          .select($"fileName", $"dateWithoutMillis", $"rtpDlsnAggCount")
          .groupBy($"fileName", $"dateWithoutMillis")
          .agg($"dateWithoutMillis")
          .join(
            dmatIpDfOneSecAgg.select(
              $"dateWithoutMillis" as "r_date",
              $"rtpdlsn" as "r_rtpdlsn",
              $"rtpDlsnAggMin" as "rtpDlsnAggMin",
              $"rtpDlsnAggMax" as "rtpDlsnAggMax",
              $"rtpDlsnAggCount" as "rtpDlsnAggCount"
            ),
            $"r_date" > udfDateShiftBefore($"dateWithoutMillis") and $"r_date" < udfDateShiftAfter(
              $"dateWithoutMillis") and $"r_rtpdlsn" >= ($"rtpDlsnAggMin") and $"r_rtpdlsn" <= ($"rtpDlsnAggMax")
          )
          .groupBy($"fileName", $"dateWithoutMillis")
          .agg(
            $"dateWithoutMillis",
            (max($"r_rtpdlsn") - min($"r_rtpdlsn") + 1 - count($"r_rtpdlsn")) as "rtpPacketLoss_3Seconds")
      } else {
        rtpPacketCounts1SecDf
          .filter($"rtpdlsnCountAgg" > 0)
          .select($"fileName", $"dmTimeStamp", $"rtpDlsnAggCount")
          .groupBy($"fileName", $"dmTimeStamp")
          .agg($"dmTimeStamp")
          .join(
            dmatIpDfOneSecAgg.select(
              $"dmTimeStamp" as "r_date",
              $"rtpdlsn" as "r_rtpdlsn",
              $"rtpDlsnAggMin" as "rtpDlsnAggMin",
              $"rtpDlsnAggMax" as "rtpDlsnAggMax",
              $"rtpDlsnAggCount" as "rtpDlsnAggCount"
            ),
            $"r_date" > udfDateShiftBefore($"dmTimeStamp") and $"r_date" < udfDateShiftAfter(
              $"dmTimeStamp") and $"r_rtpdlsn" >= ($"rtpDlsnAggMin") and $"r_rtpdlsn" <= ($"rtpDlsnAggMax")
          )
          .groupBy($"fileName", $"dmTimeStamp")
          .agg(
            $"dmTimeStamp",
            (max($"r_rtpdlsn") - min($"r_rtpdlsn") + 1 - count($"r_rtpdlsn")) as "rtpPacketLoss_3Seconds")
      }
      rtpPacketsLoss = if (!reportFlag) {
        rtpPacketCounts1SecDf.join(rtpPacketCounts3SecDf,
                                   Seq("fileName", "dateWithoutMillis"),
                                   "full_outer")
      } else {
        rtpPacketCounts1SecDf.join(rtpPacketCounts3SecDf,
                                   Seq("fileName", "dmTimeStamp"),
                                   "full_outer")
      }
    }

    //val joinedAllTablesDmatIp = dmatIpDf.join(dmatIPDS, Seq("fileName", "dmTimeStamp", "dateWithoutMillis"), "full_outer")

    val windowFirstLocationRecord =
      Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)
    val windowLastLocationRecord =
      Window.partitionBy($"fileName").orderBy($"dmTimeStamp".desc)
    val dmatQualComm1Df = if (!reportFlag) {
      dmatQualComm1
        .select($"fileName",
                $"dmTimeStamp",
                $"dateWithoutMillis",
                $"Longitude",
                $"Latitude",
                $"Longitude2",
                $"Latitude2")
        .filter(
          ($"Longitude".isNotNull && $"Latitude".isNotNull) || ($"Longitude2".isNotNull && $"Latitude2".isNotNull))
        .withColumn("rnFR", row_number.over(windowFirstLocationRecord))
        .withColumn("rnLR", row_number.over(windowLastLocationRecord))
    } else {
      dmatQualComm1
        .select($"fileName",
                $"dmTimeStamp",
                $"Longitude",
                $"Latitude",
                $"Longitude2",
                $"Latitude2")
        .filter(
          ($"Longitude".isNotNull && $"Latitude".isNotNull) || ($"Longitude2".isNotNull && $"Latitude2".isNotNull))
        .withColumn("rnFR", row_number.over(windowFirstLocationRecord))
        .withColumn("rnLR", row_number.over(windowLastLocationRecord))
    }

    val firstLastLocationRecordDf = if (!reportFlag) {
      dmatQualComm1Df
        .select($"fileName",
                $"dmTimeStamp",
                $"dateWithoutMillis",
                $"rnFR",
                $"rnLR")
        .where($"rnFR" === 1 || $"rnLR" === 1)
    } else {
      dmatQualComm1Df
        .select($"fileName", $"dmTimeStamp", $"rnFR", $"rnLR")
        .where($"rnFR" === 1 || $"rnLR" === 1)
    }

    val pdcpRlcTputDFTemp = if (!reportFlag) {
      spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + QualCommQuery
        .getQueryPdcpRlcTput(false) +
        "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + "))) where rn = 1")
    } else {
      spark.sql("select * from (select fileName,dmTimeStamp, " + QualCommQuery
        .getQueryPdcpRlcTput(true) +
        " from (select * " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ")))")
    }
    val windPdcpRlc =
      Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)
    val pdcpRlcTputDF = pdcpRlcTputDFTemp
      .withColumn("prePdcpDlTotalBytes",
                  lag("pdcpDlTotalBytes", 1).over(windPdcpRlc))
      .withColumn("prePdcpDlTimeStamp",
                  lag("pdcpDlTimeStamp", 1).over(windPdcpRlc))
      .withColumn("prePdcpUlTotalBytes",
                  lag("pdcpUlTotalBytes", 1).over(windPdcpRlc))
      .withColumn("prePdcpUlTimeStamp",
                  lag("pdcpUlTimeStamp", 1).over(windPdcpRlc))
      .withColumn("prerlcDlTotalBytes",
                  lag("rlcDlTotalBytes", 1).over(windPdcpRlc))
      .withColumn("prerlcDlTimeStamp",
                  lag("rlcDlTimeStamp", 1).over(windPdcpRlc))
      .withColumn("prerlcUlTotalBytes",
                  lag("rlcUlTotalBytes", 1).over(windPdcpRlc))
      .withColumn("prerlcUlTimeStamp",
                  lag("rlcUlTimeStamp", 1).over(windPdcpRlc))

    //logger.info("pdcpDlTputDF output => " + pdcpDlTputDF.show(200))

    val Qualcomm5GDf = if (!reportFlag) {
      spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis, " +
          Nr5gQuery.query + "from (select *, split(dmTimeStamp, '[\\.]')[0] " +
          "as dateWithoutMillis from dmat_logs." + commonConfigParams.TBL_QCOMM5G_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")
    } else {
      spark.sql("select fileName,max(dmTimeStamp) as dmTimeStamp, " +
        Nr5gQuery.query + " from (select * " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM5G_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") sort by fileName, dmTimeStamp desc) group by fileName, dmTimeStamp")
    }
    val Qualcomm5GDf_merged = Qualcomm5GDf
      .withColumn("Serving_BRSRP", mergeList3($"collectedB975_nrsbrsrp"))
      .withColumn("Serving_BRSRQ", mergeList3($"collectedB975_nrsbrsrq"))
      .withColumn("Serving_BSNR", mergeList4($"collectedB975_nrsbsnr"))
      .withColumn("Serving_nrbeamcount",
                  mergeList4($"collectedB975_nrbeamcount"))
      .withColumn("Serving_ccindex", mergeList4($"collectedB975_ccindex"))
      .withColumn("Serving_nrpci", mergeList4($"collectedB975_nrpci"))

    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      startTime.getTime() - endTime.getTime()
    })
    val win = Window
      .partitionBy($"fileName", $"dateWithoutMillis")
      .orderBy($"dmTimeStamp".asc)
    val win5MsDlTPUT =
      Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)

    var finalDlTput5MsDiffDf: DataFrame = null
    val Qualcomm5GTPUT0xB888Df = spark.sql(
      "select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM5G_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and hexlogcode = '0xB888'")
    // Determining 5 Milliseconds Dl throughput KPIs
    if (Qualcomm5GTPUT0xB888Df.count() > 0) {
      val timeStampTbsizeDiff = Qualcomm5GTPUT0xB888Df
        .select($"fileName",
                $"dmTimeStamp",
                $"dateWithoutMillis",
                $"TB_Size_0xB888")
        .withColumn("dmTimeStampPrevious",
                    lag("dmTimeStamp", 1, null).over(win5MsDlTPUT))
        .withColumn("tbSizePrev",
                    lag("TB_Size_0xB888", 1, null).over(win5MsDlTPUT))
      val diff_secs_col = timeStampTbsizeDiff
        .filter($"dmTimeStampPrevious".isNotNull)
        .select($"fileName",
                $"dmTimeStamp",
                $"dateWithoutMillis",
                $"dmTimeStampPrevious",
                $"TB_Size_0xB888",
                $"tbSizePrev")
        .withColumn("timeDff",
                    timestamp_diff(($"dmTimeStamp"), ($"dmTimeStampPrevious")))
        .withColumn("tPUT5Seconds",
                    ($"TB_Size_0xB888" - $"tbSizePrev") * 1600 / Constants.Mbps)
      val DlTput5MsDiffDf = diff_secs_col
        .filter($"timeDff" === 5)
        .drop("TB_Size_0xB888", "tbSizePrevtimeDff")
      finalDlTput5MsDiffDf = DlTput5MsDiffDf
        .withColumn("rn", row_number().over(win))
        .filter($"rn" === 1)
        .drop($"rn")
    }

    var finalFirstLastDf: DataFrame = null
    val Qualcomm5GTPUT0xB887Df = spark.sql(
      "select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_QCOMM5G_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and hexlogcode = '0xB887'")

    //Estimating DL throughput KPIs per 1 second difference
    if (Qualcomm5GTPUT0xB887Df.count() > 0) {
      val tbSizeAggdf = Qualcomm5GTPUT0xB887Df
        .select("*")
        .groupBy("fileName", "dateWithoutMillis")
        .agg(
          sum("pdschSum_0xB887").as("pdschSum0xB887"),
          sum("pdschSumPcc_0xB887").as("pdschSumPcc0xB887"),
          sum("pdschSumScc1_0xB887").as("pdschSumScc10xB887"),
          sum("pdschSumScc2_0xB887").as("pdschSumScc20xB887"),
          sum("pdschSumScc3_0xB887").as("pdschSumScc30xB887"),
          sum("pdschSumScc4_0xB887").as("pdschSumScc40xB887"),
          sum("pdschSumScc5_0xB887").as("pdschSumScc50xB887"),
          sum("pdschSumScc6_0xB887").as("pdschSumScc60xB887"),
          sum("pdschSumScc7_0xB887").as("pdschSumScc70xB887")
        )

      val firstLastSfnWin = Window
        .partitionBy($"fileName", $"dateWithoutMillis")
        .orderBy($"dmTimeStamp".asc)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

      val firstLastDf = Qualcomm5GTPUT0xB887Df
        .withColumn("firstSfn15kHz_0xB887",
                    first("firstSfn_15kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfnSlot15kHz_0xB887",
          first("firstSfnSlot_15kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("firstSfn30kHz_0xB887",
                    first("firstSfn_30kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("firstSfnSlot30kHz_0xB887",
                    first("firstSfn_30kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("firstSfn60kHz_0xB887",
                    first("firstSfn_60kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfnSlot60kHz_0xB887",
          first("firstSfnSlot_60kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfn120kHz_0xB887",
          first("firstSfn_120kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfnSlot120kHz_0xB887",
          first("firstSfnSlot_120kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfn240kHz_0xB887",
          first("firstSfn_240kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "firstSfnSlot240kHz_0xB887",
          first("firstSfnSlot_240kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfn15kHz_0xB887",
                    last("lastSfn_15kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "lastSfnSlot15kHz_0xB887",
          last("lastSfnSlot_15kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfn30kHz_0xB887",
                    last("lastSfn_30kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "lastSfnSlot30kHz_0xB887",
          last("lastSfnSlot_30kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfn60kHz_0xB887",
                    last("lastSfn_60kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfnSlot60kHz_0xB887",
                    last("lastSfn_60kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfn120kHz_0xB887",
                    last("lastSfn_120kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "lastSfnSlot120kHz_0xB887",
          last("lastSfnSlot_120kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("lastSfn240kHz_0xB887",
                    last("lastSfn_240kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn(
          "lastSfnSlot240kHz_0xB887",
          last("lastSfnSlot_240kHz_0xB887", true) over (firstLastSfnWin))
        .withColumn("rn", row_number().over(win))
        .filter($"rn" === 1)
        .drop($"rn")

      finalFirstLastDf = tbSizeAggdf.join(firstLastDf,
                                          Seq("fileName", "dateWithoutMillis"),
                                          "full_outer")

      //      logger.info("tbSizeAggdf data frame ==>> " + tbSizeAggdf.show(500))
      //      logger.info("finalFirstLastDf data frame ==>> " + finalFirstLastDf.show(500))

    }
    var finalFirstLast4gDf: DataFrame = null

    /* val Qualcomm4GTPUT0xB173Df = spark.sql("select fileName,dmTimeStamp, pcellTBSize_0xB173, scc1TBSize_0xB173, scc2TBSize_0xB173, scc3TBSize_0xB173, scc4TBSize_0xB173, scc5TBSize_0xB173, scc6TBSize_0xB173, scc7TBSize_0xB173, totalTBSize_0xB173,firstFN_0xB173,firstSFN_0xB173,lastFN_0xB173,lastSFN_0xB173, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
       "from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and hexlogcode = '0xB173'")
     */
    /*finalFirstLast4gDf = if(!reportFlag) {
      spark.sql("select * from (select hexlogcode, fileName,dateWithoutMillis, " + QualCommQuery.getQuery4gTput(false) +
        " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ")  and testId in (" + testIds + ")  and hexlogcode = '0xB173'))")
    } else {
      spark.sql("select * from (select hexlogcode, fileName,dateWithoutMillis, " + QualCommQuery.getQuery4gTput(false) +
        " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        " from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ")  and testId in (" + testIds + ")  and hexlogcode = '0xB173'))")
    }*/

    /*    if(Qualcomm4GTPUT0xB173Df.count()>0) {
          val tbSizeAgg4gDf = Qualcomm4GTPUT0xB173Df.select("*").groupBy("fileName","dateWithoutMillis")
            .agg(sum("pcellTBSize_0xB173").as("pcellTBSizeSum0xB173"), sum("scc1TBSize_0xB173").as("scc1TBSizeSum0xB173"),
              sum("scc2TBSize_0xB173").as("scc2TBSizeSum0xB173"), sum("scc3TBSize_0xB173").as("scc3TBSizeSum0xB173"),
              sum("scc4TBSize_0xB173").as("scc4TBSizeSum0xB173"), sum("scc5TBSize_0xB173").as("scc5TBSizeSum0xB173"),
              sum("scc6TBSize_0xB173").as("scc6TBSizeSum0xB173"), sum("scc7TBSize_0xB173").as("scc7TBSizeSum0xB173"),
              sum("totalTBSize_0xB173").as("totalTBSizeSum0xB173"))

          val firstLastSfnWin = Window.partitionBy($"fileName",$"dateWithoutMillis").orderBy($"dmTimeStamp".asc).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

          val firstLast4gDf = Qualcomm4GTPUT0xB173Df.select($"fileName", $"dmTimeStamp",$"dateWithoutMillis",$"firstFN_0xB173",$"firstSFN_0xB173",$"lastFN_0xB173",$"lastSFN_0xB173").withColumn("firstFn_0xB173", first("firstFN_0xB173", true) over (firstLastSfnWin))
            .withColumn("firstSfn_0xB173", first("firstSFN_0xB173", true) over (firstLastSfnWin))
            .withColumn("lastFn_0xB173", last("lastFN_0xB173", true) over (firstLastSfnWin))
            .withColumn("lastSfn_0xB173", last("lastSFN_0xB173", true) over (firstLastSfnWin))

          finalFirstLast4gDf = tbSizeAgg4gDf.join(firstLast4gDf, Seq("fileName", "dateWithoutMillis"), "full_outer")

        }*/

    val dmatAsndf = spark.sql(
      "select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
        "from dmat_logs." + commonConfigParams.TBL_ASN_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and hexlogcode = '0xB0C0'")

    var Prv_dmTimeStamp: String = null
    val forwardFill = udf(
      (dateWithoutMillis: String, lteRrcCon: Integer, rccConn: String) => {
        if (rccConn != null) {
          Prv_dmTimeStamp = rccConn
          rccConn
        } else {
          Prv_dmTimeStamp = dateWithoutMillis
          dateWithoutMillis
        }
      })

    var final5gDLSessionDf: DataFrame = null
    if (dmatAsndf.count() > 0 && Qualcomm5GTPUT0xB887Df.count() > 0) {
      val dmatAsnRrcCon = dmatAsndf
        .filter($"lteRrcConRel".isNotNull || $"lteRrcConSetupCmp".isNotNull)
        .select($"fileName",
                $"dmTimeStamp",
                $"dateWithoutMillis",
                $"lteRrcConSetupCmp",
                $"lteRrcConRel")
      val dmatAsnRrcCon1 = dmatAsnRrcCon.withColumn(
        "rccConnectionRelease",
        when(lag("lteRrcConSetupCmp", 1, null).over(windIp).isNotNull,
             lag("dateWithoutMillis", 1, null).over(windIp)).otherwise(null))
      val dmatAsnRrcCon2 = dmatAsnRrcCon1
        .withColumn("rccConnStart",
                    forwardFill($"dateWithoutMillis",
                                $"lteRrcConRel",
                                $"rccConnectionRelease"))
        .drop($"dmTimeStamp")
      val dmatAsnRrcCon3 = Qualcomm5GTPUT0xB887Df
        .groupBy($"fileName", $"dateWithoutMillis")
        .agg(sum("pdschSum_0xB887").as("pdschSize"))
      val dmatAsnRrcCon4 = dmatAsnRrcCon3.join(
        dmatAsnRrcCon2,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")
      val w = Window
        .partitionBy($"fileName")
        .orderBy($"dateWithoutMillis".asc)
        .rowsBetween(Window.unboundedPreceding, -1)
      val dmatAsnRrcCon5 = dmatAsnRrcCon4
        .select($"fileName",
                $"dateWithoutMillis",
                $"rccConnStart",
                $"pdschSize")
        .withColumn(
          "RRC_startTime",
          coalesce($"rccConnStart", last($"rccConnStart", true).over(w)))
      dmatAsnRrcCon5.createOrReplaceTempView("dmatAsnRrcCon5")
      final5gDLSessionDf = spark
        .sql(
          "select *, sum(pdschSize) over (partition by RRC_startTime order by dateWithoutMillis asc rows unbounded preceding) as sessionDlDataBytes from dmatAsnRrcCon5")
        .drop("pdschSize", "rccConnStart", "RRC_startTime")
    }

    var finalOperatorKpiDf: DataFrame = null

    if (dmatQualCommQueryOperator.count() > 0) {
      val winOperator = Window
        .partitionBy($"fileName")
        .orderBy($"dateWithoutMillis".asc)
        .rowsBetween(Window.unboundedPreceding, -1)
      finalOperatorKpiDf = dmatQualCommQueryOperator
        .select($"fileName",
                $"dateWithoutMillis",
                $"mcc",
                $"mnc",
                $"lteCellID",
                $"lteeNBId",
                $"pUSCHTxPower")
        .withColumn("MCC",
                    coalesce($"mcc", last($"mcc", true).over(winOperator)))
        .withColumn("MNC",
                    coalesce($"mnc", last($"mnc", true).over(winOperator)))
        .withColumn("LteCellID",
                    coalesce($"lteCellID",
                             last($"lteCellID", true).over(winOperator)))
        .withColumn("LteeNBId",
                    coalesce($"lteeNBId",
                             last($"lteeNBId", true).over(winOperator)))
        .withColumn("PuschTxPower",
                    coalesce($"pUSCHTxPower",
                             last($"pUSCHTxPower", true).over(winOperator)))
        .orderBy($"dateWithoutMillis".asc)
    }
    logger.info(
      "Finshed reading file ==>> " + (System.currentTimeMillis - startTimeRead))

    val startTimeMerge = System.currentTimeMillis
    logger.info("START Joining tables ==>> " + startTimeMerge)

    if (Qualcomm5GDf_merged.count() > 0) {
      kpiQualComm5GExistsQuery = true
    }

    var joinedAllTablesTemp: DataFrame = null

    if (dmatQualComm1.count() > 0) {
      joinedAllTablesTemp = if (!reportFlag) {
        dmatQualComm1
          .join(dmatQualComm2,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(dmatNas, Seq("fileName", "dateWithoutMillis"), "full_outer")
          .join(dmatB192_merged,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(finalInbuildingDS,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(firstLastLocationRecordDf,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(Qualcomm5GDf_merged,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .drop("dmTimeStamp")
      } else {
        dmatQualComm1
          .join(dmatQualComm2, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(dmatNas, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(dmatB192_merged, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(finalInbuildingDS, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(firstLastLocationRecordDf,
                Seq("fileName", "dmTimeStamp"),
                "full_outer")
          .join(Qualcomm5GDf_merged,
                Seq("fileName", "dmTimeStamp"),
                "full_outer")
      }
    } else if (Qualcomm5GDf_merged.count() > 0) {
      kpiQualComm5GExistsQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        Qualcomm5GDf_merged
          .join(dmatQualComm1,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(dmatQualComm2,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(dmatNas, Seq("fileName", "dateWithoutMillis"), "full_outer")
          .join(dmatB192_merged,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(finalInbuildingDS,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .join(firstLastLocationRecordDf,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .drop("dmTimeStamp")
      } else {
        Qualcomm5GDf_merged
          .join(dmatQualComm1, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(dmatQualComm2, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(dmatNas, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(dmatB192_merged, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(finalInbuildingDS, Seq("fileName", "dmTimeStamp"), "full_outer")
          .join(firstLastLocationRecordDf,
                Seq("fileName", "dmTimeStamp"),
                "full_outer")
      }
    }

    if (dmatB193_merged.count() > 0) {
      kpi0xB193ExistsQuery = true
      println("get b193 count: " + dmatB193_merged.count())
      println("b193 table: " + dmatB193_merged.show(10))
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp
          .join(dmatB193_merged,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .drop("dmTimeStamp")
      } else {
        joinedAllTablesTemp.join(dmatB193_merged,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (gsmNormalSrvCnt.count() > 0) {
      kpiGsmNormalSrvQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(gsmNormalSrvCnt,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(gsmNormalSrvCnt,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (lteNormalSrvCnt.count() > 0) {
      kpiLteNormalSrvQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp
          .join(lteNormalSrvCnt,
                Seq("fileName", "dateWithoutMillis"),
                "full_outer")
          .drop("dmTimeStamp")
      } else {
        joinedAllTablesTemp.join(lteNormalSrvCnt,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }
    if (wcdmaNormalSrvCnt.count() > 0) {
      kpiWcdmaNormalSrvQuery = true
      joinedAllTablesTemp = joinedAllTablesTemp
        .join(wcdmaNormalSrvCnt,
              Seq("fileName", "dateWithoutMillis"),
              "full_outer")
        .drop("dmTimeStamp")
    }

    if (noSrvCnt.count() > 0) {
      kpiNoSrvQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(noSrvCnt,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(noSrvCnt,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (cdmaNormalSrvCnt.count() > 0) {
      kpiCdmaNormalSrvQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(cdmaNormalSrvCnt,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(cdmaNormalSrvCnt,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }
    if (dmatIPDS.count() > 0) {
      kpiDmatIPDSSrvQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(dmatIPDS,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(dmatIPDS,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (pdcpRlcTputDF.count() > 0) {
      kpiPdcpRlcTputDFQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(pdcpRlcTputDF,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(pdcpRlcTputDF,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (dmatAsn.count() > 0) {
      kpiDmatAsnQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(dmatAsn,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(dmatAsn,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    //    if (dmatIp.count() > 0) {
    //      kpiDmatIpQuery = true
    //      joinedAllTablesTemp = joinedAllTablesTemp.join(dmatIp, Seq("fileName", "dateWithoutMillis"), "full_outer")
    //    }

    if (dmatIpDf.count() > 0) {
      kpiDmatIpQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(rtpPacketsLoss,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(rtpPacketsLoss,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (dmatIpQuery.count() > 0) {

      kpiDmatRtpGapQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(finalRtpGapDf,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(finalRtpGapDf,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }
    // val joinedAllTablesTemp = lc0xB975Df_merged.drop("dmTimeStamp")

    if (fileNamesList.nonEmpty) {
      var fileList: String = ""
      fileList = getMOD4g5gFiles(fileNamesList)
      mod4g5gFiles = fileList.toString.split(";").map(_.trim).toList
      shelbyFiles = fileList.toString
        .replace("mod4g5g", "Shelby")
        .split(";")
        .map(_.trim)
        .toList
    }

    if (Qualcomm5GTPUT0xB888Df.count() > 0) {
      kpiNrDlScg5MsTputQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(finalDlTput5MsDiffDf,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(finalDlTput5MsDiffDf,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    if (Qualcomm5GTPUT0xB887Df.count() > 0) {
      kpiNrDlScg1SecTputQuery = true
      joinedAllTablesTemp = if (!reportFlag) {
        joinedAllTablesTemp.join(finalFirstLastDf,
                                 Seq("fileName", "dateWithoutMillis"),
                                 "full_outer")
      } else {
        joinedAllTablesTemp.join(finalFirstLastDf,
                                 Seq("fileName", "dmTimeStamp"),
                                 "full_outer")
      }
    }

    /*if(finalFirstLast4gDf != null) {
      kpi4gDlTputQuery = true
      joinedAllTablesTemp = joinedAllTablesTemp.join(finalFirstLast4gDf, Seq("fileName", "dateWithoutMillis"), "full_outer")
    }*/
    if (dmatAsndf.count() > 0 && Qualcomm5GTPUT0xB887Df.count() > 0) {
      kpi5gDlDataBytesQuery = true
      joinedAllTablesTemp = joinedAllTablesTemp.join(
        final5gDLSessionDf,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")
    }
    //kpiNrDlScg1SecTputQuery

    if (dmatQualCommQueryOperator.count() > 0) {
      kpiDmatOperatorQuery = true
      joinedAllTablesTemp = joinedAllTablesTemp.join(
        finalOperatorKpiDf,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")
    }

    if (joinedAllTablesTemp != null) {
      joinedAllTablesTemp.createOrReplaceTempView("joinedAllTablesTemp")
    }

    // Set previous latitude, longitude value for records with no values
    val joinedAllTablesFinalTemp = if (!reportFlag) {
      spark.sql(
        "select *, last_value(Latitude, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLatitude, " +
          "last_value(Longitude, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLongitude, " +
          "last_value(Latitude2, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLatitude2, " +
          "last_value(Longitude2, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLongitude2 " +
          "from joinedAllTablesTemp")
    } else {
      joinedAllTablesTemp
    }

    //Merging/Joining TestId to the Payload
    //By default json4s lib updates UTC time, as a temporary fix timestamp is reduced to 4 hrs DST for restoring original log captured date

    var geoBoundingDistDf: DataFrame = null
    var joinedAllTablesFinalDist: DataFrame = null
    var joinedAllTablesFinalDistTemp: DataFrame = null

    /*    if(joinedAllTablesFinalTemp!=null){
      logger.info("joinedAllTablesFinalTemp is not null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      // joinedAllTablesFinalTemp.select($"fileName",$"Latitude",$"Longitude",$"collectedB193_pci",$"collected_pci").show(5)
      geoBoundingDistDf = joinedAllTablesFinalTemp.filter($"Latitude".isNotNull && $"Longitude".isNotNull).select($"fileName",$"Latitude",$"Longitude",$"dateWithoutMillis").groupBy("fileName").agg(max("Latitude").as("Latitude_tl"),max("Longitude").as("Longitude_br"),min("Latitude").as("Latitude_br"),min("Longitude").as("Longitude_tl"),min("dateWithoutMillis").as("fileStartTime"),max("dateWithoutMillis").as("fileEndTime"))

      val getPciInfoForDist = udf((PciVal: String, lteeNbid: String, Latitude_tl: String, Longitude_tl: String, Latitude_br: String, Longitude_br: String, startTime:String, endTIme:String) =>
      {
        val cellIndexes = getCellSiteIndex(startTime.split(" ").apply(0),startTime.split(" ").apply(0),"cellsite")
        val pciRequestInfo = PciRequestInfo(PciVal, lteeNbid, Latitude_tl.toDouble, Longitude_tl.toDouble, Latitude_br.toDouble, Longitude_br.toDouble, cellIndexes, startTime.split(" ").apply(0), endTIme.split(" ").apply(0))
        val localTransportAddress = LocalTransportAddress(commonConfigParams.ES_NODE_ID,commonConfigParams.ES_PORT_ID,commonConfigParams.ES_CLUSTER_NAME)
        CommonDaoImpl.getPCIBasedLatLon(pciRequestInfo, localTransportAddress)
      })

      if(geoBoundingDistDf != null) {
        kpiGeoDistToCellsQuery = true
        val geoBoundingDistDfTemp = geoBoundingDistDf.join(dmatB192_NeighboringPciDist, Seq("fileName"), "left_outer")
          .join(dmatB193_ServingPciDist, Seq("fileName"), "left_outer")
        joinedAllTablesFinalDist = geoBoundingDistDfTemp.withColumn("servingPciInfo", when($"ServingPCI".isNotNull,getPciInfoForDist($"ServingPCI",$"Latitude_tl", $"Longitude_tl", $"Latitude_br",$"Longitude_br",$"fileStartTime",$"fileEndTIme")).otherwise(lit(null))).withColumn("NeighboringPciInfo", when($"NeighboringPCI".isNotNull,getPciInfoForDist($"NeighboringPCI",$"Latitude_tl", $"Longitude_tl", $"Latitude_br",$"Longitude_br",$"fileStartTime",$"fileEndTIme")).otherwise(lit(null)))
        CommonDaoImpl.closeESClient()
      }
    }*/

    if (joinedAllTablesFinalDist != null) {
      joinedAllTablesFinalDistTemp = joinedAllTablesFinalTemp.join(
        joinedAllTablesFinalDist,
        Seq("fileName"),
        "left_outer")
    }

    if (logsTblDs != null) {
      logger.info("logsTblDs is not null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      logsTblDs.show
    }
    var joinedAllTables: DataFrame = null
    if (joinedAllTablesFinalDistTemp != null) {
      joinedAllTables = joinedAllTablesFinalDistTemp.join(logsTblDs,
                                                          Seq("fileName"),
                                                          "left_outer")
    }
    //logger.info("joinedAllTables => " + joinedAllTables.show(200))

    logger.info(
      "Finished Joining tables ==>> " + (System.currentTimeMillis - startTimeMerge))

    var logRecordJoinedDF: LogRecordJoinedDF = LogRecordJoinedDF(
      kpiisRachExists,
      kpiisRlcUlInstTputExists,
      kpiPdcpUlInstTputExists,
      kpiRlcInstTputExists,
      kpiPdcpDlExists,
      kpiDmatIpExists,
      kpiRlcDLTputExists,
      kpiNrDlScg5MsTputExists,
      kpiNrPhyULThroughputExists,
      kpi5gDlDataBytesExists,
      //      kpiDmatRtpGapQuery,
      //      kpiRtpDslFieldExistsQuery,
      //      kpiDmatOperatorQuery,
      //      kpi4gDlTputQuery,
      //      kpiVoLTEcallQuery,
      kpiGeoDistToCellsQuery,
      kpiisinbuilding,
      gNbidGeoDistToCellsQuery,
      joinedAllTables = joinedAllTables
    )

    //    var logRecordJoinedDF: LogRecordJoinedDF = LogRecordJoinedDF(
    //      kpi0xB193ExistsQuery,
    //      kpiLteNormalSrvQuery,
    //      kpiWcdmaNormalSrvQuery,
    //      kpiGsmNormalSrvQuery,
    //      kpiCdmaNormalSrvQuery,
    //      kpiNoSrvQuery,
    //      kpiDmatIPDSSrvQuery,
    //      kpiPdcpRlcTputDFQuery,
    //      kpiDmatAsnQuery,
    //      kpiDmatIpQuery,
    //      kpiQualComm5GExistsQuery,
    //      kpiRtpDslFieldExistsQuery,
    //      joinedAllTables,
    //      dmatIp
    //    )
    //
    logRecordJoinedDF
  }

  def getMOD4g5gFiles(fileNames: List[String]): String = {
    var sb = new StringBuffer()

    val FileCheck = """(\w+)_mod4g5g_merged_([\w.]+)""".r
    try {
      for (i <- 0 to fileNames.size - 1) {
        if (fileNames(i).toLowerCase().matches(FileCheck.toString())) {
          sb.append(fileNames(i))
          sb.append(";")
        }
      }
    } catch {
      case ge: Throwable =>
        logger.error(
          "Exception occured while checking Mod4g5g files : " + ge.getMessage)
    }
    sb.toString()
  }
}
