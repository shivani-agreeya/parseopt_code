package com.verizon.oneparser.eslogrecord

import java.sql.Timestamp
import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants.{LocalTransportAddress, PciRequestInfo, gNbidRequestInfo}
import com.verizon.oneparser.common.{CommonDaoImpl, Constants, DBSink, LteIneffEventInfo}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.LogsData
import com.verizon.oneparser.eslogrecord.utils.ParseUtils.getCellSiteIndex
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator

import scala.collection.immutable.{List, Seq}
import scala.collection.mutable.{ListBuffer, WrappedArray}
import scala.reflect.internal.util.Collections
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter
import com.vividsolutions.jts.geom.Geometry
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery

object ExecuteSqlQueryForSingle extends Serializable with LazyLogging {

  def getTestThroughputJoin(fileNamesStr: String,
                            datesStr: String,
                            testIds: String,
                            spark: SparkSession,
                            logsTblDs_array: Array[LogsData],
                            startTimeRead: Long,
                            fileNamesList: List[String],
                            hiveTableName: String,
                            reportFlag: Boolean,
                            commonConfigParams: CommonConfigParameters,
                            isSigfileType: Boolean): DataFrame = {

    import spark.implicits._
    val logsTblDs = logsTblDs_array.toList.toDF()

    val kpiDmatIpQuery: Boolean = true
    val kpiRtpDslFieldExistsQuery: Boolean = true
    val kpiDmatRtpGapQuery: Boolean = true
    val kpiNrDlScg5MsTputQuery: Boolean = true
    val kpi5gDlDataBytesQuery: Boolean = true
    val kpiDmatOperatorQuery: Boolean = true
    val kpi4gDlTputQuery: Boolean = true
    var kpiVoLTEcallQuery: Boolean = true
    var kpiisinbuilding: Boolean = false
    var kpiGeoDistToCellsQuery: Boolean = false

    logger.info("Get the filenames: " + fileNamesStr)

    val fileDataFrame = spark.sql(
      "select * from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from " + hiveTableName + " where " +
        "insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ")) distribute by filename,dateWithoutMillis")

    fileDataFrame.createOrReplaceTempView("fileDataFrame")

    val dmatMilliSecQuery
      : String = "select * from (select fileName,dmTimeStamp,dateWithoutMillis, testId, insertedDate, " + B192QuerySingle.query + "," + B193QuerySingle.query + "," +
      IpQuerySingle.query + "," + Nr5gQueryForSingle.query + "," + AsnQueryForSingle.query + "," +
      "stk_SYS_MODE0_0x184E, stk_SRV_STATUS0x184E, stk_SYS_MODE, stk_SRV_STATUS, hexlogcode,  inbldImgId, inbldX, inbldY, inbldUserMark, inbldAccuracy, inbldAltitude, lteeNBId " +
      "from fileDataFrame)"

    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      if (startTime == null || endTime == null) 0.0d.toLong
      else startTime.getTime() - endTime.getTime()
    })

    val timestamp_diffseconds = udf(
      (startTime: Timestamp, endTime: Timestamp) => {
        if (startTime == null || endTime == null) 0.0d
        else
          (startTime.getTime().toDouble - endTime.getTime().toDouble) / 1000.0d
      })

    val dmatMillSecDataframeTemp = spark.sql(dmatMilliSecQuery)
    dmatMillSecDataframeTemp.createOrReplaceTempView("dmatMillSecDataframeTemp")

    val totaltxbytes0xb868Duery =
      """select totaltxbytes_0xb868,dmtimestamp,filename,lag(totaltxbytes_0xb868,1,null) over (partition by filename order by dmtimestamp) as prevtotaltxbytes0xb868,
      lag(dmtimestamp,1,null) over (partition by filename order by dmtimestamp)  prevdmtimestamp
      from dmatMillSecDataframeTemp where logcode in ('47208')"""

    val nr5grlculinstTputDf = spark.sql(totaltxbytes0xb868Duery)

    val nr5grlculinstTputDiffsDf = nr5grlculinstTputDf
      .withColumn("nr5grlculinstTput0xb868DiffSeconds",
                  timestamp_diffseconds(col("dmtimestamp"),
                                        col("prevdmtimestamp")))
      .withColumn("nr5grlculinstTput0xb868DiffTbs",
                  col("totaltxbytes_0xb868") - col("prevtotaltxbytes0xb868"))
      .drop("prevtotaltxbytes0xb868", "prevdmtimestamp")
      .toDF("totaltxbytes_0xb868",
            "dmtimestamp",
            "filename",
            "nr5grlculinstTput0xb868DiffSeconds",
            "nr5grlculinstTput0xb868DiffTbs")

    val totalNumRxBytes0xb860query =
      """select totalNumRxBytes_0xB860,dmtimestamp,filename,lag(totalNumRxBytes_0xB860,1,null) over (partition by filename order by dmtimestamp) as prevtotalNumRxBytes0xB860,
        lag(dmtimestamp,1,null) over (partition by filename order by dmtimestamp)  prevdmtimestamp
        from dmatMillSecDataframeTemp where logcode in ('47200')"""

    val nr5gNumRxBytes0xb860TputDf = spark.sql(totalNumRxBytes0xb860query)

    val nr5gNumRxBytes0xb860TputDiffsDf = nr5gNumRxBytes0xb860TputDf
      .withColumn("nr5gRxBytes0xB860DiffSeconds",
                  timestamp_diffseconds(col("dmtimestamp"),
                                        col("prevdmtimestamp")))
      .withColumn(
        "nr5gRxBytes0xB860DiffTbs",
        col("totalNumRxBytes_0xB860") - col("prevtotalNumRxBytes0xB860"))
      .drop("prevtotalNumRxBytes0xB860", "prevdmtimestamp")
      .toDF("totalNumRxBytes_0xB860",
            "dmtimestamp",
            "filename",
            "nr5gRxBytes0xB860DiffSeconds",
            "nr5gRxBytes0xB860DiffTbs")
    /*nr5gNumRxBytes0xb860TputDiffsDf.collect().foreach(row => {
      logger.info(s"totalNumRxBytes_0xB860 : ${row(0)} , filename: ${row(2)}, nr5gRxBytes0xB860DiffSeconds:${row(3)}")
    })*/
    logger.info(
      "nr5gNumRxBytes0xb860TputDiffsDf count >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ============" + nr5gNumRxBytes0xb860TputDiffsDf.count)
    val dmatMillSecDataframe = dmatMillSecDataframeTemp
      .join(nr5grlculinstTputDiffsDf,
            Seq("fileName", "totaltxbytes_0xb868", "dmTimeStamp"),
            "left_outer")
      .join(nr5gNumRxBytes0xb860TputDiffsDf,
            Seq("fileName", "totalNumRxBytes_0xB860", "dmTimeStamp"),
            joinType = "left_outer")
    val start = System.currentTimeMillis()
    logger.info(
      s"dmatMillSecDataframe count is ================================================================= ${dmatMillSecDataframe
        .count()}")
    val joinTime = System.currentTimeMillis() - start
    logger.info(
      "joined dmatmillsecdf and nr5grlculinstTputDiffsDf , nr5gNumRxBytes0xb860TputDiffsDf ;;;;;;;;;;;;;;;;;;;;;;;;;;;>>>>>>>=====" + joinTime)
    dmatMillSecDataframe
  }

  def getJoinedTable(fileNamesStr: String,
                     datesStr: String,
                     testIds: String,
                     spark: SparkSession,
                     logsTblDs: DataFrame,
                     logcodeList: Array[String],
                     startTimeRead: Long,
                     fileNamesList: List[String],
                     hiveTableName: String,
                     reportFlag: Boolean,
                     commonConfigParams: CommonConfigParameters): LogRecordJoinedDF = {
    import spark.implicits._
    //val logsTblDs = logsTblDs_array.toList.toDF()

    var kpiisRachExists:Boolean = false
    var kpiisRlcUlInstTputExists:Boolean = false
    var kpiPdcpUlInstTputExists:Boolean = false
    var kpiRlcInstTputExists:Boolean = false
    var kpiPdcpDlExists:Boolean = false
    var kpiDmatIpExists: Boolean = false
    var kpiRlcDLTputExists:Boolean = false
//    val kpiRtpDslFieldExistsQuery: Boolean = true
//    val kpiDmatRtpGapQuery: Boolean = true
    var kpiNrDlScg5MsTputExists: Boolean = false
    var kpiNrPhyULThroughputExists:Boolean = false
    var kpi5gDlDataBytesExists: Boolean = false
   // val kpiDmatOperatorQuery: Boolean = true
//    val kpi4gDlTputQuery: Boolean = true
//    var kpiVoLTEcallQuery: Boolean = true
    var kpiisinbuilding: Boolean = false
    var kpiGeoDistToCellsQuery: Boolean = false
    var gNbidGeoDistToCellsQuery: Boolean = false
    logger.info("Get the filenames: " + fileNamesStr)

    var dmatEvents = ListBuffer($"lteRrcConRestRej", $"lteRrcConRej", $"VoLTEAbortEvt", $"VoLTEDropEvt", $"ltePdnRej", $"lteAttRej",
      $"lteAuthRej", $"lteServiceRej", $"lteTaRej", $"lteOos", $"lteRlf", $"lteIntraReselFail", $"lteIntraHoFail", $"lteMobilityFromEutraFail",
      $"lteSibReadFailure", $"lteReselFromGsmUmtsFail", $"NRBeamFailureEvt", $"NRRrcHoFailEvt",
      $"NRRlfEvt", $"NRScgFailureEvt")

    val fileDataFrame = spark.sql(
      "select * from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from " + hiveTableName + " where " +
        "insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ")) distribute by filename,dateWithoutMillis")
    fileDataFrame.createOrReplaceTempView("fileDataFrame")

    val dmatOneSecQuery
      : String = "select * from (select fileName,dmTimeStamp,dateWithoutMillis, testId, insertedDate, " +
      QualCommQueryForSingle.getQCommQuery(false) +
      QualComm2QueryForSingle.getQComm2Query(false) +
      AsnQuery.getAsnQuery(false) +
      QualCommQueryForSingle.getQuery4gTput(false) +
      IpQuery.getIpQuery(false) +
      QualCommQueryForSingle.getQueryPdcpRlcTput(false) +
      NasQueryForSingle.getNasQuery(false) +
      "last_value(voLTEDropEvt,true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as VoLTEDropEvt, " +
      "last_value(mcc_QComm, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mcc_QComm, " +
      "last_value(mnc_QComm, true) over(partition by filename,dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mnc_QComm, " +
      "last_value(lteCellID, true) over(partition by filename,dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteCellID, " +
      "last_value(lteeNBId, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteeNBId, " +
      "last_value(pUSCHTxPower, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower, " +
      "last_value(CarrierID_0xB887, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CarrierID0xB887, " +
      "last_value(NRDLMCS, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRDLMCS0xB887, " +
      "last_value(cellIdentity, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cellIdentity, " +
      "last_value(spCellBandDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellBandDL, " +
      "last_value(spCellNRARFCNDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellNRARFCNDL, " +
      "last_value(spCellSubCarSpaceDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellSubCarSpaceDL, " +
      "last_value(spCellNumPRBDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellNumPRBDL, " +
      "last_value(spCellBandwidthDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellBandwidthDL, " +
      "last_value(spCellDuplexMode, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellDuplexMode, " +
      "last_value(spCellTypeDL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellTypeDL, " +
      "last_value(spCellBandUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellBandUL, " +
      "last_value(spCellNRARFCNUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellNRARFCNUL, " +
      "last_value(spCellSubCarSpaceUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellSubCarSpaceUL, " +
      "last_value(spCellNumPRBUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellNumPRBUL, " +
      "last_value(spCellBandwidthUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellBandwidthUL, " +
      "last_value(spCellTypeUL, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spCellTypeUL, " +
      "last_value(nrSpcellPci, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrSpcellPci, " +
      "last_value(NRCellId_0xB825, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrcellid, " +
      "last_value(NRArfcn_0xB825, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrarfcn, " +
      "last_value(dmTimeStamp) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lasttimestamp, " +
      "last_value(totaltxbytes_0xb868, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lasttotaltxbytes0xb868, " +
      "last_value(totalNumRxBytes_0xB860, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lasttotalNumRxBytes0xB860, " +
      "last_value(rlcDataBytesVal_0xB84D, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastrlcDataBytes0xB84D, " +
      "last_value(dataPDUBytesReceivedAllRBSum_0xB842, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastdataPDUBytesReceivedAllRBSum_0xB842, " +
      "last_value(controlPDUBytesReceivedAllRBSum_0xB842, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastcontrolPDUBytesReceivedAllRBSum_0xB842, " +
      "last_value(numMissPDUToUpperLayerSum_0xB842, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumMissPDUToUpperLayerSum_0xB842, " +
      "last_value(numDataPDUReceivedSum_0xB842, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumDataPDUReceivedSum_0xB842, " +
      "last_value(totalNumDiscardPacketsSum_0xB860, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastTotalNumDiscardPacketsSum_0xB860, " +
      "last_value(numRXPacketsSum_0xB860, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumRXPacketsSum_0xB860, " +
      "last_value(totalReTxPDUsSum_0x0xB868, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastTotalReTxPDUsSum_0x0xB868, " +
      "last_value(totalTxPDUsSum_0x0xB868, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastTotalTxPDUsSum_0x0xB868, " +
      "last_value(numReTxPDUSum_0xB84D, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumReTxPDUSum_0xB84D, " +
      "last_value(numMissedUMPDUSum_0xB84D, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumMissedUMPDUSum_0xB84D, " +
      "last_value(numDroppedPDUSum_0xB84D, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumDroppedPDUSum_0xB84D, " +
      "last_value(numDataPDUSum_0xB84D, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastNumDataPDUSum_0xB84D, " +
      "last_value(tbNewTxBytesB881, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lasttbNewTxBytesB881, " +
      "last_value(numNewTxTbB881, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastnumNewTxTbB881, " +
      "last_value(numReTxTbB881, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastnumReTxTbB881, " +
      "last_value(nr5GPhyULInstTputPCC_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstTputPCC_0XB883, " +
      "last_value(nr5GPhyULInstTputSCC1_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstTputSCC1_0XB883, " +
      "last_value(nr5GPhyULInstTput_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstTput_0XB883, " +
      "last_value(nr5GPhyULInstRetransRatePCC_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstRetransRatePCC_0XB883, " +
      "last_value(nr5GPhyULInstRetransRateSCC1_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstRetransRateSCC1_0XB883, " +
      "last_value(nr5GPhyULInstRetransRate_0XB883, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr5GPhyULInstRetransRate_0XB883, " +
      "last_value(rlcDlDataBytes, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlBytes, " +
      "last_value(rlcRbConfigIndex, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcRbConfigIndex, " +
      "last_value(rlcDlNumRBs, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlNumRBs, " +
      "last_value(rlcDlTimeStamp, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTimeStamp0xb087v49, " +
      "last_value(ipSubtitle, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as subtitle, " +
      "last_value(httpMethod, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as httpMethod, " +
      "last_value(httpHost, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as httpHost, " +
      "last_value(httpContentLength, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as httpContentLength, " +
      //      "last_value(TB_Size_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeCurrent, " +
      //      "last_value(TB_SizePcc_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizePccCurrent, " +
      //      "last_value(TB_SizeScc1_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc1Current, " +
      //      "last_value(TB_SizeScc2_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc2Current, " +
      //      "last_value(TB_SizeScc3_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc3Current, " +
      //      "last_value(TB_SizeScc4_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc4Current, " +
      //      "last_value(TB_SizeScc5_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc5Current, " +
      //      "last_value(TB_SizeScc6_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc6Current, " +
      //      "last_value(TB_SizeScc7_0xB888, true) over(partition by filename, dateWithoutMillis order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tbSizeScc7Current, " +
      "row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn " +
      "from (select * from fileDataFrame where (inbldUserMark != 'true' or inbldUserMark != 'false'))) where rn = 1 "

    val dmatDataframe = spark.sql(dmatOneSecQuery)

    dmatDataframe.createOrReplaceTempView("dmatDataframe")

    val filePartitionKpiDf = spark.sql("select fileName, dateWithoutMillis,Latitude,Longitude,Latitude2,Longitude2," +
      "last_value(mSYS_MODE_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as mSYS_MODE," +
      "last_value(mSYS1_MODE_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as mSYS1_MODE," +
      "last_value(mSRV_STATUS_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as mSRV_STATUS, " +
      "last_value(mSRV1_STATUS_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as mSRV1_STATUS, " +
      "last_value(ROAM_STATUS_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as ROAM_STATUS, " +
      "last_value(lteRrcState_temp, true) over(partition by filename order by dmTimeStamp asc rows between unbounded preceding and current row) as lteRrcState, " +
      "last_value(stk_SYS_MODE0_0x184E_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as stk_SYS_MODE0_0x184E, " +
      "last_value(stk_SYS_MODE1_0x184E_temp, true) over(partition by filename  order by dmTimeStamp desc rows between unbounded preceding and current row) as stk_SYS_MODE1_0x184E," +
      "last_value(SRV_STATUS0x184E_temp, true) over(partition by filename  order by dmTimeStamp desc rows between unbounded preceding and current row) as SRV_STATUS0x184E, " +
      "last_value(nrSpCellPci_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellPci, " +
      "last_value(nrCellId_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrCellId, " +
      "last_value(nrArfcn_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrArfcn, " +
      "last_value(nrSpCellDlFreq_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDlFreq, " +
        "last_value(nrSpCellBand_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellBand, " +
        "last_value(nrSpCellDuplexMode_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDuplexMode, " +
        "last_value(nrSpCellDlBw_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDlBw, " +
        "last_value(nrSpCellUlBw_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellUlBw, " +
        "last_value(nrSpCellUlFreq_temp, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellUlFreq, " +
        "last_value(Latitude, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLatitude, " +
        "last_value(Longitude, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLongitude," +
        "last_value(Latitude2, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLatitude2, " +
        "last_value(Longitude2, true) over(partition by filename order by cast(dateWithoutMillis as timestamp) asc range between interval 5 seconds preceding and current row) as previousLongitude2, " +
        "coalesce(mcc_QComm,last_value(mcc_QComm,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as MCC, " +
        "coalesce(mnc_QComm,last_value (mnc_QComm,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as MNC, " +
        "coalesce(lteCellID,last_value(lteCellID,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as LteCellID_deriv, " +
        "coalesce(lteeNBId,last_value(lteeNBId,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as LteeNBId_deriv, " +
        "coalesce(pUSCHTxPower,last_value(pUSCHTxPower,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as PuschTxPower_deriv," +
        "coalesce(cellIdentity,last_value(cellIdentity,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as CellIdentity_deriv," +

        "coalesce(spCellBandDL,last_value(spCellBandDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellBandDL_deriv," +
        "coalesce(spCellNRARFCNDL,last_value(spCellNRARFCNDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellNRARFCNDL_deriv," +
        "coalesce(spCellSubCarSpaceDL,last_value(spCellSubCarSpaceDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellSubCarSpaceDL_deriv," +
        "coalesce(spCellNumPRBDL,last_value(spCellNumPRBDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellNumPRBDL_deriv," +
        "coalesce(spCellBandwidthDL,last_value(spCellBandwidthDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellBandwidthDL_deriv," +
        "coalesce(spCellDuplexMode,last_value(spCellDuplexMode,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellDuplexMode_deriv," +
        "coalesce(spCellTypeDL,last_value(spCellTypeDL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellTypeDL_deriv," +

        "coalesce(spCellBandUL,last_value(spCellBandUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellBandUL_deriv," +
        "coalesce(spCellNRARFCNUL,last_value(spCellNRARFCNUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellNRARFCNUL_deriv," +
        "coalesce(spCellSubCarSpaceUL,last_value(spCellSubCarSpaceUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellSubCarSpaceUL_deriv," +
        "coalesce(spCellNumPRBUL,last_value(spCellNumPRBUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellNumPRBUL_deriv," +
        "coalesce(spCellBandwidthUL,last_value(spCellBandwidthUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellBandwidthUL_deriv," +
        "coalesce(spCellTypeUL,last_value(spCellTypeUL,true) over(partition by filename order by dateWithoutMillis rows between unbounded preceding and 1 preceding)) as spCellTypeUL_deriv," +
      "last_value(RrcConSetupCmp,true) over (partition by filename order by cast(dateWithoutMillis as timestamp) asc range between current row and interval 1 seconds following) as postRrcConSetupCmp, " +
    //  "last_value(nrReleaseEvt,true) over (partition by filename order by cast(dateWithoutMillis as timestamp) asc range between current row and unbounded following) as nrReleaseFornrscgstate, " +
        "lag(pdcpDlTotalBytes,1) over(partition by filename order by dateWithoutMillis) as prePdcpDlTotalBytes," +
        "lag(pdcpDlTimeStamp,1) over(partition by filename order by dateWithoutMillis) as prePdcpDlTimeStamp," +
        "lag(pdcpUlTotalBytes,1) over(partition by filename order by dateWithoutMillis) as prePdcpUlTotalBytes," +
        "lag(pdcpUlTimeStamp,1) over(partition by filename order by dateWithoutMillis) as prePdcpUlTimeStamp ," +
        "lag(rlcDlTotalBytes,1) over(partition by filename order by dateWithoutMillis) as prerlcDlTotalBytes," +
        "lag(rlcDlTimeStamp,1) over(partition by filename order by dateWithoutMillis) as prerlcDlTimeStamp," +
        "lag(rlcUlTotalBytes,1) over(partition by filename order by dateWithoutMillis) as prerlcUlTotalBytes," +
      "lag(rlcUlTimeStamp,1) over(partition by filename order by dateWithoutMillis) as prerlcUlTimeStamp," +
      "lag(lasttimestamp,1) over(partition by filename order by dateWithoutMillis) as prevdmtimestamp, " +
      "lag(lasttotaltxbytes0xb868,1) over(partition by filename order by dateWithoutMillis) as prevtotaltxbytes0xb868, " +
      "lag(lasttotalNumRxBytes0xB860,1) over(partition by filename order by dateWithoutMillis) as prevtotalNumRxBytes0xB860, " +
      "lag(lastrlcDataBytes0xB84D,1) over(partition by filename order by dateWithoutMillis) as prevrlcDataBytes0xB84D, " +
      "lag(lastdataPDUBytesReceivedAllRBSum_0xB842,1) over(partition by filename order by dateWithoutMillis) as prevdataPDUBytesReceivedAllRBSum_0xB842, " +
      "lag(lastcontrolPDUBytesReceivedAllRBSum_0xB842,1) over(partition by filename order by dateWithoutMillis) as prevcontrolPDUBytesReceivedAllRBSum_0xB842, " +
      "lag(lastNumMissPDUToUpperLayerSum_0xB842,1) over(partition by filename order by dateWithoutMillis) as prevNumMissPDUToUpperLayerSum_0xB842, " +
      "lag(lastNumDataPDUReceivedSum_0xB842,1) over(partition by filename order by dateWithoutMillis) as prevNumDataPDUReceivedSum_0xB842, " +
      "lag(lastTotalNumDiscardPacketsSum_0xB860,1) over(partition by filename order by dateWithoutMillis) as prevTotalNumDiscardPacketsSum_0xB860, " +
      "lag(lastNumRXPacketsSum_0xB860,1) over(partition by filename order by dateWithoutMillis) as prevNumRXPacketsSum_0xB860, " +
      "lag(lastTotalReTxPDUsSum_0x0xB868,1) over(partition by filename order by dateWithoutMillis) as prevTotalReTxPDUsSum_0x0xB868, " +
      "lag(lastTotalTxPDUsSum_0x0xB868,1) over(partition by filename order by dateWithoutMillis) as prevTotalTxPDUsSum_0x0xB868, " +
      "lag(lastNumReTxPDUSum_0xB84D,1) over(partition by filename order by dateWithoutMillis) as prevNumReTxPDUSum_0xB84D, " +
      "lag(lastNumMissedUMPDUSum_0xB84D,1) over(partition by filename order by dateWithoutMillis) as prevNumMissedUMPDUSum_0xB84D, " +
      "lag(lastNumDroppedPDUSum_0xB84D,1) over(partition by filename order by dateWithoutMillis) as prevNumDroppedPDUSum_0xB84D, " +
      "lag(lastNumDataPDUSum_0xB84D,1) over(partition by filename order by dateWithoutMillis) as prevNumDataPDUSum_0xB84D, " +
      "lag(lasttbNewTxBytesB881,1) over(partition by filename order by dateWithoutMillis) as tbNewTxBytesB881_prev, " +
      "lag(lastnumNewTxTbB881,1) over(partition by filename order by dateWithoutMillis) as numNewTxTbB881_prev, " +
      "lag(lastnumReTxTbB881,1) over(partition by filename order by dateWithoutMillis) as numReTxTbB881_prev, " +
      "lag(rlcDlBytes,1) over(partition by filename order by dateWithoutMillis) as prerlcDlBytes, " +
      "lag(rlcRbConfigIndex,1) over(partition by filename order by dateWithoutMillis) as prerlcRbConfigIndex, " +
      "lag(rlcDlNumRBs,1) over(partition by filename order by dateWithoutMillis) as prerlcDlNumRBs, " +
      "lag(rlcDlTimeStamp0xb087v49,1) over(partition by filename order by dateWithoutMillis) as prerlcDlTimeStamp0xb087v49, " +
      "last_value(m2mReceiveEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as M2MReceive_End," +
      "last_value(m2mCallEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as M2MCall_End," +
      "last_value(udpechoEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as UDPECHO_End, " +
      "last_value(smsEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as SMS_End, " +
      "last_value(idleEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as Idle_End," +
      "last_value(dnsEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as DNS_End, " +
      "last_value(ldrEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as LDR_End, " +
      "last_value(ldrsEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as LDRS_End, " +
      "last_value(uploadEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as UPLOAD_End, " +
      "last_value(downloadEnd, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and current row) as DOWNLOAD_End, " +
      "last_value(testCycleId, true) over(partition by filename order by dmTimeStamp asc rows between unbounded preceding and current row) as testcycleid " +
      //      "lag(tbSizeCurrent,1) over(partition by filename order by dateWithoutMillis) as tbSizePrev, " +
      //      "lag(tbSizePccCurrent,1) over(partition by filename order by dateWithoutMillis) as tbSizePccPrev, " +
      //      "lag(tbSizeScc1Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc1Prev, " +
      //      "lag(tbSizeScc2Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc2Prev, " +
      //      "lag(tbSizeScc3Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc3Prev, " +
      //      "lag(tbSizeScc4Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc4Prev, " +
      //      "lag(tbSizeScc5Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc5Prev, " +
      //      "lag(tbSizeScc6Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc6Prev, " +
      //      "lag(tbSizeScc7Current,1) over(partition by filename order by dateWithoutMillis) as tbSizeScc7Prev " +
      "from dmatDataFrame")

    /** **** Get the operator kpi for dlf file. ******/
    import org.apache.spark.sql.types.{
      StructType, StructField, StringType
    }
    import org.apache.spark.sql.Row
    val schema = StructType(
      StructField("fileName", StringType, true) ::
        StructField("dateWithoutMillis", StringType, true) ::
        StructField("dmTimeStamp", StringType, true) :: Nil)

    var stateDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    var countyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      val stateSpatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, commonConfigParams.GEO_SHAPE_FILE_PATH + "States_VZW*")
      val countiesSpatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, commonConfigParams.GEO_SHAPE_FILE_PATH + "Counties_VZW*")

      val filePartitionKpiDfTemp = filePartitionKpiDf.select(col("Latitude").alias("Lat1").cast(DecimalType(10, 4)), col("Longitude").alias("Long1").cast(DecimalType(10, 4)),
        col("Latitude2").alias("Lat2").cast(DecimalType(10, 4)), col("Longitude2").alias("Long2").cast(DecimalType(10, 4)), col("fileName"), col("dateWithoutMillis"))
        .filter((col("Latitude").isNotNull && col("Longitude").isNotNull) || (col("Latitude2").isNotNull && col("Longitude2").isNotNull))

      filePartitionKpiDfTemp.createOrReplaceTempView("filePartitionKpiDfTemp")

      val geoshapeDfWithXY = spark.sql("select (case when Long1 is not null and Lat1 is not null then ST_Transform(ST_Point(Long1, Lat1), 'epsg:4326', 'epsg:3857') " +
        " when Long2 is not null and Lat2 is not null then ST_Transform(ST_Point(Long2, Lat2), 'epsg:4326', 'epsg:3857') " +
        "else ST_Transform(ST_Point(0.0, 0.0), 'epsg:4326', 'epsg:3857') end) as point, fileName, dateWithoutMillis from filePartitionKpiDfTemp")

    geoshapeDfWithXY.createOrReplaceTempView("geoshapeDfWithXY")

    val geoshapeRDD = new SpatialRDD[Geometry]
    geoshapeRDD.rawSpatialRDD = Adapter.toRdd(geoshapeDfWithXY)
    stateSpatialRDD.analyze()
    stateSpatialRDD.spatialPartitioning(GridType.QUADTREE)
    geoshapeRDD.spatialPartitioning(stateSpatialRDD.getPartitioner)
    val joinResultPairRDD1 = JoinQuery.SpatialJoinQueryFlat(geoshapeRDD, stateSpatialRDD, true, true)
    var joinResultDf1 = Adapter.toDf(joinResultPairRDD1, spark)
    stateDf = joinResultDf1.select(col("_c5").alias("stusps"), col("_c6").alias("vz_regions"), col("_c8").alias("CountryCode"), col("_c12").alias("fileName"), col("_c13").alias("dateWithoutMillis"))

    countiesSpatialRDD.analyze()
    countiesSpatialRDD.spatialPartitioning(GridType.QUADTREE)
    geoshapeRDD.spatialPartitioning(countiesSpatialRDD.getPartitioner)
    val joinResultPairRDD2 = JoinQuery.SpatialJoinQueryFlat(geoshapeRDD, countiesSpatialRDD, true, true)
    var joinResultDf2 = Adapter.toDf(joinResultPairRDD2, spark)
    countyDf = joinResultDf2.select(col("_c4").alias("countyns"), col("_c10").alias("fileName"), col("_c11").alias("dateWithoutMillis"))

    var finalOperatorKpiDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    //    logger.info(s"finalOperatorKpiDf=$finalOperatorKpiDf")
    finalOperatorKpiDf = dmatDataframe.join(filePartitionKpiDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
      .join(stateDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
      .join(countyDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
      .drop("mSYS_MODE_temp", "mSYS1_MODE_temp", "mSRV_STATUS_temp",
        "mSRV1_STATUS_temp", "ROAM_STATUS_temp", "lteRrcState_temp", "stk_SYS_MODE0_0x184E_temp", "stk_SYS_MODE1_0x184E_temp", "SRV_STATUS0x184E_temp", "nrSpCellPci_temp", "nrCellId_temp", "nrArfcn_temp", "nrSpCellDlFreq_temp",
        "nrSpCellBand_temp", "nrSpCellDuplexMode_temp", "nrSpCellDlBw_temp", "nrSpCellUlBw_temp", "nrSpCellUlFreq_temp", "Lat1", "Long1", "Lat2", "Long2", "point")


    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      if (startTime == null || endTime == null) 0.0d.toLong
      else startTime.getTime() - endTime.getTime()
    })

    val timestamp_diffseconds = udf(
      (startTime: Timestamp, endTime: Timestamp) => {
        if (startTime == null || endTime == null) 0.0d
        else
          (startTime.getTime().toDouble - endTime.getTime().toDouble) / 1000.0d
      })

    finalOperatorKpiDf = finalOperatorKpiDf.withColumn("nr5grlculinstTput0xb868DiffSeconds",timestamp_diffseconds(col("lasttimestamp"),col("prevdmtimestamp")))
      .withColumn("nr5grlculinstTput0xb868DiffTbs",col("lasttotaltxbytes0xb868") - col("prevtotaltxbytes0xb868"))
      .withColumn("nr5gRxBytes0xB860DiffSeconds",timestamp_diffseconds(col("lasttimestamp"),col("prevdmtimestamp")))
      .withColumn("nr5gRxBytes0xB860DiffTbs",col("lasttotalNumRxBytes0xB860") - col("prevtotalNumRxBytes0xB860"))
      .withColumn("nr5gRlcDataBytes0xB84DDiffSeconds",timestamp_diffseconds(col("lasttimestamp"),col("prevdmtimestamp")))
      .withColumn("nr5gRlcDataBytes0xB84DDiff",col("lastrlcDataBytes0xB84D") - col("prevrlcDataBytes0xB84D"))
      .withColumn("nr5GPdcpDl0xB842DiffSeconds",timestamp_diffseconds(col("lasttimestamp"),col("prevdmtimestamp")))
      .withColumn("nr5gPdcpDl0xB842DataDiff",(col("lastdataPDUBytesReceivedAllRBSum_0xB842") + col("lastcontrolPDUBytesReceivedAllRBSum_0xB842")) -
        (col("prevdataPDUBytesReceivedAllRBSum_0xB842") + col("prevcontrolPDUBytesReceivedAllRBSum_0xB842")))
      .withColumn("numMissPDUToUpperLayer0xB842Diff", col("lastNumMissPDUToUpperLayerSum_0xB842") - col("prevNumMissPDUToUpperLayerSum_0xB842"))
      .withColumn("numDataPDUReceived0xB842Diff", col("lastNumDataPDUReceivedSum_0xB842") - col("prevNumDataPDUReceivedSum_0xB842"))
      .withColumn("totalNumDiscardPackets0xB860Diff", col("lastTotalNumDiscardPacketsSum_0xB860") - col("prevTotalNumDiscardPacketsSum_0xB860"))
      .withColumn("numRXPackets0xB860Diff", col("lastNumRXPacketsSum_0xB860") - col("prevNumRXPacketsSum_0xB860"))
      .withColumn("totalReTxPDUs0x0xB868Diff", col("lastTotalReTxPDUsSum_0x0xB868") - col("prevTotalReTxPDUsSum_0x0xB868"))
      .withColumn("totalTxPDUs0x0xB868Diff", col("lastTotalTxPDUsSum_0x0xB868") - col("prevTotalTxPDUsSum_0x0xB868"))
      .withColumn("numReTxPDU0xB84DDiff", col("lastNumReTxPDUSum_0xB84D") - col("prevNumReTxPDUSum_0xB84D"))
      .withColumn("numMissedUMPDU0xB84DDiff", col("lastNumMissedUMPDUSum_0xB84D") - col("prevNumMissedUMPDUSum_0xB84D"))
      .withColumn("numDroppedPDU0xB84DDiff", col("lastNumDroppedPDUSum_0xB84D") - col("prevNumDroppedPDUSum_0xB84D"))
      .withColumn("numDataPDU0xB84DDiff", col("lastNumDataPDUSum_0xB84D") - col("prevNumDataPDUSum_0xB84D"))
      .withColumn("elapsedTimeDiffSeconds", timestamp_diffseconds($"lasttimestamp",$"prevdmtimestamp"))
      .withColumn("tbNewTxBytesB881_delta",coalesce($"lasttbNewTxBytesB881",lit(0)) - coalesce($"tbNewTxBytesB881_prev",lit(0)))
      .withColumn("numNewTxTbB881_delta", coalesce($"lastnumNewTxTbB881",lit(0)) - coalesce($"numNewTxTbB881_prev",lit(0)))
      .withColumn("numReTxTbB881_delta", coalesce($"lastnumReTxTbB881",lit(0)) - coalesce($"numReTxTbB881_prev",lit(0)))
      .withColumn("nr5gPhyULTputB881", when($"tbNewTxBytesB881_delta">0,$"tbNewTxBytesB881_delta" / $"elapsedTimeDiffSeconds"  ).otherwise(null))
      .withColumn("nr5gPhyULRetransRateB881", when(($"numNewTxTbB881_delta" + $"numReTxTbB881_delta")>0, ($"numReTxTbB881_delta" * 100) / ($"numNewTxTbB881_delta" + $"numReTxTbB881_delta")).otherwise(null))
//      .withColumn("timeDff", timestamp_diff(($"lasttimestamp"), ($"prevdmtimestamp")))
//      .withColumn("tPUT5MilliSeconds", when($"tbSizeCurrent".isNotNull && $"tbSizePrev".isNotNull,($"tbSizeCurrent" - $"tbSizePrev") * 1600 / Constants.Mbps).otherwise(null))
//      .withColumn("timeDffSeconds", timestamp_diffseconds(($"lasttimestamp"), ($"prevdmtimestamp")))
//      .withColumn("tPUT1Sec", when($"tbSizeCurrent".isNotNull && $"tbSizePrev".isNotNull,($"tbSizeCurrent" - $"tbSizePrev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTPccSec", when($"tbSizePccCurrent".isNotNull && $"tbSizePccPrev".isNotNull,($"tbSizePccCurrent" - $"tbSizePccPrev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc1Sec", when($"tbSizeScc1Current".isNotNull && $"tbSizeScc1Prev".isNotNull,($"tbSizeScc1Current" - $"tbSizeScc1Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc2Sec", when($"tbSizeScc2Current".isNotNull && $"tbSizeScc2Prev".isNotNull,($"tbSizeScc2Current" - $"tbSizeScc2Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc3Sec", when($"tbSizeScc3Current".isNotNull && $"tbSizeScc3Prev".isNotNull,($"tbSizeScc3Current" - $"tbSizeScc3Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc4Sec", when($"tbSizeScc4Current".isNotNull && $"tbSizeScc4Prev".isNotNull,($"tbSizeScc4Current" - $"tbSizeScc4Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc5Sec", when($"tbSizeScc5Current".isNotNull && $"tbSizeScc5Prev".isNotNull,($"tbSizeScc5Current" - $"tbSizeScc5Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc6Sec", when($"tbSizeScc6Current".isNotNull && $"tbSizeScc6Prev".isNotNull,($"tbSizeScc6Current" - $"tbSizeScc6Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
//      .withColumn("tPUTScc7Sec", when($"tbSizeScc7Current".isNotNull && $"tbSizeScc7Prev".isNotNull,($"tbSizeScc7Current" - $"tbSizeScc7Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).otherwise(null))
      .drop("lasttotaltxbytes0xb868","lasttotalNumRxBytes0xB860","lastrlcDataBytes0xB84D", "lastdataPDUBytesReceivedAllRBSum_0xB842","lastcontrolPDUBytesReceivedAllRBSum_0xB842",
        "lasttbNewTxBytesB881","lastnumNewTxTbB881","lastnumReTxTbB881","prevtotaltxbytes0xb868","prevtotalNumRxBytes0xB860","prevrlcDataBytes0xB84D",
        "prevdataPDUBytesReceivedAllRBSum_0xB842","prevcontrolPDUBytesReceivedAllRBSum_0xB842","tbNewTxBytesB881_prev","numNewTxTbB881_prev","numReTxTbB881_prev","prevdmtimestamp","lasttimestamp","dmtimestamp",
        "lastNumMissPDUToUpperLayerSum_0xB842", "lastNumDataPDUReceivedSum_0xB842", "prevNumMissPDUToUpperLayerSum_0xB842", "prevNumDataPDUReceivedSum_0xB842",
        "lastTotalNumDiscardPacketsSum_0xB860", "lastNumRXPacketsSum_0xB860", "lastTotalReTxPDUsSum_0x0xB868", "lastTotalTxPDUsSum_0x0xB868",
        "prevTotalNumDiscardPacketsSum_0xB860", "prevNumRXPacketsSum_0xB860", "prevTotalReTxPDUsSum_0x0xB868", "prevTotalTxPDUsSum_0x0xB868",
        "lastNumReTxPDUSum_0xB84D", "lastNumMissedUMPDUSum_0xB84D", "lastNumDroppedPDUSum_0xB84D", "lastNumDataPDUSum_0xB84D",
        "prevNumReTxPDUSum_0xB84D", "prevNumMissedUMPDUSum_0xB84D", "prevNumDroppedPDUSum_0xB84D", "prevNumDataPDUSum_0xB84D")
    /** **** Get the first and last location records for every file. ***** */
    val windowFirstLocationRecord =
      Window.partitionBy($"fileName").orderBy($"dateWithoutMillis".asc)
    val windowLastLocationRecord =
      Window.partitionBy($"fileName").orderBy($"dateWithoutMillis".desc)
    var firstLastLocationRecordDf: DataFrame =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    //if (!isSigfileType) {
      firstLastLocationRecordDf = dmatDataframe
        .select($"fileName",
                $"dateWithoutMillis",
                $"Longitude",
                $"Latitude",
                $"Longitude2",
                $"Latitude2")
        .filter(
          ($"Longitude".isNotNull && $"Latitude".isNotNull) || ($"Longitude2".isNotNull && $"Latitude2".isNotNull))
        .withColumn("rnFR", row_number.over(windowFirstLocationRecord))
        .withColumn("rnLR", row_number.over(windowLastLocationRecord))
        .select($"fileName", $"dateWithoutMillis", $"rnFR", $"rnLR")
        .where($"rnFR" === 1 || $"rnLR" === 1)
    //}

    /** **** Get the first and last location records for every file ***** */
    var finalJoinedTables: DataFrame =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    finalJoinedTables = finalOperatorKpiDf.join(
      firstLastLocationRecordDf,
      Seq("fileName", "dateWithoutMillis"),
      "left_outer")

    /** ***************************************** Processing of Millisecond Records ********************************************* */
    var joinedCombo =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val dmatMilliSecQuery
      : String = "select * from (select fileName,dmTimeStamp,dateWithoutMillis, testId, insertedDate, " + B192QuerySingle.query + "," + B193QuerySingle.query + "," +
      IpQuerySingle.query + "," + Nr5gQueryForSingle.query + "," + AsnQueryForSingle.query + "," +
      "stk_SYS_MODE0_0x184E, stk_SRV_STATUS0x184E, stk_SYS_MODE, stk_SRV_STATUS, hexlogcode,  inbldImgId, inbldX, inbldY, inbldUserMark, inbldAccuracy, inbldAltitude, lteeNBId " +
      "from fileDataFrame)"

    val rachGroup = udf((trigTime: Timestamp, trigger: Boolean) => {
      if (trigger) trigTime else null
    })

    val mergeList1 = udf { (strings: WrappedArray[WrappedArray[Integer]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    val mergeList2 = udf { (strings: WrappedArray[WrappedArray[Double]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    val mergeList3 = udf { (strings: WrappedArray[WrappedArray[String]]) =>
      strings.map(x => x.mkString(",")).mkString(",")
    }
    val mergeList4 = udf { (strings: WrappedArray[String]) =>
      strings.mkString(",")
    }


    // udf ignores rach aborted/failure that occurs within 2 seconds after connection request and success msgs to avoid being declared as nr setup failure evt
    val NrSetupSuccessUdf = udf((strings:String) => {
      var msg = ""
      if (strings.contains("SUCCESS") && strings.contains("ABORTED")) {
        var indxSucesss = strings.indexOf("SUCCESS")
        var indxFailure = strings.indexOf("ABORTED")
        if (indxFailure > indxSucesss) msg =  "SUCCESS"
      } else {
        msg = strings
      }
      msg
    })

    // udf ignores rach success that occurs within 2 seconds after connection request and failure msgs to avoid being declared as nr setup success evt
    val NrSetupFailureUdf = udf((strings:String) => {
      var msg = ""
      if (strings.contains("SUCCESS") && strings.contains("ABORTED")) {
        var indxSucesss = strings.indexOf("SUCCESS")
        var indxFailure = strings.indexOf("ABORTED")
        if (indxSucesss > indxFailure) msg =  "ABORTED"
      } else {
        msg = strings
      }
      msg
    })

    val mergeList5 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(row=>(row.getAs[Int]("freq"),row.getAs[Double]("rsrp")))
      if (data == null || data.size == 0) {
        data = List((0, 0.0))
      }

      def topN(n: Int, list: List[(Int, Double)]): List[(Int, Double)] = {
        def update(l: List[(Int, Double)],
                   e: (Int, Double)): List[(Int, Double)] = {
          if (e._2 > l.head._2) (e :: l.tail).sortWith(_._2 < _._2) else l
        }

        list.drop(n).foldLeft(list.take(n).sortWith(_._2 < _._2))(update)
      }

      if (data.size > 7)
        topN(7, data)
      else
        data
    }
    val mergeList5v2 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(row=>(row.getAs[Int]("freq"),row.getAs[Double]("rsrp")))
      if (data == null || data.size == 0) {
        data = List((0,0.0))
      }

      def topN(n: Int, list: List[(Int, Double)]): List[(Int, Double)] = {
        def update(l: List[(Int, Double)],
                   e: (Int, Double)): List[(Int, Double)] = {
          if (e._2 > l.head._2) (e :: l.tail).sortWith(_._2 < _._2) else l
        }

        list.drop(n).foldLeft(list.take(n).sortWith(_._2 < _._2))(update)
      }

      if (data.size > 7)
        topN(7, data)
          .map(freqpow => (freqpow._1 + "@" + freqpow._2))
          .mkString(",")
      else
        data.map(freqpow => (freqpow._1 + "@" + freqpow._2)).mkString(",")
    }
    val mergeList6 = udf { (strings: WrappedArray[Integer]) =>
      strings.mkString(",")
    }
    val flatten = udf((xs: WrappedArray[WrappedArray[WrappedArray[Int]]]) =>
      xs.flatten.flatten.distinct.mkString(","))
    val flatten1 = udf((xs: WrappedArray[WrappedArray[Int]]) =>
      xs.flatten.distinct.mkString(","))
    val flatten2 = udf((xs: WrappedArray[WrappedArray[String]]) =>
      xs.flatten.distinct.mkString(","))

    val mergeList7 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(
        row =>
          (row.getAs[Int]("freq"),
           row.getAs[String]("channelNumber"),
           row.getAs[List[String]]("cinr"),
           row.getAs[List[String]]("cellId"),
           row.getAs[List[String]]("primaryRsrp"),
           row.getAs[List[String]]("primaryRsrq"),
           row.getAs[List[String]]("secondaryRsrp"),
           row.getAs[List[String]]("secondaryRsrq"),
           row.getAs[List[String]]("cptType")))
      if (data.size == 0) {
        data = List(
          (0,
           "0.0",
           List("0.0"),
           List("0.0"),
           List("0.0"),
           List("0.0"),
           List("0.0"),
           List("0.0"),
           List("0.0")))
      }

      def topN(n: Int,
               list: List[
                 (Int,
                  String,
                  List[String],
                  List[String],
                  List[String],
                  List[String],
                  List[String],
                  List[String],
                  List[String])]): List[(Int,
                                         String,
                                         List[String],
                                         List[String],
                                         List[String],
                                         List[String],
                                         List[String],
                                         List[String],
                                         List[String])] = {
        def update(l: List[
                     (Int,
                      String,
                      List[String],
                      List[String],
                      List[String],
                      List[String],
                      List[String],
                      List[String],
                      List[String])],
                   e: (Int,
                       String,
                       List[String],
                       List[String],
                       List[String],
                       List[String],
                       List[String],
                       List[String],
                       List[String])): List[(Int,
                                             String,
                                             List[String],
                                             List[String],
                                             List[String],
                                             List[String],
                                             List[String],
                                             List[String],
                                             List[String])] = {
          if (e._1 > l.head._1) (e :: l.tail).sortWith(_._1 < _._1) else l
        }

        var uniqueList = Collections.distinctBy(list)(_._1)
        uniqueList
          .drop(n)
          .foldLeft(uniqueList.take(n).sortWith(_._1 < _._1))(update)
      }

      if (data.size > 0) {
        topN(8, data)
      } else {
        data
      }

    }

    val mergeList8 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(
        row =>
          (row.getAs[Int]("freq"),
           row.getAs[Int]("ChannelNumber"),
           row.getAs[String]("Bandwidth"),
           row.getAs[List[String]]("cinr"),
           row.getAs[List[String]]("cellId"),
           row.getAs[List[String]]("primaryRsrp"),
           row.getAs[List[String]]("primaryRsrq")))
      if (data.size == 0) {
        data =
          List((0, 0, "0", List("0.0"), List("0.0"), List("0.0"), List("0.0")))
      }

      def topN(n: Int,
               list: List[(Int,
                           Int,
                           String,
                           List[String],
                           List[String],
                           List[String],
                           List[String])]): List[(Int,
                                                  Int,
                                                  String,
                                                  List[String],
                                                  List[String],
                                                  List[String],
                                                  List[String])] = {
        def update(l: List[(Int,
                            Int,
                            String,
                            List[String],
                            List[String],
                            List[String],
                            List[String])],
                   e: (Int,
                       Int,
                       String,
                       List[String],
                       List[String],
                       List[String],
                       List[String])): List[(Int,
                                             Int,
                                             String,
                                             List[String],
                                             List[String],
                                             List[String],
                                             List[String])] = {
          if (e._1 > l.head._1) (e :: l.tail).sortWith(_._1 < _._1) else l
        }

        var uniqueList = Collections.distinctBy(list)(_._1)
        uniqueList
          .drop(n)
          .foldLeft(uniqueList.take(n).sortWith(_._1 < _._1))(update)
      }

      if (data.size > 0) {
        topN(20, data)
      } else {
        data
      }

    }

    val mergeList8nrTopNSignal = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(row => (row.getAs[Int]("signalNumber"), row.getAs[Int]("freq"), row.getAs[Int]("channelNumber"),
        row.getAs[Int]("numCells"), row.getAs[Double]("ssbRssi"), row.getAs[String]("band"), row.getAs[Int]("subCarrierSpacing"),
        row.getAs[List[String]]("pssCinr"), row.getAs[List[String]]("pssRsrp"), row.getAs[List[String]]("pssRsrq"),
        row.getAs[List[String]]("sssCinr"), row.getAs[List[String]]("sssRsrp"), row.getAs[List[String]]("sssRsrq"),
        row.getAs[List[String]]("ssbCinr"), row.getAs[List[String]]("ssbRsrp"), row.getAs[List[String]]("ssbRsrq"),row.getAs[List[String]]("cellId"),row.getAs[List[String]]("beamIndex")))
      if (data.isEmpty) {
        data = List((0, 0, 0, 0, 0D, "0", 0, List("0.0"), List("0.0"), List("0.0"), List("0.0"), List("0.0"), List("0.0"), List("0.0"), List("0.0"), List("0.0"),List("0.0"),List("0.0")))
      }

      def topN(n: Int, list: List[(Int, Int, Int, Int, Double, String, Int, List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String],List[String],List[String])]) = {
        def update(l: List[(Int, Int, Int, Int, Double, String, Int, List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String],List[String],List[String])],
                   e: (Int, Int, Int, Int, Double, String, Int, List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String], List[String],List[String],List[String])) = {
          if (e._1 > l.head._1) (e :: l.tail).sortWith(_._1 < _._1) else l
        }
        //      logger.info(s"list>>>$list")
        list.drop(n).foldLeft(list.take(n).sortWith(_._1 < _._1))(update)
      }
      //      logger.info(s"data>>>$data")
      if (data.nonEmpty) {
        topN(8, data)
      } else {
        data
      }
    }

    val mergeList9 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(row => (row.getAs[List[String]]("slot_0xB890"), row.getAs[List[String]]("frame_0xB890"), row.getAs[List[String]]("cdrxEvent_0xB890"), row.getAs[List[String]]("prevState_0xB890")))
      if (data.size == 0) {
        data = List((List("0.0"), List("0.0"), List("0.0"), List("0.0")))
      }
      var slotCurrent = List[String]()
      var slotPrevious = List[String]()
      var sfnCurrent = List[String]()
      var sfnPrevious = List[String]()
      def topN(list: List[(List[String], List[String], List[String], List[String])]): List[(List[String], List[String], List[String], List[String])] = {
        var evt:List[String] = null
        if(data.head._3.contains("CDRX_ON_2_OFF")) {
          evt = data.head._3
        } else if(data.head._4.contains("INACTIVE")) {
          evt = data.head._4
        }
        if(evt !=null) {
          for ((elem, index) <- evt.zipWithIndex) {
              slotCurrent=slotCurrent:+data.head._1(index)
              slotPrevious=slotPrevious:+data.head._1(index-1)
              sfnCurrent=sfnCurrent:+data.head._2(index)
              sfnPrevious=sfnPrevious:+data.head._2(index-1)
          }
        }
        List((slotCurrent, slotPrevious, sfnCurrent, sfnPrevious))
      }
      if (data.size > 0) {
        topN(data)
      } else {
        data
      }
    }

    val mergeList10 = udf { (combo: WrappedArray[Seq[Row]]) =>
      var data = combo.toList.flatten.map(row => (row.getAs[Int]("centerFreq"), row.getAs[Int]("numSignals"), row.getAs[List[String]]("pci"), row.getAs[List[String]]("rsrp"), row.getAs[List[String]]("rsrq"), row.getAs[List[String]]("rscinr"), row.getAs[List[String]]("numberOfAntennas"),row.getAs[List[String]]("antennaRsrp"),row.getAs[List[String]]("antennaRsrq"),row.getAs[List[String]]("antennaCinr")))
      if (data.size == 0) {
        data = List((0, 0, List("0.0"),List("0.0"), List("0.0"), List("0.0"), List("0.0"),List("0.0"),List("0.0"),List("0.0")))
      }
      def topN(n: Int, list: List[(Int, Int, List[String], List[String],List[String],List[String], List[String], List[String], List[String],List[String])]): List[(Int, Int, List[String],List[String],List[String], List[String], List[String], List[String], List[String],List[String])] = {
        def update(l: List[(Int, Int, List[String],List[String], List[String], List[String], List[String],List[String],List[String],List[String])], e: (Int,Int,List[String],List[String], List[String],List[String], List[String], List[String], List[String],List[String])): List[(Int,Int, List[String],List[String], List[String], List[String], List[String],List[String], List[String],List[String])] = {
          if (e._1 > l.head._1) (e :: l.tail).sortWith(_._1 < _._1) else l
        }
        var uniqueList = Collections.distinctBy(list)(_._1)
        uniqueList.drop(n).foldLeft(uniqueList.take(n).sortWith(_._1 < _._1))(update)
      }
      if (data.size > 0) {
        topN(20, data)
      } else {
        data
      }

    }

    val dmatMillSecDataframe = spark.sql(dmatMilliSecQuery)
    dmatMillSecDataframe.createOrReplaceTempView("dmatMillSecDataframe")
    val windIp = Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)
    var rachSetupEvtsFinal = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    if(logcodeList.contains("0xB889") || logcodeList.contains("0xB88A")) {
      logger.info("rach kpis exists..........")
      kpiisRachExists = true
    val rachDf= dmatMillSecDataframe.filter("rachTriggered_0xb889 = true or rachAttempted_0xb88a = true or rachResult_0xb88a is not null").select("rachTriggered_0xb889","dmTimeStamp","fileName","RachReason","rachAttempted_0xb88a","rachResult_0xb88a").orderBy("dmTimeStamp")

    rachDf.createOrReplaceTempView("rachDf")

      val rachdfTemp = spark.sql("select *, rachResult_0xb88a, collect_list(RachReason) over (partition by fileName order by cast(dmtimestamp AS timestamp) asc range between current row and interval 2 seconds following) as collected_rachReason, " +
      "collect_list(rachTriggered_0xb889) over (partition by fileName order by cast(dmtimestamp AS timestamp) asc range between current row and interval 2 seconds following) as collected_rachTriggered, " +
      "collect_list(rachAttempted_0xb88a) over (partition by fileName order by cast(dmtimestamp AS timestamp) asc range between current row and interval 2 seconds following) as collected_rachAttempted, " +
      "collect_list(rachResult_0xb88a) over (partition by fileName order by cast(dmtimestamp AS timestamp) asc range between current row and interval 2 seconds following) as collected_rachResult from (select *, split(dmtimestamp, '[\\.]')[0] as dateWithoutMillis  from rachDf where rachResult_0xb88a = 'ABORTED' or rachResult_0xb88a = 'SUCCESS' or RachReason = 'CONNECTION_REQUEST')")

      var rachSetupEvtsTemp = rachdfTemp.withColumn("nrsetupattempt", when(col("rachTriggered_0xb889")==="true" && col("RachReason")==="CONNECTION_REQUEST", "1").otherwise(lit(null)))
        .withColumn("nrsetupsuccess", when(col("rachTriggered_0xb889") ==="true"  && col("RachReason")==="CONNECTION_REQUEST" && mergeList4(col("collected_rachAttempted")).contains("true")  && NrSetupSuccessUdf(mergeList4(col("collected_rachResult"))).contains("SUCCESS"), "1").otherwise(lit(null)))
        .withColumn("nrsetupfailure", when(col("rachTriggered_0xb889") ==="true"  && col("RachReason")==="CONNECTION_REQUEST" && mergeList4(col("collected_rachAttempted")).contains("true")  && NrSetupFailureUdf(mergeList4(col("collected_rachResult"))).contains("ABORTED"), "1").otherwise(lit(null)))
        .where($"nrsetupattempt" === 1 || $"nrsetupsuccess" === 1 || $"nrsetupfailure" === 1).select($"fileName",$"dateWithoutMillis",$"nrsetupattempt",$"nrsetupsuccess",$"nrsetupfailure",$"rachResult_0xb88a")
      .withColumn("nrsetupstatus", when($"nrsetupsuccess".isNotNull || $"nrsetupsuccess"=="1", lit("success")).otherwise(lit(null)))

      val rachSetupWindow=Window.partitionBy("fileName","dateWithoutMillis").orderBy($"dateWithoutMillis".asc)

      rachSetupEvtsFinal = rachSetupEvtsTemp.withColumn("Nrsetupattempt",last($"nrsetupattempt", true).over(rachSetupWindow))
        .withColumn("Nrsetupsuccess",last($"nrsetupsuccess", true).over(rachSetupWindow))
        .withColumn("Nrsetupfailure",last($"nrsetupfailure", true).over(rachSetupWindow))
        .withColumn("nrsetupsuccesscnt", when($"nrsetupsuccess".isNotNull ,count($"nrsetupsuccess").over(rachSetupWindow)).otherwise(lit(null)))
        .withColumn("nrsetupfailurecnt", when($"nrsetupfailure".isNotNull ,count($"nrsetupfailure").over(rachSetupWindow)).otherwise(lit(null)))
        .withColumn("nrsetupattemptcnt", when($"nrsetupattempt".isNotNull ,count($"nrsetupattempt").over(rachSetupWindow)).otherwise(lit(null)))
        .withColumn("NRRachFailureEvt", when(col("rachResult_0xb88a").contains("FAILURE"), 1).otherwise(lit(null)))
        .withColumn("NRRachAbortEvt", when(col("rachResult_0xb88a").contains("ABORTED"), 1).otherwise(lit(null)))
        .withColumn("rownum",row_number().over(rachSetupWindow))
        .filter("rownum = 1")

      dmatEvents += ($"Nrsetupfailure",$"NRRachFailureEvt",$"NRRachAbortEvt")

    val rachGroupDf=rachDf.withColumn("rachGroup",rachGroup(col("dmTimeStamp"),col("rachTriggered_0xb889")))

    val rachWindow=Window.partitionBy("fileName").orderBy($"dmTimeStamp".asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val rachFFDF=rachGroupDf.withColumn("rachffhash",last($"rachGroup",true).over(rachWindow))
    val rachResultsWithLatencyDF= rachFFDF.withColumn("rachLatency", timestamp_diff($"dmTimeStamp",$"rachffhash"))

    val rachResultLatencyDF = rachResultsWithLatencyDF.filter("rachTriggered_0xb889 = false").drop("rachTriggered_0xb889","rachGroup","dmTimeStamp","rachResult_0xb88a")

    rachResultLatencyDF.createOrReplaceTempView("rachlatency")

    val rachlatencyAvg = spark.sql("select *, split(rachffhash, '[\\.]')[0] as dateWithoutMillis from rachlatency")
    val rachLatencyAvgPerSecondDF=rachlatencyAvg.groupBy("dateWithoutMillis","fileName").agg(avg($"rachLatency").as("avgRachLatencyperSecond"))
      joinedCombo = joinedCombo.join(rachSetupEvtsFinal,Seq("fileName", "dateWithoutMillis"), "full_outer")

      joinedCombo = joinedCombo.join(rachLatencyAvgPerSecondDF, Seq("fileName", "dateWithoutMillis"), "left_outer")
      val beamRecoveryEvtsDfTemp = spark.sql("select dmtimestamp,fileName,NRBeamFailureEvt, rachResult_0xb88a,split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from dmatMillSecDataframe where (NRBeamFailureEvt = 1 or (rachResult_0xb88a = 'SUCCESS' or rachResult_0xb88a = 'ABORTED'))")
      val beamRecoveryEvtsDf = beamRecoveryEvtsDfTemp.withColumn("nrbeamFailureLag", lag("NRBeamFailureEvt", 1, null)
        .over(windIp)).filter($"nrbeamFailureLag"===1).select($"fileName",$"dateWithoutMillis",$"rachResult_0xb88a",$"nrbeamFailureLag")
      joinedCombo =   joinedCombo.join(beamRecoveryEvtsDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
    }
    val throughputWindow = Window.partitionBy($"fileName",$"dateWithoutMillis").orderBy($"dmtimestamp".asc)
//    if(logcodeList.contains("0xB868")) {
//      kpiisRlcUlInstTputExists = true
//      val totaltxbytes0xb868Query="""select totaltxbytes_0xb868,dmtimestamp,filename, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis
//        from dmatMillSecDataframe where logcode in ('47208')"""
//
//      val nr5grlculinstTputDf=spark.sql(totaltxbytes0xb868Query)
//
//      val nr5grlculinstTputDiffsDf=nr5grlculinstTputDf.withColumn("rownum",row_number().over(throughputWindow))
//        .filter("rownum = 1")
//        .withColumn("prevtotaltxbytes0xb868",lag($"totaltxbytes_0xb868",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp"))
//        .withColumn("prevdmtimestamp",lag($"dmtimestamp",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp"))
//        .withColumn("nr5grlculinstTput0xb868DiffSeconds",timestamp_diffseconds(col("dmtimestamp"),col("prevdmtimestamp")))
//        .withColumn("nr5grlculinstTput0xb868DiffTbs",col("totaltxbytes_0xb868") - col("prevtotaltxbytes0xb868"))
//        .drop("prevtotaltxbytes0xb868","prevdmtimestamp","totaltxbytes_0xb868","dmtimestamp","rownum")
//        .toDF("filename",  "dateWithoutMillis","nr5grlculinstTput0xb868DiffSeconds","nr5grlculinstTput0xb868DiffTbs")
//
//      joinedCombo = joinedCombo.join(nr5grlculinstTputDiffsDf,Seq("fileName", "dateWithoutMillis"), "left_outer")
//
//    }

//   if(logcodeList.contains("0xB860")) {
//     kpiPdcpUlInstTputExists = false
//     val totalNumRxBytes0xb860query="""select totalNumRxBytes_0xB860,dmtimestamp,filename, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis
//        from dmatMillSecDataframe where logcode in ('47200')"""
//
//     val nr5gNumRxBytes0xb860TputDf=spark.sql(totalNumRxBytes0xb860query).filter("totalNumRxBytes_0xB860 > 0")
//
//     val nr5gNumRxBytes0xb860TputDiffsDf=nr5gNumRxBytes0xb860TputDf.withColumn("rownum",row_number().over(throughputWindow))
//       .filter("rownum = 1")
//       .withColumn("prevtotalNumRxBytes0xB860",lag($"totalNumRxBytes_0xB860",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp") )
//       .withColumn("prevdmtimestamp", lag($"dmtimestamp",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp"))
//       .withColumn("nr5gRxBytes0xB860DiffSeconds",timestamp_diffseconds(col("dmtimestamp"),col("prevdmtimestamp")))
//       .withColumn("nr5gRxBytes0xB860DiffTbs",col("totalNumRxBytes_0xB860") - col("prevtotalNumRxBytes0xB860"))
//       .drop("prevtotalNumRxBytes0xB860","prevdmtimestamp","totalNumRxBytes_0xB860","rownum","dmtimestamp")
//       .toDF("filename","dateWithoutMillis","nr5gRxBytes0xB860DiffSeconds","nr5gRxBytes0xB860DiffTbs")
//
//     joinedCombo = joinedCombo.join(nr5gNumRxBytes0xb860TputDiffsDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
//   }

//    if(logcodeList.contains("0xB84D")) {
//      kpiRlcInstTputExists = true
//      val rlcDataBytes0xB84Dquery="""select rlcDataBytesVal_0xB84D,dmtimestamp,filename,split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis
//        from dmatMillSecDataframe where logcode in ('47181')"""
//
//      val rlcDataBytes0xB84DDF=spark.sql(rlcDataBytes0xB84Dquery)
//
//      val rlcDataBytes0xB84DDiffsDf=rlcDataBytes0xB84DDF.filter("rlcDataBytesVal_0xB84D>0")
//        .withColumn("rownum",row_number().over(throughputWindow))
//        .filter("rownum = 1")
//        .withColumn("prevrlcDataBytesVal_0xB84D",last($"rlcDataBytesVal_0xB84D",true) over Window.partitionBy($"filename").orderBy($"dmtimestamp").rowsBetween(Window.unboundedPreceding,-1))
//        .withColumn("prevdmtimestamp",last($"dmtimestamp",true) over Window.partitionBy($"filename").orderBy($"dmtimestamp").rowsBetween(Window.unboundedPreceding,-1))
//        .withColumn("nr5gRlcDataBytes0xB84DDiffSeconds",timestamp_diffseconds(col("dmtimestamp"),col("prevdmtimestamp")))
//        .withColumn("nr5gRlcDataBytes0xB84DDiff",col("rlcDataBytesVal_0xB84D") - col("prevrlcDataBytesVal_0xB84D"))
//        .drop("rownum","prevrlcDataBytesVal_0xB84D","prevdmtimestamp")
//
//      joinedCombo = joinedCombo.join(rlcDataBytes0xB84DDiffsDf,Seq("fileName", "dateWithoutMillis"), "left_outer")
//    }

//    if(logcodeList.contains("0xB842")) {
//      //dataPDUBytesReceivedAllRBSum_0xB842,controlPDUBytesReceivedAllRBSum_0xB842
//      //nr5GPdcpDl0xB842Query
//      kpiPdcpDlExists = true
//      val nr5GPdcpDl0xB842Query="""select dataPDUBytesReceivedAllRBSum_0xB842,controlPDUBytesReceivedAllRBSum_0xB842,dmtimestamp,filename, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis
//      from dmatMillSecDataframe where logcode in ('47170')"""
//
//      val nr5GPdcpDl0xB842DF = spark.sql(nr5GPdcpDl0xB842Query)
//
//      val nr5GPdcpDl0xB842DiffsDF = nr5GPdcpDl0xB842DF.withColumn("rownum",row_number().over(throughputWindow))
//        .filter("rownum = 1")
//        .withColumn("prevdataPDUBytesReceivedAllRBSum_0xB842", lag($"dataPDUBytesReceivedAllRBSum_0xB842",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp"))
//        .withColumn("prevcontrolPDUBytesReceivedAllRBSum_0xB842", lag($"controlPDUBytesReceivedAllRBSum_0xB842",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp"))
//        .withColumn("prevdmtimestamp", lag($"dmtimestamp",1,null) over Window.partitionBy($"filename").orderBy($"dmtimestamp") )
//        .withColumn("nr5GPdcpDl0xB842DiffSeconds",timestamp_diffseconds(col("dmtimestamp"),col("prevdmtimestamp")))
//        .withColumn("nr5gPdcpDl0xB842DataDiff",(col("dataPDUBytesReceivedAllRBSum_0xB842") + col("controlPDUBytesReceivedAllRBSum_0xB842")) -
//          (col("prevdataPDUBytesReceivedAllRBSum_0xB842") + col("prevcontrolPDUBytesReceivedAllRBSum_0xB842")))
//        .drop("prevdataPDUBytesReceivedAllRBSum_0xB842","prevcontrolPDUBytesReceivedAllRBSum_0xB842","prevdmtimestamp","dataPDUBytesReceivedAllRBSum_0xB842","controlPDUBytesReceivedAllRBSum_0xB842","dmtimestamp","rownum")
//        .toDF("filename",  "dateWithoutMillis","nr5GPdcpDl0xB842DiffSeconds","nr5gPdcpDl0xB842DataDiff")
//
//      joinedCombo = joinedCombo.join(nr5GPdcpDl0xB842DiffsDF,Seq("fileName", "dateWithoutMillis"), "left_outer")
//    }

    val dmatB192_B193 = spark.sql(
      "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis," + B192Query.query + "," + B193Query.query + " from (select *  " +
        " from dmatMillSecDataframe sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")

    var sig_logcodes_Final: DataFrame = null
    var sig0x2406logcode_Final: DataFrame = null
    var sig0x240blogcode_Final: DataFrame = null
    var siglogcodesUnionDf: DataFrame = null
    var sig0x2501logcode_Final:DataFrame = null
    var sig0x240Clogcode_Final:DataFrame = null

      /*val sigLogcodesDf = spark.sql("select * from (select fileName,dmTimeStamp, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis, " +  SigQuerySingle.query +
        " from (select * from " + hiveTableName + " where insertedDate in (" + datesStr + ") and filename in (" + fileNamesStr + ") and testId in (" + testIds + ") and logRecordName = 'SIG'))")*/

      val sigLogcodesDf = spark.sql(
        "select * from (select fileName,dmTimeStamp, dateWithoutMillis, " + SigQuerySingle.query + " from fileDataFrame where logRecordName = 'SIG')")

      sigLogcodesDf.createOrReplaceTempView("sigLogcodesDf")
      val sig_logcodes = spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis," + SigQuery.query + " from (select *  " +
          " from sigLogcodesDf where hexLogCode in ('0x240B','0x2406','0x2408','0x240D','0x2501','0x2157','0x240C') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis,frequency0x240D_sig,frequency0x2408_sig")

      val sig0x2406logcode = spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis,collect_list(freqKpiList0x2406) as collected_freqKpiList0x2406 from (select *  " +
          " from sigLogcodesDf where hexLogCode in ('0x2406') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")

      val sig0x240blogcode = spark.sql(
        "select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis,collect_list(freqKpiList0x240b) as collected_freqKpiList0x240b from (select *  " +
          " from sigLogcodesDf where hexLogCode in ('0x240B') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")

      val sig0x240Clogcode = spark.sql("select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis,collect_list(pciKpi0x240C_SIG) as collected_pciKpiList0x240C from (select *  " +
        " from sigLogcodesDf where hexLogCode in ('0x240C') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")

      sig0x2501logcode_Final = spark.sql("select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis," + SigQuery.query0x2501 + " from (select *  " +
        " from sigLogcodesDf where hexLogCode in ('0x2501') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis,channelNumber0x2501_sig")


      sig_logcodes_Final = sig_logcodes.withColumn("CINR0x2408_collectedSig", mergeList3($"collected_CINR0x2408_sig"))
        .withColumn("cellId0x2408_collectedSig", mergeList3($"collected_cellId0x2408_sig"))
        .withColumn("totalRsPower0x2408_collectedSig", mergeList3($"collected_totalRsPower0x2408_sig"))
        .withColumn("overallPower0x2408_collectedSig", mergeList3($"collected_overallPower0x2408_sig"))
        .withColumn("overallQuality0x2408_collectedSig", mergeList3($"collected_overallQuality0x2408_sig"))
        .withColumn("cpType0x2408_collectedSig", mergeList3($"collected_cpType0x2408_sig"))
        .withColumn("antQuality0x2408_collectedSig", mergeList3($"collected_antQuality0x2408_sig"))
        .withColumn("antPower0x2408_collectedSig", mergeList3($"collected_antPower0x2408_sig"))
        .withColumn("numberOfTxAntennaPorts0x2408_collectedSig", mergeList3($"collected_numberOfTxAntennaPorts0x2408_sig"))
        .withColumn("cellId0x240D_collectedSig", mergeList3($"collected_pci0x240D_sig"))
        .withColumn("rsrp0x240D_collectedSig", mergeList3($"collected_Rsrp0x240D_sig"))
        .withColumn("rsrq0x240D_collectedSig", mergeList3($"collected_Rsrq0x240D_sig"))
        .withColumn("CINR0x240D_collectedSig", mergeList3($"collected_CINR0x240D_sig"))
        .withColumn("numberOfTxAntennaPorts0x240D_collectedSig", mergeList3($"collected_numberOfTxAntennaPorts0x240D_sig"))
        .withColumn("freqRsrp0x2157", mergeList5v2($"collected_frequencyRSRP0x2157_SIG"))
        .withColumn("freqRsrp0x2157_topPairs", explode(mergeList5($"collected_frequencyRSRP0x2157_SIG")))
        .selectExpr("*", "freqRsrp0x2157_topPairs._1 as freq0x2157")

        .drop("Latitude", "Longitude","collected_frequencyRSRP0x2157_SIG", "freqRsrp0x2157_topPairs")


      sig0x2406logcode_Final = sig0x2406logcode.withColumn("freqComboKpis0x2406", explode(mergeList7($"collected_freqKpiList0x2406"))).selectExpr("*","freqComboKpis0x2406._1 as freq0x2406","freqComboKpis0x2406._2 as channelNumber0x2406","freqComboKpis0x2406._3 as cinr0x2406","freqComboKpis0x2406._4 as cellId0x2406",
          "freqComboKpis0x2406._5 as primaryRsrp0x2406","freqComboKpis0x2406._6 as primaryRsrq0x2406","freqComboKpis0x2406._7 as secondaryRsrp0x2406","freqComboKpis0x2406._8 as secondaryRsrq0x2406","freqComboKpis0x2406._9 as cptType0x2406").drop("freqComboKpis0x2406","collected_freqKpiList0x2406")

      sig0x240blogcode_Final = sig0x240blogcode.withColumn("freqComboKpis0x240b", explode(mergeList8($"collected_freqKpiList0x240b"))).selectExpr("*","freqComboKpis0x240b._1 as freq0x240b","freqComboKpis0x240b._2 as channelNumber0x240b","freqComboKpis0x240b._3 as bandwidth0x240b","freqComboKpis0x240b._4 as cinr0x240b","freqComboKpis0x240b._5 as cellId0x240b",
        "freqComboKpis0x240b._6 as primaryRsrp0x240b","freqComboKpis0x240b._7 as primaryRsrq0x240b").drop("freqComboKpis0x240b","collected_freqKpiList0x240b")

      sig0x240Clogcode_Final = sig0x240Clogcode.withColumn("pciComboKpis0x240c", explode(mergeList10($"collected_pciKpiList0x240C"))).selectExpr("*","pciComboKpis0x240c._1 as centerFreq0x240c","pciComboKpis0x240c._2 as numSignals0x240c","pciComboKpis0x240c._3 as pci0x240c","pciComboKpis0x240c._4 as rsrp0x240c","pciComboKpis0x240c._5 as rsrq0x240c",
        "pciComboKpis0x240c._6 as rscinr0x240c","pciComboKpis0x240c._7 as numberOfAntennas0x240c","pciComboKpis0x240c._8 as antennaRsrp0x240c","pciComboKpis0x240c._9 as antennaRsrq0x240c","pciComboKpis0x240c._10 as antennaCinr0x240c").drop("pciComboKpis0x240c","collected_pciKpiList0x240C")

      val cols1 = sig0x2406logcode_Final.columns.toSet
      val cols2 = sig0x240blogcode_Final.columns.toSet
      val totalCols = cols1 ++ cols2

      def expr(myCols: Set[String], allCols: Set[String]) = {
        allCols.toList.map(x =>
          x match {
            case x if myCols.contains(x) => col(x)
            case _                       => lit(null).as(x)
        })
      }

      siglogcodesUnionDf = sig0x2406logcode_Final
        .select(expr(cols1, totalCols): _*)
        .union(sig0x240blogcode_Final.select(expr(cols2, totalCols): _*))

//      firstLastLocationRecordDf = sigLogcodesDf
//        .select($"fileName",
//                $"dateWithoutMillis",
//                $"Longitude_sig",
//                $"Latitude_sig",
//                $"rn")
    //        .withColumn("rnFR", row_number.over(windowFirstLocationRecord))
    //        .withColumn("rnLR", row_number.over(windowLastLocationRecord))
    //        .select($"fileName",
    //                $"dateWithoutMillis",
    //                $"Longitude_sig",
    //                $"Latitude_sig",
    //                $"rnFR",
    //                $"rnLR",
    //                $"rn")
    //        .where($"rnFR" === 1 || $"rnLR" === 1 || $"rn" === 1)

    var traceScannerMsgFinal: DataFrame = null
    var NRTopNSignalKPIs: DataFrame = null
    var autotestMsgDf: DataFrame = null

    if (logcodeList.contains("0xFFF4") || logcodeList.contains("0xFFF3")) {
      val traceScannerMsgDf = spark.sql("select fileName,max(dmTimeStamp) as dmTimeStamp, dateWithoutMillis, " +
        "collect_list(lteTopNSignalkpiList) as collected_lteTopNSignal, " +
        "collect_list(nrTopNSignalKpiList) as collected_nrTopNSignal, " +
        "collect_list(lteBlindScanKpiList) as collected_lteBlindScan, " +
        "collect_list(nrBlindScanKpiList) as collected_nrBlindScan, " +
        "last_value(hexLogCode, true) as tracelogcodes " +
        "from (select * from fileDataFrame where hexLogCode in ('0xFFF4') sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis")
      //      logger.info(s"traceScannerMsgDf = ${traceScannerMsgDf.show(20, false)}")

      traceScannerMsgFinal = traceScannerMsgDf.withColumn("freqComboKpisLTETopNSignal", explode(mergeList8($"collected_lteTopNSignal"))).selectExpr("*", "freqComboKpisLTETopNSignal._1 as freqLTETopNSig", "freqComboKpisLTETopNSignal._2 as channelNumberLTETopNSig", "freqComboKpisLTETopNSignal._3 as bandwidthLTETopNSig", "freqComboKpisLTETopNSignal._4 as cinrLTETopNSig", "freqComboKpisLTETopNSignal._5 as cellIdLTETopNSig",
        "freqComboKpisLTETopNSignal._6 as primaryRsrpLTETopNSig", "freqComboKpisLTETopNSignal._7 as primaryRsrqLTETopNSig").drop("freqComboKpisLTETopNSignal", "collected_lteTopNSignal")
        .withColumn("freqComboKpisLTEBlindScan", explode(mergeList8($"collected_lteBlindScan"))).selectExpr("*", "freqComboKpisLTEBlindScan._1 as freqLTEBlindScan", "freqComboKpisLTEBlindScan._2 as channelNumberLTEBlindScan", "freqComboKpisLTEBlindScan._3 as bandwidthLTEBlindScan", "freqComboKpisLTEBlindScan._4 as cinrLTEBlindScan", "freqComboKpisLTEBlindScan._5 as cellIdLTEBlindScan",
        "freqComboKpisLTEBlindScan._6 as primaryRsrpLTEBlindScan", "freqComboKpisLTEBlindScan._7 as primaryRsrqLTEBlindScan").drop("freqComboKpisLTEBlindScan", "collected_lteBlindScan")
        .withColumn("freqComboKpisNRBlindScan", explode(mergeList8($"collected_nrBlindScan"))).selectExpr("*", "freqComboKpisNRBlindScan._1 as freqNRBlindScan", "freqComboKpisNRBlindScan._2 as channelNumberNRBlindScan", "freqComboKpisNRBlindScan._3 as bandwidthNRBlindScan", "freqComboKpisNRBlindScan._4 as cinrNRBlindScan", "freqComboKpisNRBlindScan._5 as cellIdNRBlindScan",
        "freqComboKpisNRBlindScan._6 as primaryRsrpNRBlindScan", "freqComboKpisNRBlindScan._7 as primaryRsrqNRBlindScan").drop("freqComboKpisNRBlindScan", "collected_nrBlindScan")

      NRTopNSignalKPIs =  traceScannerMsgDf.withColumn("freqComboKpisNRTopNSignal", explode(mergeList8nrTopNSignal($"collected_nrTopNSignal")))
        .selectExpr("*", "freqComboKpisNRTopNSignal._1 as NRTopNSignal", "freqComboKpisNRTopNSignal._2 as freqNRTopNSignal",
          "freqComboKpisNRTopNSignal._3 as channelNumberNRTopNSignal", "freqComboKpisNRTopNSignal._4 as numCellsNRTopNSignal",
          "freqComboKpisNRTopNSignal._5 as ssbRssiNRTopNSignal", "freqComboKpisNRTopNSignal._6 as bandNRTopNSignal",
          "freqComboKpisNRTopNSignal._7 as subCarrierSpacingNRTopNSignal",
          "freqComboKpisNRTopNSignal._8 as pssCinrNRTopNSignal", "freqComboKpisNRTopNSignal._9 as pssRsrpNRTopNSignal", "freqComboKpisNRTopNSignal._10 as pssRsrqNRTopNSignal",
          "freqComboKpisNRTopNSignal._11 as sssCinrNRTopNSignal", "freqComboKpisNRTopNSignal._12 as sssRsrpNRTopNSignal", "freqComboKpisNRTopNSignal._13 as sssRsrqNRTopNSignal",
          "freqComboKpisNRTopNSignal._14 as ssbCinrNRTopNSignal", "freqComboKpisNRTopNSignal._15 as ssbRsrpNRTopNSignal", "freqComboKpisNRTopNSignal._16 as ssbRsrqNRTopNSignal",
          "freqComboKpisNRTopNSignal._17 as cellIdNRTopNSignal", "freqComboKpisNRTopNSignal._18 as beamIndexNRTopNSignal")
        .drop("freqComboKpisNRTopNSignal", "collected_NRTopNSignal")
        .groupBy($"fileName",$"dateWithoutMillis",$"freqNRTopNSignal")
        .agg(last("channelNumberNRTopNSignal",true).alias("channelNumber_NRTopNSignal"),last("numCellsNRTopNSignal",true).alias("numCells_NRTopNSignal"),
          last("ssbRssiNRTopNSignal",true).alias("ssbRssi_NRTopNSignal") ,last("bandNRTopNSignal",true).alias("band_NRTopNSignal"),
          last("subCarrierSpacingNRTopNSignal",true).alias("subCarrierSpacing_NRTopNSignal"),
          concat_ws(";",collect_list(mergeList4($"pssCinrNRTopNSignal"))).alias("psscinr_nrTopNSignal"),
          concat_ws(";",collect_list($"NRTopNSignal")).alias("nr_topnSignal"),
          concat_ws(";",collect_list(mergeList4($"pssRsrpNRTopNSignal"))).alias("pssrsrpnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"pssRsrqNRTopNSignal"))).alias("pssrsrqnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"sssCinrNRTopNSignal"))).alias("ssscinrnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"sssRsrpNRTopNSignal"))).alias("sssrsrpnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"sssRsrqNRTopNSignal"))).alias("sssrsrqnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"ssbCinrNRTopNSignal"))).alias("ssbcinrnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"ssbRsrpNRTopNSignal"))).alias("ssbrsrpnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"ssbRsrqNRTopNSignal"))).alias("ssbrsrqnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"cellIdNRTopNSignal"))).alias("cellIdnr_TopNSignal"),
          concat_ws(";",collect_list(mergeList4($"beamIndexNRTopNSignal"))).alias("beamIndexnr_TopNSignal"))

      autotestMsgDf = spark.sql("select fileName,dmTimeStamp, dateWithoutMillis,nr_rsrq,nr_rsrp,nr_rssnr,lte_rsrq,lte_rsrp,lte_rssnr,lte_enbId,cellId,startTime,testName,resultType,avg_nr_rsrq,avg_nr_rsrp,avg_nr_rssnr,avg_lte_rsrq,avg_lte_rsrp,avg_lte_rssnr, " +
        "task_time_median,task_time_05p,task_time_95p,access_success,task_success,ttc_median,ttc_05p,ttc_95p,iteration_id,seq_id,pingRtt,downloadTput,uploadTput,hexLogCode as logcodes " +
        "from fileDataFrame where hexLogCode in ('0xFFF3') and testName is not null")
    }


    var dmatB192_B193NeighboringPciDist: DataFrame =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val dmatB192_B193merged = dmatB192_B193
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

    joinedCombo = joinedCombo.join(dmatB192_B193merged,
                                   Seq("fileName", "dateWithoutMillis"),
                                   "full_outer")

    dmatB192_B193NeighboringPciDist = dmatB192_B193merged
      .select($"fileName",
              $"collected_pci",
              $"collectedB193_pci",
              $"collectedQcomm_lteeNBId")
      .groupBy("fileName")
      .agg(
        flatten(collect_list($"collected_pci")).alias("NeighboringPCI"),
        flatten(collect_list($"collectedB193_pci")).alias("ServingPCI"),
        flatten1(collect_list($"collectedQcomm_lteeNBId"))
          .alias("Serving_lteeNBId")
      )

    val lteWcdmaGsmCdmaSrvCnt = spark.sql(
      "select fileName, dateWithoutMillis, SUM(IF(((stk_SYS_MODE0_0x184E = 9 and stk_SRV_STATUS0x184E = 2) or (stk_SYS_MODE = 9 and stk_SRV_STATUS = 2)),1,0)) " +
        "as lteNormalCount, SUM(IF(((stk_SYS_MODE0_0x184E = 5 and stk_SRV_STATUS0x184E = 2) or (stk_SYS_MODE = 5 and stk_SRV_STATUS = 2)),1,0)) as wcdmaNormalCount, SUM(IF(((stk_SYS_MODE0_0x184E = " +
        "3 and stk_SRV_STATUS0x184E = 2) or (stk_SYS_MODE = 3 and stk_SRV_STATUS = 2)),1,0)) as gsmNormalCount, SUM(IF(((stk_SYS_MODE0_0x184E = 2 and stk_SRV_STATUS0x184E = 2) or (stk_SYS_MODE = 2 " +
        "and stk_SRV_STATUS = 2)),1,0)) as cdmaNormalCount, SUM(IF(((stk_SYS_MODE0_0x184E = 0 and stk_SRV_STATUS0x184E = 0) or (stk_SYS_MODE = 0 and stk_SRV_STATUS = 0)),1,0)) as noSrvCount from " +
        "dmatMillSecDataframe group by fileName, dateWithoutMillis")

    joinedCombo = joinedCombo.join(lteWcdmaGsmCdmaSrvCnt,
                                   Seq("fileName", "dateWithoutMillis"),
                                   "full_outer")

    /** ************************************************************************************************************************************************************************* */

    var finalRtpGapDf: DataFrame =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    var dmatIpDfOneSecAggDFCached:DataFrame = null
    if(logcodeList.contains("0xFFF0")) {
//      println("0xFFF0")
      kpiDmatIpExists = true
      val dmatIpQuery = spark.sql(
        "select * from (select fileName,dmTimeStamp,dateWithoutMillis, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp asc) as rn  " +
          "from (select * from dmatMillSecDataframe where (VoLTETriggerEvt=1 or VoLTECallEndEvt=1))) where rn=1")

      val timeStampLeadDf = dmatIpQuery.withColumn(
        "callEndTimeStamp",
        when(
          lead("VoLTECallEndEvt", 1, null).over(windIp).isNotNull,
          lead("dateWithoutMillis", 1, null).over(windIp)).otherwise(lit(null)))

      val minDf = timeStampLeadDf
        .select($"dateWithoutMillis" as "rtpTimeStamp",
          $"callEndTimeStamp",
          $"fileName")
        .filter($"callEndTimeStamp".isNotNull)
        .groupBy($"fileName", $"rtpTimeStamp")
        .agg(min($"callEndTimeStamp"))

      val rtpTimeStampRange = minDf.select($"fileName", $"rtpTimeStamp", $"min(callEndTimeStamp)")

      val dmatIp = spark.sql(
        "select * from (select fileName,dmTimeStamp,dateWithoutMillis, VoLTETriggerEvt, VoLTECallEndEvt, rtpDlSn, row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp asc) as rn  " +
          "from (select * from dmatMillSecDataframe)) where rn=1")

      val FinalRtpLossDf = dmatIp.join(rtpTimeStampRange, (dmatIp.col("fileName") === rtpTimeStampRange.col("fileName") && dmatIp.col("dateWithoutMillis") >= rtpTimeStampRange.col("rtpTimeStamp") && dmatIp.col("dateWithoutMillis") <= rtpTimeStampRange.col("min(callEndTimeStamp)"))).drop(rtpTimeStampRange.col("fileName"))

      val rtpGapJoinDf = FinalRtpLossDf.withColumn("rtpGap", when($"rtpDlSn".isNotNull, lit("false")).otherwise(lit("true"))).withColumn("rn", row_number().over(Window.partitionBy($"fileName", $"dateWithoutMillis").orderBy($"dmTimeStamp")))
      finalRtpGapDf = rtpGapJoinDf.filter($"rn" === 1).select($"dmTimeStamp", $"fileName", $"dateWithoutMillis", $"VoLTETriggerEvt" as "VoLTETriggerEvtRtpGap", $"VoLTECallEndEvt" as "VoLTECallEndEvtRtpGap", $"rtpGap")

      joinedCombo = joinedCombo.join(finalRtpGapDf,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")

      //      val dmatIPDS = spark.sql("select * from (select fileName, dmTimeStamp, " + IpQuery.getIpQuery(false) + " row_number() over(partition by filename, dateWithoutMillis order by dmTimeStamp desc) as rn, dateWithoutMillis " +
      //        "from (select * from dmatMillSecDataframe where (voLTEDropEvt=1 or voLTEAbortEvt=1 or voLTETriggerEvt=1 or voLTECallEndEvt=1 or voLTECallNormalRelEvt=1 or NrRelease=1))) where rn=1")
      //
      //      joinedCombo = joinedCombo.join(dmatIPDS, Seq("fileName", "dateWithoutMillis"), "full_outer")

      /** ************************************************************************************************************************************************************************* */
      val udfDateShiftBefore =
        udf[java.sql.Timestamp, java.sql.Timestamp](dateShiftBefore)
      val udfDateShiftAfter =
        udf[java.sql.Timestamp, java.sql.Timestamp](dateShiftAfter)

      val dmatIpDF = spark.sql(
        "select dmTimeStamp, index, logCode, hexLogCode, version, rtpDirection, rtpDlSn, rtpRxTimeStamp, exceptionOccured, exceptionCause, fileName, insertedDate,dateWithoutMillis from dmatMillSecDataframe where rtpdlsn is not null")

      //1. For each second find Min SN, Max SN, Total received packets count and, Missing packet count(DeltaCount)
      val rtpPacketCounts1SecDF = dmatIpDF
        .groupBy("fileName", "dateWithoutMillis")
        .agg(
          min("rtpdlsn") as "rtpDlsnAggMin",
          max("rtpdlsn") as "rtpDlsnAggMax",
          count($"rtpdlsn") as "rtpDlsnAggCount",
          ((max("rtpdlsn") - min("rtpdlsn")) + 1) - count($"rtpdlsn") as "rtpdlsnCountAgg"
        )
        .orderBy($"dateWithoutMillis".asc)

      val dmatIpDfOneSecAggDF = spark.sql("select rtpDlSn, fileName, dateWithoutMillis from dmatMillSecDataframe where rtpdlsn is not null")
        .join(rtpPacketCounts1SecDF, Seq("fileName", "dateWithoutMillis"), "left_outer")
        .select(
          $"fileName" as "file_name",
          $"dateWithoutMillis" as "r_date",
          $"rtpdlsn" as "r_rtpdlsn",
          $"rtpDlsnAggMin" as "rtpDlsnAggMin",
          $"rtpDlsnAggMax" as "rtpDlsnAggMax",
          $"rtpDlsnAggCount" as "rtpDlsnAggCount"
        ).filter($"r_rtpdlsn" >= $"rtpDlsnAggMin").filter($"r_rtpdlsn" <= $"rtpDlsnAggMax")

      //cache will prevent DF recomputation in case of timeout during broadcast
      dmatIpDfOneSecAggDFCached =
        if (commonConfigParams.KPI_RTP_PACKET_CACHE) {
          println("cache dmatIpDfOneSecAggDF")
          dmatIpDfOneSecAggDF.cache()
        } else dmatIpDfOneSecAggDF

      //2. Collect all the SN which is between Min SN and Max SN where 1SecDeltaSN > 0 from the SNs fpr the period of + or - 3 Seconds
      val rtpPacketCounts3SecDf = rtpPacketCounts1SecDF
        .filter($"rtpdlsnCountAgg" > 0)
        .select($"fileName", $"dateWithoutMillis")
        .join(
          if (commonConfigParams.KPI_RTP_PACKET_BROADCAST) {
            println("force broadcast dmatIpDfOneSecAggDF")
            println("dmatIpDfOneSecAggDF size:" + SizeEstimator.estimate(dmatIpDfOneSecAggDFCached))
            broadcast(dmatIpDfOneSecAggDFCached)
          } else {
            dmatIpDfOneSecAggDFCached
          },
          $"file_name" === $"fileName" and
            $"r_date".cast("timestamp") > $"dateWithoutMillis".cast("timestamp") - org.apache.spark.sql.functions.expr("INTERVAL 3 seconds") and
            $"r_date".cast("timestamp") < $"dateWithoutMillis".cast("timestamp") + org.apache.spark.sql.functions.expr("INTERVAL 3 seconds"))
        .drop("file_name")
        .groupBy($"fileName", $"dateWithoutMillis")
        .agg((max($"r_rtpdlsn") - min($"r_rtpdlsn") + 1 - count($"r_rtpdlsn")) as "rtpPacketLoss_3Seconds")

      val rtpPacketsLoss = rtpPacketCounts1SecDF.join(rtpPacketCounts3SecDf,
        Seq("fileName", "dateWithoutMillis"), "full_outer")

      if (commonConfigParams.KPI_RTP_PACKET) {
        println("join rtpPacketsLoss")
        joinedCombo = joinedCombo.join(rtpPacketsLoss,
          Seq("fileName", "dateWithoutMillis"),
          "full_outer")
      }

      var rtpRxIncomDelay: DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      var rtpRxOutDelay: DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      var rtpRxDelay: DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

      var RtpRxDelayCalDf = spark.sql(
        "select * from (select fileName, rtpDirection, dateWithoutMillis, last_value(dmTimeStamp) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc) as arrivalTimeInSec, " +
          "lag(last_value(dmTimeStamp) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc),1) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc) as lastArrivalTimeInSec, " +
          "last_value(rtpRxTimeStamp) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc) as rtpRxTimeInSec, " +
          "lag(last_value(rtpRxTimeStamp) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc),1) over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp asc) as lastrtpRxTimeInSec, " +
          "row_number() over(partition by fileName, SSRCIDHex, dateWithoutMillis order by dmTimeStamp desc) as rn from (select fileName, rtpDirection, dmTimeStamp, SSRCIDHex, rtpRxTimeStamp, " +
          "split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from dmatMillSecDataframe where (rtpDirection = 'INCOMING' or rtpDirection = 'OUTGOING'))) where rn =1")

      var RtpRxDelayCalTempDf =
        RtpRxDelayCalDf.filter($"lastArrivalTimeInSec".isNotNull)

      rtpRxIncomDelay =
        RtpRxDelayCalTempDf
          .filter($"rtpDirection" === "INCOMING")
          .withColumn("RtpRxDeltaDelayInc",
            (timestamp_diff($"arrivalTimeInSec",
              $"lastArrivalTimeInSec") - timestamp_diff(
              $"rtpRxTimeInSec",
              $"lastrtpRxTimeInSec") / 16000) * 1000)
          .withColumn(
            "RtpRxDelayInc",
            (timestamp_diff($"arrivalTimeInSec", $"lastArrivalTimeInSec") * 1000))

      rtpRxOutDelay =
        RtpRxDelayCalTempDf
          .filter($"rtpDirection" === "OUTGOING")
          .withColumn("RtpRxDeltaDelayOut",
            (timestamp_diff($"arrivalTimeInSec",
              $"lastArrivalTimeInSec") - timestamp_diff(
              $"rtpRxTimeInSec",
              $"lastrtpRxTimeInSec") / 16000) * 1000)
          .withColumn(
            "RtpRxDelayOut",
            (timestamp_diff($"arrivalTimeInSec", $"lastArrivalTimeInSec") * 1000))

      rtpRxDelay = rtpRxIncomDelay.join(rtpRxOutDelay, Seq("fileName", "dateWithoutMillis"), "inner")

      joinedCombo = joinedCombo.join(rtpRxDelay, Seq("fileName", "dateWithoutMillis"), "full_outer")
    }

    /** **************************************************************************Inbuilding checks********************************************************************** */
    if (!logsTblDs.filter(logsTblDs("isinbuilding_stat") === "TRUE").isEmpty) {
      kpiisinbuilding = true
      val inbuilding = spark.sql(
        "select fileName, dmTimeStamp,dateWithoutMillis, inbldImgId, inbldX, inbldY, inbldUserMark, inbldAccuracy, inbldAltitude " +
          "from dmatMillSecDataframe where (inbldUserMark = 'true' or inbldUserMark = 'false')")

      val windInbuilding =
        Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)

      val inbuildingAlt = inbuilding
        .join(
          inbuilding
            .groupBy("dateWithoutMillis")
            .agg($"dateWithoutMillis", max("inbldUserMark") as "inbldUserMark"),
          Seq("dateWithoutMillis", "inbldUserMark"))
        .withColumn("rn", row_number.over(windInbuilding))

      val inbuildingDer = inbuildingAlt
        .filter($"inbldUserMark" === "true")
        .withColumn("rn1", $"rn")
        .withColumn("inbldX1", lead("inbldX", 1, 0).over(windInbuilding))
        .withColumn("inbldY1", lead("inbldY", 1, 0).over(windInbuilding))
        .withColumn("rn2", lead("rn", 1, 999999).over(windInbuilding))

      inbuildingAlt.createOrReplaceTempView("inbuildingAlt")
      inbuildingDer.createOrReplaceTempView("inbuildingDer")

      var finalInbuildingDS = spark.sql(
        "select a.fileName, a.dmTimeStamp, a.dateWithoutMillis, a.inbldImgId, a.inbldAccuracy, a.inbldAltitude, b.inbldX, b.inbldX1, b.inbldY, b.inbldY1, " +
          "a.inbldUserMark, b.rn1, b.rn2, a.rn, b.inbldX+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldX1 - b.inbldX)) as interpolated_x, " +
          "b.inbldY+(((a.rn - b.rn1)/(b.rn2 - b.rn1))*(b.inbldY1 - b.inbldY)) as interpolated_y  from inbuildingAlt a join inbuildingDer b " +
          "on a.fileName = b.fileName and (a.rn >= b.rn and a.rn < b.rn2)")

      logger.info("Inbuilding File(s) Found.. Fetching Image Height from DB...")
      val imgIds = finalInbuildingDS
        .filter($"inbldImgId" =!= "")
        .groupBy("inbldImgId")
        .agg(max("inbldImgId"))
        .select("inbldImgId")
        .collect()
        .map(_(0))
        .toList
      if(imgIds.nonEmpty) {
        val inbldImgWithHeightDS =
          DBSink(spark, commonConfigParams)
            .query(
              "select id, image_height from inbuilding where id in (" + imgIds
                .mkString(",") + ") and image_height is not null")
            .getDataFrame
            .toDF("inbldImgId", "inbldImgHeight")
        finalInbuildingDS = finalInbuildingDS.join(inbldImgWithHeightDS,
          Seq("inbldImgId"),
          "left_outer")
        finalInbuildingDS = finalInbuildingDS.withColumn(
          "interpolated_y",
          $"inbldImgHeight" - $"interpolated_y")
      }

      joinedCombo = joinedCombo.join(finalInbuildingDS,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")
    }

    /** ************************************************************************************************************************************************************************* */

    //    if(logcodeList.contains("0xB087")) {
//      kpiRlcDLTputExists = true
//      var rlcDLTput0xB087v49 = spark.sql("select * from (select fileName,dateWithoutMillis, rlcDlBytes, rlcRbConfigIndex, rlcDlNumRBs, rlcDlTimeStamp0xb087v49,lag(rlcDlBytes,1) over(partition by fileName order by dmTimeStamp asc) as prerlcDlBytes, " +
//        "lag(rlcRbConfigIndex,1) over(partition by fileName order by dmTimeStamp asc) as prerlcRbConfigIndex, " +
//        "lag(rlcDlNumRBs,1) over(partition by fileName order by dmTimeStamp asc) as prerlcDlNumRBs, " +
//        "lag(rlcDlTimeStamp0xb087v49,1) over(partition by fileName order by dmTimeStamp asc) as prerlcDlTimeStamp0xb087v49 " +
//        "from (select fileName,dmTimeStamp,dateWithoutMillis, first_value(rlcDlDataBytes, true) over(partition by fileName, dateWithoutMillis order by dmTimeStamp asc) as rlcDlBytes, " +
//        "first_value(rlcRbConfigIndex, true) over(partition by fileName, dateWithoutMillis order by dmTimeStamp asc) as rlcRbConfigIndex, " +
//        "first_value(rlcDlNumRBs, true) over(partition by fileName, dateWithoutMillis order by dmTimeStamp asc) as rlcDlNumRBs, " +
//        "first_value(rlcDlTimeStamp, true) over(partition by fileName,dateWithoutMillis order by dmTimeStamp asc) as rlcDlTimeStamp0xb087v49, " +
//        "row_number() over(partition by fileName, dateWithoutMillis order by dmTimeStamp asc) as rn from (select fileName, rlcDlDataBytes, dmTimeStamp, rlcRbConfigIndex, rlcDlNumRBs, rlcDlTimeStamp, " +
//        "split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis from dmatMillSecDataframe where rlcDlNumRBs is not null)) where rn=1)")
//
//      joinedCombo = joinedCombo.join(rlcDLTput0xB087v49, Seq("fileName", "dateWithoutMillis"), "full_outer")
//
//    }
    /** ************************************************************************************************************************************************************************* */
    val Qualcomm5GTPUT0xB887Df = spark.sql(
      "select * from dmatMillSecDataframe where hexlogcode = '0xB887'")

    val dmatAsndf = spark.sql(
      "select * from dmatMillSecDataframe where hexlogcode = '0xB0C0'")

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

    var final5gDLSessionDf =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    if(logcodeList.contains("0xB0C0") || logcodeList.contains("0xB887") ) {
//      println("0xB0C0 || 0xB887")
      kpi5gDlDataBytesExists = true
      val Qualcomm5GTPUT0xB887Df = spark.sql("select * from dmatMillSecDataframe where hexlogcode = '0xB887'")

      val dmatAsndf = spark.sql("select * from dmatMillSecDataframe where hexlogcode = '0xB0C0'")

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
        .select($"fileName", $"dmTimeStamp", $"dateWithoutMillis", $"rccConnStart", $"pdschSize")
        .withColumn(
          "RRC_startTime",
          coalesce($"rccConnStart", last($"rccConnStart", true).over(w)))
      dmatAsnRrcCon5.createOrReplaceTempView("dmatAsnRrcCon5")
      final5gDLSessionDf = spark.sql("select * from (select *, sum(pdschSize) over (partition by RRC_startTime order by dateWithoutMillis asc rows unbounded preceding) as sessionDlDataBytes, row_number() over(partition by fileName, dateWithoutMillis order by dmTimeStamp asc) as rn from " +
        "dmatAsnRrcCon5) where rn = 1").drop("pdschSize", "rccConnStart", "RRC_startTime")
      joinedCombo = joinedCombo.join(final5gDLSessionDf, Seq("fileName", "dateWithoutMillis"), "full_outer")

      /** ******************************************************************************************************** */
      var carrierID0xB887Df =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

      carrierID0xB887Df = Qualcomm5GTPUT0xB887Df
        .select($"fileName",
          $"dateWithoutMillis",
          $"CarrierID_0xB887",
          $"numerology_0xB887")
        .groupBy("fileName", "dateWithoutMillis")
        .agg(
          flatten2(collect_list($"CarrierID_0xB887")).as("carrierIDCount"),
          mergeList3(collect_list($"CarrierID_0xB887"))
            .as("carrierIDsFoAvgRbCal"),
          flatten2(collect_list($"numerology_0xB887")).as("numerologyForAvgRbCal")
        )
      joinedCombo = joinedCombo.join(carrierID0xB887Df,
        Seq("fileName", "dateWithoutMillis"),
        "full_outer")
    }
    /** ****************************************************************************************************** */
    /** *****************************************************join all 5g datasets *********************************************************************** */

    val Qualcomm5GDf1 = spark.sql(s"""select fileName,max(dmTimeStamp) as dmTimeStamp,dateWithoutMillis,
      ${Nr5gQuery.query} from (select * from dmatMillSecDataframe sort by fileName, dmTimeStamp desc) group by fileName, dateWithoutMillis""")

//    if(logcodeList.contains("0xB881") && logcodeList.contains("0xB883")) {
//      kpiNrPhyULThroughputExists = true
//      val Qualcomm5gTPUT0xB881Df2 = spark.sql(
//        """select * from (select filename, dmTimeStamp, dateWithoutMillis, tbNewTxBytesB881, numNewTxTbB881, numReTxTbB881,
//             nr5GPhyULInstTputPCC_0XB883, nr5GPhyULInstTputSCC1_0XB883, nr5GPhyULInstTput_0XB883, nr5GPhyULInstRetransRatePCC_0XB883, nr5GPhyULInstRetransRateSCC1_0XB883, nr5GPhyULInstRetransRate_0XB883,
//             row_number() over (partition by dateWithoutMillis order by dmTimeStamp asc) as rn from dmatMillSecDataframe where logcode in (47233,47235)) where rn =1""")
//
//      val window0xB881 = Window.partitionBy($"filename").orderBy($"dmTimeStamp".asc)
//
//      val Qualcomm5gTPUT0xB881Df3 = Qualcomm5gTPUT0xB881Df2.withColumn("tbNewTxBytesB881_prev", lag($"tbNewTxBytesB881",1,null) over window0xB881)
//        .withColumn("numNewTxTbB881_prev", lag($"numNewTxTbB881",1,null) over window0xB881)
//        .withColumn("numReTxTbB881_prev",lag($"numReTxTbB881",1,null) over window0xB881)
//        .withColumn("dmTimeStamp_prev",lag($"dmTimeStamp",1,null) over window0xB881)
//        .withColumn("elapsedTimeDiffSeconds", timestamp_diffseconds($"dmTimeStamp",$"dmTimeStamp_prev"))
//        .withColumn("tbNewTxBytesB881_delta",coalesce($"tbNewTxBytesB881",lit(0)) - coalesce($"tbNewTxBytesB881_prev",lit(0)))
//        .withColumn("numNewTxTbB881_delta", coalesce($"numNewTxTbB881",lit(0)) - coalesce($"numNewTxTbB881_prev",lit(0)))
//        .withColumn("numReTxTbB881_delta", coalesce($"numReTxTbB881",lit(0)) - coalesce($"numReTxTbB881_prev",lit(0)))
//        .withColumn("nr5gPhyULTputB881", when($"tbNewTxBytesB881_delta">0,$"tbNewTxBytesB881_delta" / $"elapsedTimeDiffSeconds"  ).otherwise(null))
//        .withColumn("nr5gPhyULRetransRateB881", when(($"numNewTxTbB881_delta" + $"numReTxTbB881_delta")>0, ($"numReTxTbB881_delta" * 100) / ($"numNewTxTbB881_delta" + $"numReTxTbB881_delta")).otherwise(null))
//        .select($"filename",$"dateWithoutMillis",$"nr5gPhyULTputB881",$"nr5gPhyULRetransRateB881",$"nr5GPhyULInstTputPCC_0XB883", $"nr5GPhyULInstTputSCC1_0XB883", $"nr5GPhyULInstTput_0XB883", $"nr5GPhyULInstRetransRatePCC_0XB883", $"nr5GPhyULInstRetransRateSCC1_0XB883", $"nr5GPhyULInstRetransRate_0XB883")
//
//      Qualcomm5GDf1 = Qualcomm5GDf1.join(Qualcomm5gTPUT0xB881Df3, Seq("filename","dateWithoutMillis"), "left_outer")
//
//    }
    var Qualcomm5GDf_merged = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    //collected0xb887_carrierid,collected0xb887_crcstatus
    Qualcomm5GDf_merged = Qualcomm5GDf1.withColumn("Serving_BRSRP", mergeList3($"collectedB975_nrsbrsrp"))
      .withColumn("Serving_BRSRQ", mergeList3($"collectedB975_nrsbrsrq"))
      .withColumn("Serving_BSNR", mergeList4($"collectedB975_nrsbsnr"))
      .withColumn("Serving_nrbeamcount", mergeList4($"collectedB975_nrbeamcount"))
      .withColumn("Serving_ccindex", mergeList4($"collectedB975_ccindex"))
      .withColumn("Serving_nrpci", mergeList4($"collectedB975_nrpci"))
      .withColumn("Serving_NRServingBeamIndex", mergeList6($"collectedB975_NRServingBeamIndex"))
      .withColumn("rachResult_b88a", mergeList4($"collectedb88a_rachResult"))
      .withColumn("carrieridcsv_0xb887",mergeList3($"collected0xb887_carrierid"))
      .withColumn("cellid_0xb887",mergeList3($"collected0xb887_CellId"))
      .withColumn("crcstatuscsv_0xb887",mergeList3($"collected0xb887_crcstatus"))
      .withColumn("Serving_BRSRP_0xB97F", mergeList3($"collected0xb97f_nrsbrsrp"))
     // .withColumn("cdrxKpis0xb890",explode(mergeList9($"cdrxkpilist_0xB890"))).selectExpr("*","cdrxKpis0xb890._1 as slotCurrent0xb890", "cdrxKpis0xb890._2 as slotPrev0xb890", "cdrxKpis0xb890._3 as sfnCurrent0xb890", "cdrxKpis0xb890._4 as sfnPrev0xb890")
    joinedCombo = joinedCombo.join(Qualcomm5GDf_merged, Seq("fileName", "dateWithoutMillis"), "full_outer")

    /** ************************************************************************************************************************************************************************* */
    /*TODO join rach average df here */



    //forward-fill for nrsetupsuccess to set nrscgstate kpi as connected/disconnected as per availability of serving beam rsrp
    val nrSCGStateWindow=Window.partitionBy("fileName").orderBy($"dateWithoutMillis".asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val beamRsrpWin = Window.partitionBy("fileName").orderBy($"dateWithoutMillis".asc)
    //backward-fill for nrscgrleease
    val nrSCGReleaseWindow=Window.partitionBy("fileName").orderBy($"dateWithoutMillis".asc).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val nrstateWin = Window.partitionBy("fileName","dateWithoutMillis").orderBy($"dmtimestamp".asc)
   /* joinedCombo=joinedCombo.withColumn("nrsetupstatus",when($"nrsetupsuccess".isNotNull || $"nrsetupsuccess"=="1", lit("success")).otherwise(lit(null)))
      .withColumn("nrreleasestatus", when($"nrReleaseEvt".isNotNull || $"nrReleaseEvt"==1, lit("release")).otherwise(lit(null)))
*/     // .withColumn("nrreleasecnt", when($"nrReleaseEvt".isNotNull || $"nrReleaseEvt"==1, count("nrReleaseEvt").over(nrstateWin)).otherwise(lit(null)))

    // As nr setup success/failure could happen on same timestamp that results in duplicates ES documents, aggregating them to single timestamp
    //
    //     if(logCodesInFile.contains("0xB889") || logCodesInFile.contains("0xB88A")) {
    //       kpiisRachExists = true
    //       joinedCombo = joinedCombo.withColumn("Nrsetupattempt",last($"nrsetupattempt", true).over(rachSetupWindow))
    //         .withColumn("Nrsetupsuccess",last($"nrsetupsuccess", true).over(rachSetupWindow))
    //         .withColumn("Nrsetupfailure",last($"nrsetupfailure", true).over(rachSetupWindow))
    //         .withColumn("nrsetupsuccesscnt", count($"nrsetupsuccess").over(rachSetupWindow))
    //         .withColumn("nrsetupfailurecnt", count($"nrsetupfailure").over(rachSetupWindow))
    //         .withColumn("nrsetupattemptcnt", count($"nrsetupattempt").over(rachSetupWindow))
    //         .withColumn("NRRachFailureEvt", when(col("rachResult_b88a").contains("FAILURE"), 1).otherwise(lit(null)))
    //         .withColumn("NRRachAbortEvt", when(col("rachResult_b88a").contains("ABORTED"), 1).otherwise(lit(null)))
    //         // .withColumn("nrstatesuccess",mergeList10($"nrstate"))
    //         // .withColumn("mergedNrSuccessState",when($"nrstatesuccess".isNotNull || $"nrsetupsuccess".isNotNull, mergeListSum(array($"nrstatesuccess", $"nrsetupsuccess")).otherwise(lit(null))))
    //         // .withColumn("connectedstate",when($"mergedNrSuccessState".isNull && lag($"nrReleaseEvt",1,0).over(rachSetupWindow) >0, last($"mergedNrSuccessState", true).over(nrSCGStateWindow)).otherwise(null))
    //         //.withColumn("nrstate",when($"nrsetupstatus".isNotNull || $"nrreleasestatus".isNotNull, mergeListNrSuccess(mergeList4(array($"nrsetupstatus", $"nrreleasestatus"))).otherwise(lit(null))))
    //         .withColumn("rownum",row_number().over(rachSetupWindow))
    //         .filter("rownum = 1")
    //
    //       dmatEvents += ($"Nrsetupfailure",$"NRRachFailureEvt",$"NRRachAbortEvt")
    //
    //     }
   /* var NrSuccessToReleaseDfFinal = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    joinedCombo.createOrReplaceTempView("NrScgStateDf")
    var NrSuccessToReleaseDfTemp = spark.sql("select fileName, dateWithoutMillis, mergedNrSuccessState, nrReleaseEvt from NrScgStateDf where  mergedNrSuccessState = 1 or nrReleaseEvt =1")
    NrSuccessToReleaseDfFinal =  NrSuccessToReleaseDfTemp.withColumn("NrReleaseTimestamp",  when($"mergedNrSuccessState".isNotNull, lead($"dateWithoutMillis",1,null).over(nrSCGReleaseWindow)))
    joinedCombo = joinedCombo.join(NrSuccessToReleaseDfFinal, Seq("fileName", "dateWithoutMillis"), "left_outer")
*/
      /*.withColumn("beamRsrspOfPreviousLogPac",lag($"Serving_BRSRP",1,null).over(beamRsrpWin))
      .withColumn("beamRsrspOfB97f_PreviousLogPac",lag($"nrsbrsrp_0xb97f",1,null).over(beamRsrpWin))
      .withColumn("beamIndexOfB97f_PreviousLogPac",lag($"nrservingbeamindex_0xb97f",1,null).over(beamRsrpWin))
      .withColumn("setupsuccess_PreviousLogPac",lag($"nrsetupsuccess",1,null).over(beamRsrpWin))
*/
    //nr5gNumRxBytes0xb860TputDiffsDf,rlcDataBytes0xB84DDiffsDf
    val timestampDiffInSeconds = udf(
      (startTime: Timestamp, endTime: Timestamp) => {
        (startTime.getTime() - endTime.getTime()) / 1000

      })

    val win = Window.partitionBy($"fileName", $"dateWithoutMillis").orderBy($"dmTimeStamp".asc)
    val win5MsDlTPUT = Window.partitionBy($"fileName",$"dateWithoutMillis").orderBy($"dmTimeStamp".asc)
    val win1SecondDlTPUT = Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)


    var finalDlScgTput0xB888 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    if(logcodeList.contains("0xB888")) {
//      println("0xB888")
      kpiNrDlScg5MsTputExists = true
    val Qualcomm5GTPUT0xB888Df = spark.sql("select * " +
        "from dmatMillSecDataframe where hexlogcode = '0xB888'")
    // Determining 5 Milliseconds Dl throughput KPIs
    val timeStampTbsizeDiff = Qualcomm5GTPUT0xB888Df.select($"fileName", $"dmTimeStamp", $"dateWithoutMillis", $"TB_Size_0xB888",$"TB_SizePcc_0xB888",$"TB_SizeScc1_0xB888",$"TB_SizeScc2_0xB888",$"TB_SizeScc3_0xB888",$"TB_SizeScc4_0xB888",$"TB_SizeScc5_0xB888",$"TB_SizeScc6_0xB888",$"TB_SizeScc7_0xB888").withColumn("dmTimeStampPrevious",
      lag("dmTimeStamp", 1, null).over(win5MsDlTPUT)).withColumn("tbSizePrev", lag("TB_Size_0xB888", 1, null).over(
      win5MsDlTPUT)).withColumn("tbSizeCurrent", first("TB_Size_0xB888", true).over(win5MsDlTPUT)).withColumn("tbSizePccCurrent", first("TB_SizePcc_0xB888", true).over(win5MsDlTPUT))
      .withColumn("tbSizeScc1Current", first("TB_SizeScc1_0xB888", true).over(win5MsDlTPUT)).withColumn("tbSizeScc2Current", first("TB_SizeScc2_0xB888", true).over(win5MsDlTPUT))
      .withColumn("tbSizeScc3Current", first("TB_SizeScc3_0xB888", true).over(win5MsDlTPUT)).withColumn("tbSizeScc4Current", first("TB_SizeScc4_0xB888", true).over(win5MsDlTPUT))
      .withColumn("tbSizeScc5Current", first("TB_SizeScc5_0xB888", true).over(win5MsDlTPUT)).withColumn("tbSizeScc6Current", first("TB_SizeScc6_0xB888", true).over(win5MsDlTPUT))
      .withColumn("tbSizeScc7Current", first("TB_SizeScc7_0xB888", true).over(win5MsDlTPUT))

    val diff_secs_col = timeStampTbsizeDiff.filter($"dmTimeStampPrevious".isNotNull).select($"fileName", $"dmTimeStamp", $"dateWithoutMillis", $"dmTimeStampPrevious", $"TB_Size_0xB888", $"tbSizePrev")
      .withColumn("timeDff", timestamp_diff(($"dmTimeStamp"), ($"dmTimeStampPrevious")))
      .withColumn("tPUT5MilliSeconds", ($"TB_Size_0xB888" - $"tbSizePrev") * 1600 / Constants.Mbps)
    val DlTput5MsDiffDf = diff_secs_col.filter($"timeDff" === 5).drop("TB_Size_0xB888", "tbSizePrevtimeDff")

    val dlTput1MSDiff = timeStampTbsizeDiff.select($"fileName", $"dateWithoutMillis", $"dmTimeStamp", $"tbSizeCurrent", $"tbSizePccCurrent",$"tbSizeScc1Current",$"tbSizeScc2Current",$"tbSizeScc3Current",$"tbSizeScc4Current",$"tbSizeScc5Current",$"tbSizeScc6Current",$"tbSizeScc7Current")
    .withColumn("rn", row_number().over(win)).filter($"rn" === 1).drop("rn")

    var dlTput1Sec = dlTput1MSDiff.withColumn("tbSizePrev", lag("tbSizeCurrent", 1, null).over(win1SecondDlTPUT))
                .withColumn("tbSizePccPrev", lag("tbSizePccCurrent", 1, null).over(win1SecondDlTPUT)).withColumn("tbSizeScc1Prev", lag("tbSizeScc1Current", 1, null).over(win1SecondDlTPUT))
                .withColumn("tbSizeScc2Prev", lag("tbSizeScc2Current", 1, null).over(win1SecondDlTPUT)).withColumn("tbSizeScc3Prev", lag("tbSizeScc3Current", 1, null).over(win1SecondDlTPUT))
                .withColumn("tbSizeScc4Prev", lag("tbSizeScc4Current", 1, null).over(win1SecondDlTPUT)).withColumn("tbSizeScc5Prev", lag("tbSizeScc5Current", 1, null).over(win1SecondDlTPUT))
                .withColumn("tbSizeScc6Prev", lag("tbSizeScc6Current", 1, null).over(win1SecondDlTPUT)).withColumn("tbSizeScc7Prev", lag("tbSizeScc7Current", 1, null).over(win1SecondDlTPUT))
                 .withColumn("dmTimeStampPrev", lag("dmTimeStamp", 1, null).over(win1SecondDlTPUT)).withColumn("timeDffSeconds", timestamp_diffseconds(($"dmTimeStamp"), ($"dmTimeStampPrev")))

    var finaldlTput1Sec = dlTput1Sec.filter($"timeDffSeconds">0).withColumn("tPUT1Sec", ($"tbSizeCurrent" - $"tbSizePrev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps)
      .withColumn("tPUTPccSec", ($"tbSizePccCurrent" - $"tbSizePccPrev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).withColumn("tPUTScc1Sec", ($"tbSizeScc1Current" - $"tbSizeScc1Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps)
      .withColumn("tPUTScc2Sec", ($"tbSizeScc2Current" - $"tbSizeScc2Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).withColumn("tPUTScc3Sec", ($"tbSizeScc3Current" - $"tbSizeScc3Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps)
      .withColumn("tPUTScc4Sec", ($"tbSizeScc4Current" - $"tbSizeScc4Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).withColumn("tPUTScc5Sec", ($"tbSizeScc5Current" - $"tbSizeScc5Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps)
      .withColumn("tPUTScc6Sec", ($"tbSizeScc6Current" - $"tbSizeScc6Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps).withColumn("tPUTScc7Sec", ($"tbSizeScc7Current" - $"tbSizeScc7Prev") * 8 / $"timeDffSeconds" * 1/Constants.Mbps)
      .select($"fileName", $"dateWithoutMillis",$"tPUT1Sec",$"tPUTPccSec",$"tPUTScc1Sec",$"tPUTScc2Sec",$"tPUTScc3Sec",$"tPUTScc4Sec",$"tPUTScc5Sec",$"tPUTScc6Sec",$"tPUTScc7Sec")

      var finalDlTput5Ms = DlTput5MsDiffDf.withColumn("rn", row_number().over(win)).filter($"rn" === 1).drop($"rn")
      finalDlScgTput0xB888 = finaldlTput1Sec.join(finalDlTput5Ms, Seq("fileName", "dateWithoutMillis"), "left_outer")
      joinedCombo = joinedCombo.join(finalDlScgTput0xB888, Seq("fileName", "dateWithoutMillis"), "full_outer")
    }
    /** ************************************************************************************************************************************************************************* */
    var geoBoundingDistDf: DataFrame = null
    var joinedAllTablesFinalDist: DataFrame = null
    var geoDistanceDf: DataFrame = null
    var geoDistanceFinalDf: DataFrame = null

    // request not valid for SIG files, but neither logRecordName nor fileType available here
    // so using fileName comparison
    geoBoundingDistDf = dmatDataframe.filter(!$"fileName".contains("sig.seq") && ($"Latitude".isNotNull || $"Longitude".isNotNull || $"Latitude2".isNotNull || $"Longitude2".isNotNull)).select($"fileName", $"Latitude", $"Longitude", $"Latitude2", $"Longitude2", $"dateWithoutMillis").groupBy("fileName")
      .agg(max(when($"Latitude".isNotNull, $"Latitude").otherwise(when($"Latitude2".isNotNull, $"Latitude2"))).as("Latitude_tl"),
        min(when($"Latitude".isNotNull, $"Latitude").otherwise(when($"Latitude2".isNotNull, $"Latitude2"))).as("Latitude_br"),
        max(when($"Longitude".isNotNull, $"Longitude").otherwise(when($"Longitude2".isNotNull, $"Longitude2"))).as("Longitude_br"),
        min(when($"Longitude".isNotNull, $"Longitude").otherwise(when($"Longitude2".isNotNull, $"Longitude2"))).as("Longitude_tl"),
        min("dateWithoutMillis").as("fileStartTime"), max("dateWithoutMillis").as("fileEndTime"))

    val getPciInfoForDist = udf((PciVal: String, lteeNbid: String, Latitude_tl: String, Longitude_tl: String, Latitude_br: String, Longitude_br: String, startTime: String, endTIme: String) => {
      val cellIndexes = getCellSiteIndex(startTime.split(" ").apply(0), startTime.split(" ").apply(0), "cellsite")
      var PCI: Array[Int] = Array(0)
      var LTEenBid: Array[String] = Array("")
        if(PciVal.nonEmpty) {
          PCI = PciVal.split(",").map(_.toInt).toArray
        }
        if(lteeNbid.nonEmpty) {
          LTEenBid = lteeNbid.split(",").map(_.toString).toArray
        }
        val pciRequestInfo = PciRequestInfo(PCI, LTEenBid, Latitude_tl.toDouble, Longitude_tl.toDouble, Latitude_br.toDouble, Longitude_br.toDouble, cellIndexes, startTime.split(" ").apply(0), endTIme.split(" ").apply(0))
        val localTransportAddress = LocalTransportAddress(commonConfigParams.ES_NODE_ID, commonConfigParams.ES_PORT_ID, commonConfigParams.ES_CLUSTER_NAME)
        CommonDaoImpl.getPCIBasedLatLon(pciRequestInfo, localTransportAddress,commonConfigParams)
      })

    if (geoBoundingDistDf != null) {
      val geoBoundingDistDfTemp = geoBoundingDistDf.join(dmatB192_B193NeighboringPciDist, Seq("fileName"), "left_outer")
      joinedAllTablesFinalDist = geoBoundingDistDfTemp.withColumn("servingPciInfo", when($"ServingPCI".isNotNull && $"Serving_lteeNBId".isNotNull, getPciInfoForDist($"ServingPCI", $"Serving_lteeNBId", $"Latitude_tl", $"Longitude_tl", $"Latitude_br", $"Longitude_br", $"fileStartTime", $"fileEndTIme")).otherwise(lit(null))).withColumn("NeighboringPciInfo", when($"NeighboringPCI".isNotNull && $"Serving_lteeNBId".isNotNull, getPciInfoForDist($"NeighboringPCI", $"Serving_lteeNBId", $"Latitude_tl", $"Longitude_tl", $"Latitude_br", $"Longitude_br", $"fileStartTime", $"fileEndTIme")).otherwise(lit(null)))
      kpiGeoDistToCellsQuery = true
      CommonDaoImpl.closeESClient()
    }

    val NrsPciWindow = Window.partitionBy("fileName", "nrcellid").orderBy($"dateWithoutMillis".asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // request not valid for SIG files, but neither logRecordName nor fileType available here
    // so using fileName comparison
    geoDistanceDf = dmatDataframe.filter(!$"fileName".contains("sig.seq") && $"nrcellid".isNotNull && $"nrarfcn".isNotNull && (($"Longitude".isNotNull && $"Latitude".isNotNull)
      || ($"Longitude2".isNotNull && $"Latitude2".isNotNull)))
      .select($"fileName", $"dateWithoutMillis", $"nrcellid", $"nrarfcn", $"spCellTypeDL", $"Latitude", $"Longitude", $"Latitude2", $"Longitude2")
      .withColumn("rnPci", row_number.over(NrsPciWindow)).filter($"rnPci" === 1).drop($"rnPci")


    val gNbidForNr5gAsn = udf((PciVal: Integer, NrArfcn: String, CellType: String, Latitude: String, Longitude: String, Latitude2: String, Longitude2: String, startTime: String, endTIme: String) => {
      val cellIndexes = getCellSiteIndex(startTime.split(" ").apply(0), startTime.split(" ").apply(0), "cellsite")
      var gnodebidRequestInfo: gNbidRequestInfo = null

      if (Longitude != null && Latitude != null) {
        gnodebidRequestInfo = gNbidRequestInfo(PciVal, NrArfcn.toInt, CellType, Latitude.toDouble, Longitude.toDouble, cellIndexes, startTime.split(" ").apply(0), endTIme.split(" ").apply(0))
      } else if (Longitude2 != null && Latitude2 != null) {
        gnodebidRequestInfo = gNbidRequestInfo(PciVal, NrArfcn.toInt, CellType, Latitude2.toDouble, Longitude2.toDouble, cellIndexes, startTime.split(" ").apply(0), endTIme.split(" ").apply(0))
      }
      val localTransportAddress = LocalTransportAddress(commonConfigParams.ES_NODE_ID, commonConfigParams.ES_PORT_ID, commonConfigParams.ES_CLUSTER_NAME)
      CommonDaoImpl.gNodeBidBasedOnPci(gnodebidRequestInfo, localTransportAddress, commonConfigParams)
      })

      if(geoDistanceDf != null) {
        geoDistanceFinalDf = geoDistanceDf.withColumn("gNBidInfo", when($"nrcellid".isNotNull, gNbidForNr5gAsn($"nrcellid", $"nrarfcn", $"spCellTypeDL", $"Latitude", $"Longitude", $"Latitude2", $"Longitude2", $"dateWithoutMillis", $"dateWithoutMillis")).otherwise(lit(null))).drop("dateWithoutMillis")
        gNbidGeoDistToCellsQuery = true
       // CommonDaoImpl.closeESClient()
      }

    if (joinedAllTablesFinalDist != null) {
      joinedCombo = joinedCombo.join(joinedAllTablesFinalDist, Seq("fileName"), "left_outer")
    }
    else
    {
      joinedCombo = joinedCombo.withColumn("servingPciInfo", (lit(null))).withColumn("NeighboringPciInfo",(lit(null)))
    }

    if (geoDistanceFinalDf != null) {
      joinedCombo = joinedCombo.join(geoDistanceFinalDf, Seq("fileName", "nrcellid"), "left_outer")
    }
    else
    {
      joinedCombo = joinedCombo.withColumn("gNBidInfo", (lit(null)))
    }
    /** ************************************************************************************************************************************************************************* */
    //Voltecaldf

    var finalVolteCallLeadDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    var incomingGapTypeDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    var outgoingGapTypeDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    var volteCallFailureDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val winVolteDf = Window.partitionBy($"fileName",$"ipcallId").orderBy($"dmtimestamp".asc)
    val winVolteCallStartOrEndDf = Window.partitionBy($"fileName",$"dateWithoutMillis",$"ipcallId").orderBy($"dmtimestamp".asc)
    //call start is established when ipSubtitle and sipCSeq has INVITE message followed by 200 ok status for ipsubtitle in subsequent timestmap
    //call end is established when ipSubtitle and sipCSeq has BYE message followed by 200 ok status for ipsubtitle in subsequent timestmap
    if(logcodeList.contains("0xFFF0")) {
//      println("0xFFF0")
      kpiDmatIpExists = true
    val volteCallDf = spark.sql("select dmtimestamp,fileName,ipcallId,ipSubtitle,sipCSeq,NAS_MsgType,dateWithoutMillis from fileDataFrame "+
      "where ((ipSubtitle='INVITE' and sipCSeq like '%INVITE%') or (ipSubtitle='200 OK' and sipCSeq like '%INVITE%') or (ipSubtitle='BYE' and sipCSeq like '%BYE%') or (ipSubtitle='200 OK' and sipCSeq like '%BYE%') or (NAS_MsgType='ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST' or  NAS_MsgType = 'ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT')) ")




    /*   val volteCallLeadDf = volteCallDf.withColumn("newColSubtitle", lag("ipSubtitle", 1, 0).over(winVolteDf)).withColumn("newColsipCSeq", lag("sipCSeq", 1, 0).over(winVolteDf))
       finalVolteCallLeadDf = volteCallLeadDf.withColumn("VolteCallStart", when(col("ipSubtitle") ==="200 OK"  && col("newColSubtitle") === "INVITE" && col("sipCSeq") ===col("newColsipCSeq"), "1").otherwise(lit(null)))
         .withColumn("VolteCallEnd", when(col("ipSubtitle") ==="200 OK" && col("newColSubtitle") ==="BYE" && col("sipCSeq") ===col("newColsipCSeq"), "1").otherwise(lit(null))).drop("ipSubtitle","sipCSeq","newColSubtitle","newColsipCSeq").where($"VolteCallStart" === 1 || $"VolteCallEnd" === 1)
 */
    finalVolteCallLeadDf = volteCallDf
      .withColumn("VolteCallStart", when(col("ipSubtitle") ==="200 OK"  && col("sipCSeq").contains("INVITE"), "1").otherwise(lit(null)))
      .withColumn("VolteCallEnd", when(col("ipSubtitle") ==="200 OK" &&  col("sipCSeq").contains("BYE"), "1").otherwise(lit(null)))
      .where($"VolteCallStart" === 1 || $"VolteCallEnd" === 1)
      .withColumn("rn", row_number.over(winVolteCallStartOrEndDf))
      .filter($"rn"===1)
      .drop("rn","ipSubtitle","sipCSeq","newColSubtitle","newColsipCSeq")

    //To determine incoming/outgoing gap type 1) Find timestamp when call start and ends within a file
      val callStartandEndTimeStampDf = finalVolteCallLeadDf.withColumn("callEndTimeStamp", when(lead("VolteCallEnd", 1, null).over(winVolteDf).isNotNull,
        lead("dateWithoutMillis", 1, null).over(winVolteDf)).otherwise(lit(null))).drop("dmtimestamp","ipcallId").filter($"callEndTimeStamp".isNotNull)

    //Select all logrecords(Milliseconds) within call start and end timestamps.Each file can have several call start & end times.
      val logRecordsWithinCallStartandEndDf = fileDataFrame.select($"fileName", $"dmTimeStamp", $"dateWithoutMillis",$"rtpDirection")
        .join(callStartandEndTimeStampDf.select($"dateWithoutMillis" as "r_date", $"callEndTimeStamp",$"fileName" as "file_name"),$"fileName" === $"file_name" && $"dateWithoutMillis" >=$"r_date" and $"dateWithoutMillis" <= $"callEndTimeStamp")

      val windPreviousDmTimeStamp = Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)

    // For each logrecords within call start and end times, determine difference between current and previous timestamp.
      val incomingPreviousTimeStampDf = logRecordsWithinCallStartandEndDf.filter($"rtpDirection"==="INCOMING").withColumn("PreviousTimeStamp", lag("dmTimeStamp", 1, null).over(windPreviousDmTimeStamp))
      val outgoingPreviousTimeStampDf = logRecordsWithinCallStartandEndDf.filter($"rtpDirection"==="OUTGOING").withColumn("PreviousTimeStamp", lag("dmTimeStamp", 1, null).over(windPreviousDmTimeStamp))

    // If timstamp diff is 500ms - 1s refer as X-small,  Small = 1s - 3s, Medium = 3s - 5s, Large = 5s - 10s, X-Large = >10s

       incomingGapTypeDf = incomingPreviousTimeStampDf.filter($"PreviousTimeStamp".isNotNull).withColumn("incomingGapTypeDuration", timestampDiffInSeconds($"dmTimeStamp", $"PreviousTimeStamp")).filter($"incomingGapTypeDuration">=0.5)
       outgoingGapTypeDf = outgoingPreviousTimeStampDf.filter($"PreviousTimeStamp".isNotNull).withColumn("outgoingGapTypeDuration", timestampDiffInSeconds($"dmTimeStamp", $"PreviousTimeStamp")).filter($"outgoingGapTypeDuration">=0.5)

    volteCallDf.createOrReplaceTempView("volteCallDf")

      //  DMATBD-11772: when ipsubtitle is invite and sipCSeq is 1 Invite check if ipsubtitle has 200 OK and sipCSeq with 1 Invite in next 25 seconds of log records/hive records. When 200 ok and 1 invite is not available, mark the records as call failure evt occurred
      val volteCallFailureWithin25Seconds = spark.sql("select *, collect_list(ipSubtitle) over (partition by fileName, ipcallId order by cast(dmtimestamp AS timestamp) asc range between current row and interval 25 seconds following) as collected_ipSubtitle, " +
        "collect_list(sipCSeq) over (partition by fileName, ipcallId order by cast(dmtimestamp AS timestamp) asc range between current row and interval 25 seconds following) as collected_sipCSeq," +
        "collect_list(NAS_MsgType) over (partition by fileName order by cast(dmtimestamp AS timestamp) asc range between current row and interval 25 seconds following) as collected_nasMsgType from volteCallDf")

      val windVolteCallFailure = Window.partitionBy($"fileName",$"dateWithoutMillis",$"ipcallId").orderBy($"dateWithoutMillis".asc)
       volteCallFailureDf = volteCallFailureWithin25Seconds.withColumn("rn", row_number.over(windVolteCallFailure)).withColumn("ipSubtitleMsgLast25Secs", mergeList4($"collected_ipSubtitle"))
         .withColumn("sipCSeqInLast25Secs", mergeList4($"collected_sipCSeq")).withColumn("nasMsgTypeInLast25Secs", mergeList4($"collected_nasMsgType"))
         .filter($"rn"===1).where($"ipcallId".isNotNull).drop("rn","dmtimestamp","ipcallId","sipCSeq","NAS_MsgType","collected_ipSubtitle","collected_sipCSeq","collected_nasMsgType")

    }



    joinedCombo = joinedCombo.join(finalVolteCallLeadDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
      .join(incomingGapTypeDf, Seq("fileName", "dateWithoutMillis"), "full_outer")
      .join(outgoingGapTypeDf, Seq("fileName", "dateWithoutMillis"), "full_outer")
      .join(volteCallFailureDf, Seq("fileName", "dateWithoutMillis"), "full_outer")


    /** ************************************************************************************************************************************************************************* */
    /** ************************************************************************************************************************************************************************* */
    joinedCombo = joinedCombo.drop("dmTimeStamp")

    /** ***** Merging 1 second df with millsecond df *************** */
    finalJoinedTables = finalJoinedTables.join(joinedCombo, Seq("fileName", "dateWithoutMillis"), "left_outer")

    var updateEvtsCntInlogs = udf((testid:Int,filename:String,evtsCnt:Int) => {
      var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
      evtInfo = LteIneffEventInfo(testId=testid,fileName=Constants.getDlfFromSeq(filename),evtsCount=evtsCnt)
      CommonDaoImpl.insertEventsCountInES(commonConfigParams,evtInfo)
    })

    if (sig_logcodes_Final != null) {
      finalJoinedTables = finalJoinedTables.join(sig_logcodes_Final, Seq("fileName", "dateWithoutMillis"), "full_outer")
        .join(siglogcodesUnionDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
      //  .join(firstLastLocationRecordDf, Seq("fileName", "dateWithoutMillis"), "left_outer")
        .join(sig0x2501logcode_Final, Seq("fileName", "dateWithoutMillis"), "full_outer")
        .join(sig0x240Clogcode_Final, Seq("fileName", "dateWithoutMillis"), "full_outer")
    }

    if (traceScannerMsgFinal != null) {
      finalJoinedTables = finalJoinedTables.join(traceScannerMsgFinal, Seq("fileName", "dateWithoutMillis"), "full_outer")
      if(NRTopNSignalKPIs != null) {
        finalJoinedTables = finalJoinedTables.join(NRTopNSignalKPIs, Seq("fileName", "dateWithoutMillis"), "left_outer")
      }
    }

    if (autotestMsgDf != null) {
      finalJoinedTables = finalJoinedTables.join(autotestMsgDf, Seq("fileName", "dateWithoutMillis"), "full_outer")
    }

    var joinedAllTables: DataFrame = null

    joinedAllTables = finalJoinedTables.join(logsTblDs, Seq("fileName"), "left_outer")

    // Should be taken from DB gateway response in HDFS copier. Meanwhile we're taking it from DB as workaround.
    val deviceOsDataframe = DBSink(spark, commonConfigParams)
      .query(
        s"select id, device_os from logs where id in ($testIds)")
      .getDataFrame
      .toDF("id", "deviceOS")
    joinedAllTables = joinedAllTables.join(deviceOsDataframe, Seq("id"), "left_outer")

    val sums = dmatEvents.map(colName => coalesce(colName.cast("Int"),lit(0)))

    var dmatEvtsTemp = joinedAllTables.withColumn("evtsCnt", sums.reduce(_ + _)).select($"testId", $"fileName", $"dateWithoutMillis", $"evtsCnt")
    var dmatEventsCntTable = dmatEvtsTemp.groupBy($"testId", $"fileName").agg(sum($"evtsCnt").cast("Int").alias("totalEventsCnt")).select($"fileName", $"totalEventsCnt")

    joinedAllTables = joinedAllTables.join(dmatEventsCntTable, Seq("fileName"), "left_outer")

    val logRecordJoinedDF: LogRecordJoinedDF = LogRecordJoinedDF(
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
      joinedAllTables,
      if(commonConfigParams.KPI_RTP_PACKET_CACHE) dmatIpDfOneSecAggDFCached else null
    )

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

}
