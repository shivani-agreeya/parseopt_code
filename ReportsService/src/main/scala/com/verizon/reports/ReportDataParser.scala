package com.verizon.reports

import org.apache.spark.sql.expressions.Window
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Row, SparkSession}
import com.dmat.qc.parser.ParseUtils
import com.verizon.reports.common.Constants

import scala.collection.immutable._
import scala.collection.mutable.Map
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

object ReportDataParser extends Serializable with LazyLogging {
  def parseLogData(spark: SparkSession,jobId: Long,fileName: String,processType: String,testId: Long,tableSuffix: String) = {
    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    import spark.implicits._
    import scala.collection.mutable.WrappedArray
    import org.apache.spark.sql.functions._
    logger.info(s"the table prefix fot the file >>>>> $fileName is $tableSuffix")
    val TBL_QCOMM_NAME: String=Constants.TBL_QCOMM_NAME+tableSuffix
    val TBL_QCOMM2_NAME: String=Constants.TBL_QCOMM2_NAME+tableSuffix
    val TBL_B192_NAME: String=Constants.TBL_B192_NAME+tableSuffix
    val TBL_B193_NAME: String=Constants.TBL_B193_NAME+tableSuffix
    val TBL_ASN_NAME: String=Constants.TBL_ASN_NAME+tableSuffix
    val TBL_NAS_NAME: String=Constants.TBL_NAS_NAME+tableSuffix
    val TBL_IP_NAME: String=Constants.TBL_IP_NAME+tableSuffix
    logger.info(s"TBL_QCOMM_NAME >>>>>>>>>>>>>>> $TBL_QCOMM_NAME ")
    logger.info(s"TBL_QCOMM2_NAME >>>>>>>>>>>>>>> $TBL_QCOMM2_NAME ")
    logger.info(s"TBL_B192_NAME >>>>>>>>>>>>>>> $TBL_B192_NAME ")
    logger.info(s"TBL_B193_NAME >>>>>>>>>>>>>>> $TBL_B193_NAME ")
    logger.info(s"TBL_ASN_NAME >>>>>>>>>>>>>>> $TBL_ASN_NAME ")
    logger.info(s"TBL_NAS_NAME >>>>>>>>>>>>>>> $TBL_NAS_NAME ")
    logger.info(s"TBL_IP_NAME >>>>>>>>>>>>>>> $TBL_IP_NAME ")
    val TBL_QCOMM5G_NAME: String=Constants.TBL_QCOMM5G_NAME+tableSuffix
    val qcomm1Query=spark.sql(s"""select * from (select fileName,dmTimeStamp, 
      last_value(ulFreq, true) over (partition by filename,dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) ulFreq,
      last_value(dlFreq, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dlFreq,
      last_value(ulcarrierIndex, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ulcarrierIndex,
      last_value(TB_Size0xB139, true) over(partition by filename, dmTimeStamp  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB139,
      last_value(currentSFNSF, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as currentSFNSF,
      last_value(pUSCHTxPower, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower,
      last_value(PUCCH_Tx_Power0xB16F, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as PUCCH_Tx_Power0xB16F,
      last_value(numRecords0xB16F, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB16F,
      last_value(pUSCHTxPower, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower,
      last_value(lteCellID, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteCellID,       
      last_value(cQICW0, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW0,
      last_value(numRecords0xB173, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB173,
      last_value(ServingCellIndex, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ServingCellIndex, 
      last_value(CrcResult, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CrcResult,
      last_value(modulationType, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modulationType,
      last_value(numPRB0xB173, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numPRB0xB173,
      last_value(TB_Size0xB139, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB139,
      last_value(numRecords0xB139, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB139,    
      last_value(modUL, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modUL, 
      last_value(prb, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as prb, 
      row_number() over(partition by filename, dmTimeStamp  order by dmTimeStamp desc) as rn 
      from (select * from dmat_logs.$TBL_QCOMM_NAME  
      where  filename =$fileName and testid=$testId)) where rn = 1 """)
    
    val b193Query = spark.sql(s"""select fileName,dmTimeStamp,collect_list(b193.PhysicalCellID) collectedB193_pci, 
    collect_list(b193.servingCellIndex_B193) collectedB193_servingCell ,
    collect_list(b193.InstMeasuredRSRP) collectedB193_rsrp,last_value(b193.version) as version_B193, 
    last_value(b193.SubPacketSize) as SubPacketSize_B193,b193.filename,b193.dmTimeStamp , 
    collect_list(b193.FTLSNRRx_0) as collectedB193_snir0, collect_list(b193.FTLSNRRx_1) as collectedB193_snir1,
    collect_list(b193.InstRSRQ) as collectedB193_rsrq , 
    collect_list(InstRSSI) as collectedB193_rssi, 
    collect_list(EARFCN_B193) as collectedB193_earfcn
    from  dmat_logs.$TBL_B193_NAME  b193  
    where b193.fileName=$fileName and testid=$testId group by b193.fileName,b193.dmTimeStamp""")
    
    val dmatNas = spark.sql(s""" select * from (select fileName,dmTimeStamp, 
    sum(lteActDefContAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteActDefContAcptCnt,     
    sum(lteActDedContAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteActDedContAcptCnt,
    sum(lteEsmModifyEpsAccpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteEsmModifyEpsAccptCnt,
    last_value(lteAttRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteAttRej,
    sum(lteActDefContRej) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteActDefContRejCnt,
    sum(lteActDedContRej) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteActDedContRejCnt,
    sum(lteEsmModifyEpsRej) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteEsmModifyEpsRejCnt,
    sum(ltePdnConntReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as ltePdnConntReqCnt,
    sum(lteAttReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteAttReqCnt,
    sum(lteAttAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteAttAcptCnt,
    sum(lteDetachReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteDetachReqCnt,
    last_value(lteTaRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteTaRej,
    sum(lteAttCmp) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteAttCmpCnt, 
    sum(lteTaReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteTaReqCnt,
    sum(lteTaAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteTaAcptCnt,
    row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn    
    from (select * from dmat_logs.$TBL_NAS_NAME where filename=$fileName and testid=$testId) )
    where rn=1
    """)

    val dmatAsn =spark.sql(s"""
              select * from (select fileName,dmTimeStamp,
              sum(lteRrcConRestReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConRestReqCnt,
              last_value(lteRrcConRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRej, 
              last_value(lteRrcConRestRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRestRej, 
              sum(lteRrcConRel) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConRelCnt,
              sum(lteRrcConSetup) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConSetupCnt,
              sum(lteRrcConReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConReqCnt, 
              sum(lteRrcConSetupCmp) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConSetupCmpCnt,
              sum(lteRrcConReconfig) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConReconfigCnt,
              row_number() over(partition by filename, dmTimeStamp order by dmTimeStamp desc) as rn 
              from (select * from dmat_logs.$TBL_ASN_NAME where filename=$fileName and testid=$testId)) 
              where rn = 1
             """)
    
    val mergeList1 = udf { (strings: WrappedArray[WrappedArray[Integer]]) => strings.map(x => x.mkString(",")).mkString(",") }
    val mergeList2 = udf { (strings: WrappedArray[WrappedArray[Double]]) => strings.map(x => x.mkString(",")).mkString(",") }
    val mergeList3 = udf { (strings: WrappedArray[WrappedArray[String]]) => strings.map(x => x.mkString(",")).mkString(",") }
    // val mergeList4 = udf { (strings: WrappedArray[String]) => strings.map(x => x.mkString(",")).mkString(",") }
    val mergeList4 = udf { (strings: WrappedArray[String]) => strings.mkString(",") }

    val b193Query_merged = b193Query.withColumn("PhysicalCellID_B193", mergeList1($"collectedB193_pci"))
      .withColumn("ServingCellIndex_B193", mergeList3($"collectedB193_servingCell"))
      .withColumn("InstMeasuredRSRP", mergeList2($"collectedB193_rsrp"))
      .withColumn("FTLSNRRx_0", mergeList2($"collectedB193_snir0"))
      .withColumn("FTLSNRRx_1", mergeList2($"collectedB193_snir1"))
      .withColumn("InstRSRQ", mergeList2($"collectedB193_rsrq"))
      .withColumn("InstRSSI", mergeList2($"collectedB193_rssi"))
      .withColumn("EARFCN", mergeList1($"collectedB193_earfcn"))
      
    val qcomm2Query=spark.sql(s"""select * from (select fileName,dmTimeStamp, 
      last_value(MacDlNumSamples, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlNumSamples,
      last_value(MacDlTbs, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlTbs,
      last_value(MacDlSfn, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlSfn,
      last_value(MacDlSubSfn, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlSubSfn,
      last_value(MacUlSfn, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlSfn, 
      last_value(MacUlSubSfn, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlSubSfn,
      last_value(MacUlNumSamples, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlNumSamples,  
      last_value(MacUlGrant, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlGrant,
      last_value(lteRrcState, true) over(partition by filename order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcState,  
      last_value(HoFailure, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as HoFailure,
      last_value(lteIntraHoFail, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteIntraHoFail, 
      last_value(lteReselFromGsmUmtsFail, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteReselFromGsmUmtsFail, 
      row_number() over(partition by filename, dmTimeStamp  order by dmTimeStamp desc) as rn 
      from (select * from dmat_logs.$TBL_QCOMM2_NAME 
      where  filename =$fileName and testid=$testId)) where rn = 1 """)
      
      val dmatB192 = 
        spark.sql(s""" 
        select fileName,max(dmTimeStamp) as dmTimeStamp,
        collect_list(InstMeasuredRSRP) as collected_rsrp,
        collect_list(InstRSRQ) as collected_rsrq,
        collect_list(InstRSSI) as collected_rssi,
        collect_list(PhysicalCellID) as collected_pci                          
        from (select * from dmat_logs.$TBL_B192_NAME where
        filename = $fileName 
        sort by fileName, dmTimeStamp desc) group by fileName,dmTimeStamp 
        """)
      val dmatB192_merged = dmatB192
      .withColumn("PCI_collected", mergeList1($"collected_pci"))
      .withColumn("Rsrp_collected", mergeList2($"collected_rsrp"))
      .withColumn("Rsrq_collected", mergeList2($"collected_rsrq"))
      .withColumn("Rssi_collected", mergeList2($"collected_rssi"))
      
      
   val pdcpRlcTputDFTemp=spark.sql(s"""select * from (select fileName,dmTimeStamp,
               last_value(rlcDlTotalBytes, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTotalBytes  
              ,last_value(rlcDlTimeStamp, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTimeStamp
              ,last_value(rlcUlTotalBytes, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcUlTotalBytes
              ,last_value(rlcUlTimeStamp, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcUlTimeStamp
              from (select * from dmat_logs.$TBL_QCOMM2_NAME where filename=$fileName and testid=$testId))""")   
    val windPdcpRlc = Window.partitionBy($"fileName").orderBy($"dmTimeStamp".asc)
    val pdcpRlcTputDF = pdcpRlcTputDFTemp
    .withColumn("prerlcDlTotalBytes", lag("rlcDlTotalBytes", 1).over(windPdcpRlc))
    .withColumn("prerlcDlTimeStamp", lag("rlcDlTimeStamp", 1).over(windPdcpRlc))
    .withColumn("prerlcUlTotalBytes", lag("rlcUlTotalBytes", 1).over(windPdcpRlc))
    .withColumn("prerlcUlTimeStamp", lag("rlcUlTimeStamp", 1).over(windPdcpRlc))
   val pdcpRlcTputDFCount= pdcpRlcTputDF.count() 
   val kpi0xB193ExistsQuery = b193Query_merged.count() > 0
   var joinedQuery= b193Query_merged.join(qcomm1Query,Seq("filename","dmtimestamp"),"full_outer")
                                    .join(qcomm2Query,Seq("filename","dmtimestamp"),"full_outer")
                                    .join(dmatNas, Seq("fileName", "dmTimeStamp"), "full_outer") 
                                    .join(dmatB192_merged, Seq("fileName", "dmTimeStamp"), "full_outer")
  joinedQuery=if(pdcpRlcTputDFCount>0) joinedQuery.join(pdcpRlcTputDF, Seq("fileName", "dmTimeStamp"), "full_outer") else  joinedQuery 
  var kpiDmatAsnQuery: Boolean=false
  joinedQuery=if (dmatAsn.count() > 0) {
    kpiDmatAsnQuery = true
    joinedQuery.join(dmatAsn, Seq("fileName", "dmTimeStamp"), "full_outer")
  }else{
    joinedQuery
  }
   
  if (joinedQuery == null || joinedQuery.count == 0) {
        logger.info("No data available for file " + fileName + " and the process type is " + processType)
        throw new ReportDataException("No data available for file " + fileName + " and the process type is " + processType+ " job-id"+jobId)
  }
  //joinedQuery.show()
   
  val reportRDD=joinedQuery.mapPartitions(iter => {
	iter.map(row => {
	  //fileName
	  val fileName=if (!row.isNullAt(row.fieldIndex("fileName"))) row.getString(row.fieldIndex("fileName")) else ""
		var ltePccRsrp: Double= 0.0
		var ltePccSinr=0.0
		var ltePccRsrq=0.0
		var ltePccRssi=0.0
		var ltePccBandInd=0
		val ulFreq: Int = if (!row.isNullAt(row.fieldIndex("ulFreq"))) row.getInt(row.fieldIndex("ulFreq")) else 0
    val dlFreq: Int = if (!row.isNullAt(row.fieldIndex("dlFreq"))) row.getInt(row.fieldIndex("dlFreq")) else 0
    
    
    val numRecords0xB16F: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB16F"))) row.getInt(row.fieldIndex("numRecords0xB16F")) else 0 
    var PUCCH_Tx_Power0xB16F: Int = if (!row.isNullAt(row.fieldIndex("PUCCH_Tx_Power0xB16F")) && row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).size() >= numRecords0xB16F) row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).get(numRecords0xB16F - 1).asInstanceOf[Int] else 0
    val ltePucchActTxPwr: Int=  if (PUCCH_Tx_Power0xB16F != -128) PUCCH_Tx_Power0xB16F else Constants.DEFAULT_KPI_INTEGER_VAL
    
    val PuschTxPwr: Integer = if (!row.isNullAt(row.fieldIndex("pUSCHTxPower")) && row.getList(row.fieldIndex("pUSCHTxPower")).size() > 0) row.getList(row.fieldIndex("pUSCHTxPower")).get(0) else null
    val ltePuschTxPwr: Int=if (PuschTxPwr != null && PuschTxPwr < 24) PuschTxPwr else Constants.DEFAULT_KPI_INTEGER_VAL
    
    
    val ulcarrierIndex: String = if (!row.isNullAt(row.fieldIndex("ulcarrierIndex")) && row.getList(row.fieldIndex("ulcarrierIndex")).size() > 0) row.getList(row.fieldIndex("ulcarrierIndex")).get(0).toString else ""
    val ModulationUl: String = if (!row.isNullAt(row.fieldIndex("modUL")) && row.getList(row.fieldIndex("modUL")).size() > 0) row.getList(row.fieldIndex("modUL")).get(0).toString else null  
    val PushPrb: Integer = if (!row.isNullAt(row.fieldIndex("prb")) && row.getList(row.fieldIndex("prb")).size() > 0) row.getList(row.fieldIndex("prb")).get(0) else null
    //val PuschTxPwr: Int = if (!row.isNullAt(row.fieldIndex("pUSCHTxPower")) && row.getList(row.fieldIndex("pUSCHTxPower")).size() > 0) row.getList(row.fieldIndex("pUSCHTxPower")).get(0) else 999999
   var ltePccPuschTxPwr: Int = Constants.DEFAULT_KPI_INTEGER_VAL
    var lteModUl: String = "Not found"
    var ltePuschPrb: Int = Constants.DEFAULT_KPI_INTEGER_VAL
    if (ulcarrierIndex.isEmpty) {
              ltePccPuschTxPwr= if (PuschTxPwr != null && PuschTxPwr < 24)  PuschTxPwr else Constants.DEFAULT_KPI_INTEGER_VAL
              lteModUl= if (ModulationUl != null)  ModulationUl else "Not found"
              ltePuschPrb= if (PushPrb != null)PushPrb else Constants.DEFAULT_KPI_INTEGER_VAL  
    }else if (PuschTxPwr != null && ("PCC".equalsIgnoreCase(ulcarrierIndex) || "SCC1".equalsIgnoreCase(ulcarrierIndex) )){
      ltePccPuschTxPwr=if(PuschTxPwr < 24)  PuschTxPwr else Constants.DEFAULT_KPI_INTEGER_VAL
      lteModUl=if (ModulationUl != null)  ModulationUl else "Not found"
      ltePuschPrb= if (PushPrb != null)PushPrb else Constants.DEFAULT_KPI_INTEGER_VAL  
    }else{
      ltePccPuschTxPwr = Constants.DEFAULT_KPI_INTEGER_VAL
      lteModUl="Not found"
      ltePuschPrb = Constants.DEFAULT_KPI_INTEGER_VAL
    }
		
		//lteMacTputDl start 
		val macDlNumSamples: Integer = if (!row.isNullAt(row.fieldIndex("MacDlNumSamples"))) row.getInt(row.fieldIndex("MacDlNumSamples")) else null
		val macDlTbs: List[Integer] = if (!row.isNullAt(row.fieldIndex("MacDlTbs"))) row.getSeq(row.fieldIndex("MacDlTbs")).toList else null
		val macDlfirstSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn")) && row.getList(row.fieldIndex("MacDlSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSfn")).get(0) else null
		val macDlFirstSubSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn")) && row.getList(row.fieldIndex("MacDlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSubSfn")).get(0) else null
		val macDlSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn"))) row.getList(row.fieldIndex("MacDlSfn")).size() else null
		val macDlLastSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn")) && row.getList(row.fieldIndex("MacDlSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSfn")).get(macDlSfnSize - 1) else null
		val macDlSubSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn"))) row.getList(row.fieldIndex("MacDlSubSfn")).size() else null
		val macDlLastSubSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn")) && row.getList(row.fieldIndex("MacDlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSubSfn")).get(macDlSubSfnSize - 1) else null
		var totalDlTbs = 0D

        if (macDlNumSamples != null && macDlTbs != null) {
          //logger.info("0xB063 PDMAT: macDlNumSamples = " + macDlNumSamples)
          try {
            for (i <- 0 to macDlNumSamples - 1) {
              if (Try(macDlTbs(i)).isSuccess) {
                totalDlTbs += macDlTbs(i)
              }
            }
            //logger.info("0xB063 PDMAT: totalDlTbs = " + totalDlTbs)
          }
          catch {
            case iobe: IndexOutOfBoundsException =>
              logger.error("IndexOutOfBoundsException exception occured for operation numSamplesDl&DlTbs : " + iobe.getMessage)
          }
        }
		
		var macDlThroughput: Double = 0

    if ((macDlfirstSfn != null && macDlFirstSubSfn != null) && (macDlLastSfn != null && macDlLastSubSfn != null)) {
          //logger.info("0xB063 PDMAT: macDlfirstSfn = " + macDlfirstSfn + "; macDlFirstSubSfn = " + macDlFirstSubSfn + "; macDlLastSfn = " + macDlLastSfn + "; macDlLastSubSfn = " + macDlLastSubSfn)
          val timeSpan = ParseUtils.CalculateMsFromSfnAndSubFrames(macDlfirstSfn, macDlFirstSubSfn, macDlLastSfn, macDlLastSubSfn)

          //logger.info("0xB063 PDMAT: timeSpan = " + timeSpan + "; totalDlTbs = " + totalDlTbs)

          if (totalDlTbs > 0) {
            macDlThroughput = getThrouput(totalDlTbs, timeSpan)
            //logger.info("0xB063 PDMAT: macDlThroughput = " + macDlThroughput)
          }
    }
    val lteMacTputDl: Double=if (macDlThroughput > 0) macDlThroughput else Constants.DEFAULT_KPI_DOUBLE_VAL
    //lteMacTputDl end
    
    //lteMacTputUl start
    val macUlFirstSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn")) && row.getList(row.fieldIndex("MacUlSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSfn")).get(0) else null
    val macUlFirstSubFn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn")) && row.getList(row.fieldIndex("MacUlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSubSfn")).get(0) else null
    val macUlSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn"))) row.getList(row.fieldIndex("MacUlSfn")).size() else null
    val macUlLastSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn")) && row.getList(row.fieldIndex("MacUlSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSfn")).get(macUlSfnSize - 1) else null
    val macUlSubSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn"))) row.getList(row.fieldIndex("MacUlSubSfn")).size() else null
    val macUlLastSubFn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn")) && row.getList(row.fieldIndex("MacUlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSubSfn")).get(macUlSubSfnSize - 1) else null
    val macUlNumSamples: Integer = if (!row.isNullAt(row.fieldIndex("MacUlNumSamples"))) row.getInt(row.fieldIndex("MacUlNumSamples")) else null
    val macUlGrant: List[Integer] = if (!row.isNullAt(row.fieldIndex("MacUlGrant"))) row.getSeq(row.fieldIndex("MacUlGrant")).toList else null
    var totalUlGrant = 0D

    if (macUlNumSamples != null && macUlGrant != null) {
    try {
            for (i <- 0 to macUlNumSamples - 1) {
              if (Try(macUlGrant(i)).isSuccess) {
                totalUlGrant += macUlGrant(i)
              }
            }
          }
          catch {
            case iobe: IndexOutOfBoundsException =>
              logger.error("IndexOutOfBoundsException exception occured for operation numSamplesUl&UlGrant : " + iobe.getMessage)
          }
    }
    var macUIThroughput: Double = 0
    if ((macUlFirstSfn != null && macUlFirstSubFn != null) && (macUlLastSfn != null && macUlLastSubFn != null)) {
          //logger.info("0xB064 PDMAT: macUlFirstSfn = " + macUlFirstSfn + "; macUlFirstSubFn = " + macUlFirstSubFn + "; macUlLastSfn = " + macUlLastSfn + "; macUlLastSubFn = " + macUlLastSubFn)

          val timeSpan = ParseUtils.CalculateMsFromSfnAndSubFrames(macUlFirstSfn, macUlFirstSubFn, macUlLastSfn, macUlLastSubFn)

          //logger.info("0xB064 PDMAT: timeSpan = " + timeSpan + "; totalUlGrant = " + totalUlGrant)

          if (totalUlGrant > 0) {
            macUIThroughput = getThrouput(totalUlGrant, timeSpan)
            //logger.info("0xB064 PDMAT: macUlThroughput = " + macUIThroughput)
          }
    }
    val lteMacTputUl=if (macUIThroughput > 0) macUIThroughput else Constants.DEFAULT_KPI_DOUBLE_VAL
    
    //lteMacTputUl end
    //lteRlcTputDl Start
    import java.lang.Long
    var rlcDlTotalBytes: Long = 0L
    var prerlcDlTotalBytes: Long =0L
    var rlcDlTimeStamp: Long= 0L
    var prerlcDlTimeStamp: Long= 0L
    
    if(pdcpRlcTputDFCount>0){
      rlcDlTotalBytes = if (!row.isNullAt(row.fieldIndex("rlcDlTotalBytes"))) row.getLong(row.fieldIndex("rlcDlTotalBytes")) else null
      prerlcDlTotalBytes = if (!row.isNullAt(row.fieldIndex("prerlcDlTotalBytes"))) row.getLong(row.fieldIndex("prerlcDlTotalBytes")) else null
      rlcDlTimeStamp = if (!row.isNullAt(row.fieldIndex("rlcDlTimeStamp"))) row.getLong(row.fieldIndex("rlcDlTimeStamp")) else null
      prerlcDlTimeStamp = if (!row.isNullAt(row.fieldIndex("prerlcDlTimeStamp"))) row.getLong(row.fieldIndex("prerlcDlTimeStamp")) else null
    }
     var dlRlcTrupt: Double = 0D
     try {
          if (rlcDlTotalBytes != null && prerlcDlTotalBytes != null && rlcDlTimeStamp != null && prerlcDlTimeStamp != null && (rlcDlTimeStamp > prerlcDlTimeStamp) && (rlcDlTotalBytes > prerlcDlTotalBytes)) {

            // logger.info("rlc: DlB = " + rlcDlTotalBytes + "; pDlTB = " + prerlcDlTotalBytes + "; TS = " + rlcDlTimeStamp + "; preTS = " + prerlcDlTimeStamp)
            var timeDiff = 0D
            var rlcDlBytesDiff = 0D
            timeDiff = (rlcDlTimeStamp.doubleValue() - prerlcDlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            rlcDlBytesDiff = rlcDlTotalBytes.doubleValue() - prerlcDlTotalBytes.doubleValue()

            dlRlcTrupt = (rlcDlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
     } catch {
          case e: Exception =>
            logger.error("Error: Exception on rlc dl tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("rlc: Error DlB = " + rlcDlTotalBytes + "; pDlTB = " + prerlcDlTotalBytes + "; TS = " + rlcDlTimeStamp + "; preTS = " + prerlcDlTimeStamp)
     }
    val lteRlcTputDl=if (dlRlcTrupt > 0)  dlRlcTrupt else Constants.DEFAULT_KPI_DOUBLE_VAL
    //lteRlcTputDl end
    //LTERlcTputUl start
    
    var ulRlcTrupt: Double = 0
    var rlcUlTotalBytes: java.lang.Long = null
    var prerlcUlTotalBytes: java.lang.Long = null
    var rlcUlTimeStamp: java.lang.Long = null
    var prerlcUlTimeStamp: java.lang.Long = null
    if(pdcpRlcTputDFCount>0){
      rlcUlTotalBytes = if (!row.isNullAt(row.fieldIndex("rlcUlTotalBytes"))) row.getLong(row.fieldIndex("rlcUlTotalBytes")) else null
      prerlcUlTotalBytes = if (!row.isNullAt(row.fieldIndex("prerlcUlTotalBytes"))) row.getLong(row.fieldIndex("prerlcUlTotalBytes")) else null
      rlcUlTimeStamp = if (!row.isNullAt(row.fieldIndex("rlcUlTimeStamp"))) row.getLong(row.fieldIndex("rlcUlTimeStamp")) else null
      prerlcUlTimeStamp = if (!row.isNullAt(row.fieldIndex("prerlcUlTimeStamp"))) row.getLong(row.fieldIndex("prerlcUlTimeStamp")) else null
    }
    try {
          if (rlcUlTotalBytes != null && prerlcUlTotalBytes != null && rlcUlTimeStamp != null && prerlcUlTimeStamp != null && (rlcUlTimeStamp > prerlcUlTimeStamp) && (rlcUlTotalBytes > prerlcUlTotalBytes)) {

            // logger.info("PDCP: DlB = " + rlcUlTotalBytes + "; pDlTB = " + prerlcUlTotalBytes + "; TS = " + rlcUlTimeStamp + "; preTS = " + prerlcUlTimeStamp)

            var timeDiff = 0D
            var rlcUlBytesDiff = 0D
            timeDiff = (rlcUlTimeStamp.doubleValue() - prerlcUlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            rlcUlBytesDiff = rlcUlTotalBytes.doubleValue() - prerlcUlTotalBytes.doubleValue()

            ulRlcTrupt = (rlcUlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
    } catch {
          case e: Exception =>
            logger.error("Error: Exception on pdcp dl tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("RLC: Error DlB = " + rlcUlTotalBytes + "; pDlTB = " + prerlcUlTotalBytes + "; TS = " + rlcUlTimeStamp + "; preTS = " + prerlcUlTimeStamp)
    }
    val lteRlcTputUl: Double=if (ulRlcTrupt > 0)  ulRlcTrupt else Constants.DEFAULT_KPI_DOUBLE_VAL
    //LTERlcTputUl end
    
    //LTERrcState start
    val LTE_RRC_State: String = if (!row.isNullAt(row.fieldIndex("lteRrcState"))) row.getString(row.fieldIndex("lteRrcState")) else null
    val lteRrcState: String=if (LTE_RRC_State != null && !LTE_RRC_State.isEmpty) LTE_RRC_State else "Not Found"
    
    //LTERrcState end
      
    //LTEHoFailure start
    val lteHoFailure: Int=if (!row.isNullAt(row.fieldIndex("HoFailure"))) row.getInt(row.fieldIndex("HoFailure")) else Constants.DEFAULT_KPI_INTEGER_VAL 
    
    //LTEHoFailure end
    //lteIntraHoFail start
    val lteIntraHoFail: Int=if (!row.isNullAt(row.fieldIndex("lteIntraHoFail")))row.getInt(row.fieldIndex("lteIntraHoFail")) else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteIntraHoFail end
    
    //lteReselFromGsmUmtsFail start
    val lteReselFromGsmUmtsFail: Int=if (!row.isNullAt(row.fieldIndex("lteReselFromGsmUmtsFail")))  row.getInt(row.fieldIndex("lteReselFromGsmUmtsFail")) else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteReselFromGsmUmtsFail end
    //lteActDefContAcptCnt start
    val lteActDefContAcptCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDefContAcptCnt"))) row.getLong(row.fieldIndex("lteActDefContAcptCnt"))  else null
    val lteActDefContAcptCnt: Int = if(lteActDefContAcptCount!=null) lteActDefContAcptCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL 
    //lteActDefContAcptCnt end
    //lteActDedContAcptCnt start
    val lteActDedContAcptCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDedContAcptCnt"))) row.getLong(row.fieldIndex("lteActDedContAcptCnt")) else null
    val lteActDedContAcptCnt: Int = if(lteActDedContAcptCount!=null) lteActDedContAcptCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL 
    //lteActDedContAcptCnt end
    //lteEsmModifyEpsAccptCnt start
    val lteEsmModifyEpsAccptCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmModifyEpsAccptCnt"))) row.getLong(row.fieldIndex("lteEsmModifyEpsAccptCnt")) else null
    val lteBearerModAcptCnt: Int = if(lteEsmModifyEpsAccptCount!=null) lteEsmModifyEpsAccptCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL 
    //lteEsmModifyEpsAccptCnt end
    
    //lteAttRej start
    val lteAttRej: Int = if (!row.isNullAt(row.fieldIndex("lteAttRej"))) row.getInt(row.fieldIndex("lteAttRej")) else Constants.DEFAULT_KPI_INTEGER_VAL 
    
    //lteAttRej end
    //lteActDefContRejCnt start
    val lteActDefContRejCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDefContRejCnt"))) row.getLong(row.fieldIndex("lteActDefContRejCnt")) else null
    val lteActDefContRejCnt: Int =if (lteActDefContRejCount != null)  lteActDefContRejCount.toInt else  Constants.DEFAULT_KPI_INTEGER_VAL 
    //lteActDefContRejCnt end
    //lteActDedContRejCnt start
    val lteActDedContRejCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDedContRejCnt"))) row.getLong(row.fieldIndex("lteActDedContRejCnt")) else null
    val lteActDedContRejCnt: Int=if (lteActDedContRejCount != null)  lteActDedContRejCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteActDedContRejCnt end
    //lteBearerModRejCnt start
    val lteEsmModifyEpsRejCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmModifyEpsRejCnt"))) row.getLong(row.fieldIndex("lteEsmModifyEpsRejCnt")) else null
    val lteBearerModRejCnt:Int=if (lteEsmModifyEpsRejCount != null)  lteEsmModifyEpsRejCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteBearerModRejCnt end
    //ltePdnConntReqCnt start
    val ltePdnConntReqount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("ltePdnConntReqCnt"))) row.getLong(row.fieldIndex("ltePdnConntReqCnt")) else null
    val ltePdnConntReqCnt: Int=if (ltePdnConntReqount != null) ltePdnConntReqount.intValue()  else Constants.DEFAULT_KPI_INTEGER_VAL
    //ltePdnConntReqCnt end
    //lteAttReqCnt start
    val lteAttReqCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttReqCnt"))) row.getLong(row.fieldIndex("lteAttReqCnt")) else null
    val lteAttReqCnt: Int=if (lteAttReqCount != null) lteAttReqCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteAttReqCnt end
    //lteAttAcptCnt start
    val lteAttAcptCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttAcptCnt"))) row.getLong(row.fieldIndex("lteAttAcptCnt")) else null
    val lteAttAcptCnt: Int=if (lteAttAcptCount != null)  lteAttAcptCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteAttAcptCnt end
    //lteDetachReqCnt start
    
    val lteDetachReqCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteDetachReqCnt"))) row.getLong(row.fieldIndex("lteDetachReqCnt")) else null
    val lteDetachReqCnt: Int=if (lteDetachReqCount != null) lteDetachReqCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteDetachReqCnt end
    //lteTaRej start
    val lteTaRej: Integer = if (!row.isNullAt(row.fieldIndex("lteTaRej"))) row.getInt(row.fieldIndex("lteTaRej")) else null
    val LTETaRej: Int=if (lteTaRej != null) lteTaRej else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteTaRej end
    //lteAttCmpCnt start
    val lteAttCmpCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttCmpCnt"))) row.getLong(row.fieldIndex("lteAttCmpCnt")) else null
    val lteAttCmpCnt: Int= if(lteAttCmpCount != null)  lteAttCmpCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteAttCmpCnt end
    //lteTaReqCnt start
    val lteTaReqCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteTaReqCnt"))) row.getLong(row.fieldIndex("lteTaReqCnt")) else null
    val lteTaReqCnt: Int=if (lteTaReqCount != null)  lteTaReqCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteTaReqCnt end
    //lteTaAcptCnt start
    val lteTaAcptCount: java.lang.Long = if (!row.isNullAt(row.fieldIndex("lteTaAcptCnt"))) row.getLong(row.fieldIndex("lteTaAcptCnt")) else null
    val lteTaAcptCnt:Int=if (lteTaAcptCount != null) lteTaAcptCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //lteTaAcptCnt end
    //All ASN   kpis go here
    var lteRrcConRestReqCount: java.lang.Long = null
    var lteRrcConRej: Integer = null
    var lteRrcConRestRej: Integer = null
    var lteRrcConRelCount: java.lang.Long = null
    var lteRrcConSetupCount: java.lang.Long = null
    var lteRrcConReqCount: java.lang.Long = null
    var lteRrcConSetupCmpCount: java.lang.Long = null
    var lteRrcConReconfigCount: java.lang.Long = null
    if (kpiDmatAsnQuery) {
         lteRrcConRestReqCount = if (!row.isNullAt(row.fieldIndex("lteRrcConRestReqCnt"))) row.getLong(row.fieldIndex("lteRrcConRestReqCnt")) else null
         lteRrcConRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRej"))) row.getInt(row.fieldIndex("lteRrcConRej")) else null
         lteRrcConRestRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRestRej"))) row.getInt(row.fieldIndex("lteRrcConRestRej")) else null
         lteRrcConRelCount = if (!row.isNullAt(row.fieldIndex("lteRrcConRelCnt"))) row.getLong(row.fieldIndex("lteRrcConRelCnt")) else null
         lteRrcConSetupCount = if (!row.isNullAt(row.fieldIndex("lteRrcConSetupCnt"))) row.getLong(row.fieldIndex("lteRrcConSetupCnt")) else null
         lteRrcConReqCount = if (!row.isNullAt(row.fieldIndex("lteRrcConReqCnt"))) row.getLong(row.fieldIndex("lteRrcConReqCnt")) else null
         lteRrcConSetupCmpCount = if (!row.isNullAt(row.fieldIndex("lteRrcConSetupCmpCnt"))) row.getLong(row.fieldIndex("lteRrcConSetupCmpCnt")) else null
         lteRrcConReconfigCount = if (!row.isNullAt(row.fieldIndex("lteRrcConReconfigCnt"))) row.getLong(row.fieldIndex("lteRrcConReconfigCnt")) else null
    }
    val lteRrcConRestReqCnt: Int=if (lteRrcConRestReqCount != null) lteRrcConRestReqCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    val LTERrcConRej: Int=if (lteRrcConRej != null) lteRrcConRej else Constants.DEFAULT_KPI_INTEGER_VAL
    val LTERrcConRestRej: Int=if (lteRrcConRestRej != null)  lteRrcConRestRej else Constants.DEFAULT_KPI_INTEGER_VAL
    val lteRrcConRelCnt: Int=if (lteRrcConRelCount != null)  lteRrcConRelCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    val lteRrcConSetupCnt: Int=if (lteRrcConSetupCount != null)  lteRrcConSetupCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    val lteRrcConReqCnt: Int=if (lteRrcConReqCount != null)  lteRrcConReqCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    val lteRrcConSetupCmpCnt: Int=if (lteRrcConSetupCmpCount != null)  lteRrcConSetupCmpCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    val lteRrcConReconfigCnt: Int=if (lteRrcConReconfigCount != null)  lteRrcConReconfigCount.toInt else Constants.DEFAULT_KPI_INTEGER_VAL
    //All ASN   kpis go here end
    
    
    //ALL b192 go here  start
    val PciNcell: String = if (!row.isNullAt(row.fieldIndex("PCI_collected"))) row.getString(row.fieldIndex("PCI_collected")) else null
    
    var NCell10RSRP: Double = Constants.DEFAULT_KPI_DOUBLE_VAL
    var NCell10RSRQ: Double = Constants.DEFAULT_KPI_DOUBLE_VAL
    var NCell10RSSI: Double = Constants.DEFAULT_KPI_DOUBLE_VAL
    
    if (PciNcell != null && !PciNcell.isEmpty) {
            var Ncells = PciNcell.split(",").map(_.trim).toList.map(_.toInt)
            //var Earfcn = EarfcnNcell.split(",").map(_.trim).toList.map(_.toInt)
            val RrsrpNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrp_collected"))) row.getString(row.fieldIndex("Rsrp_collected")) else null
            val RsrqNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrq_collected"))) row.getString(row.fieldIndex("Rsrq_collected")) else null
            val RssiNcell: String = if (!row.isNullAt(row.fieldIndex("Rssi_collected"))) row.getString(row.fieldIndex("Rssi_collected")) else null
            
            breakable {
              for (i <- 1 to Ncells.size) {
                if (i > 20) {
                  break
                }
                
                  if(i==10){
                    NCell10RSRP = if (RrsrpNcell != null && !RrsrpNcell.isEmpty)  RrsrpNcell.split(",")(i - 1).toDouble else Constants.DEFAULT_KPI_DOUBLE_VAL
                  
                    NCell10RSRQ = if (RsrqNcell != null && !RsrqNcell.isEmpty)  RsrqNcell.split(",")(i - 1).toDouble else Constants.DEFAULT_KPI_DOUBLE_VAL
                  
                    NCell10RSSI = if (RssiNcell != null && !RssiNcell.isEmpty)RssiNcell.split(",")(i - 1).toDouble else Constants.DEFAULT_KPI_DOUBLE_VAL
                  }
              }
            }
            

          }
    //ALL b192 end
    val LteCellID: Int = if (!row.isNullAt(row.fieldIndex("lteCellID"))) row.getInt(row.fieldIndex("lteCellID")) else 0
    val lteCellId: Int= if(LteCellID != 0) LteCellID else Constants.DEFAULT_KPI_INTEGER_VAL
    
    val cqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("cQICW0"))) row.getInt(row.fieldIndex("cQICW0")) else null
    val ltePccCqiCw0: Int=if (cqiCw0 != null)  cqiCw0 else Constants.DEFAULT_KPI_INTEGER_VAL
    
    val numRecords0xB173: Integer = if (!row.isNullAt(row.fieldIndex("numRecords0xB173"))) row.getInt(row.fieldIndex("numRecords0xB173")) else null
    val servingCellIndex: List[String] = if (!row.isNullAt(row.fieldIndex("ServingCellIndex"))) row.getSeq(row.fieldIndex("ServingCellIndex")).toList else null
    val CRCResult: List[String] = if (!row.isNullAt(row.fieldIndex("CrcResult"))) row.getSeq(row.fieldIndex("CrcResult")).toList else null
    val modulationType: List[String] = if (!row.isNullAt(row.fieldIndex("modulationType")) && row.getList(row.fieldIndex("modulationType")).size() > 0) 
      row.getSeq(row.fieldIndex("modulationType")).toList else null
    val numRecords0xB139: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB139"))) row.getInt(row.fieldIndex("numRecords0xB139")) else 0
    var pDSCHpccModType: Integer = null
     try {
          if (numRecords0xB173 != null && numRecords0xB173 > 0) {
             for (i <- 0 to numRecords0xB173 - 1) {
              if (servingCellIndex == null) {
                 
                  pDSCHpccModType = getModulationType(modulationType(0))
                }else{
                  servingCellIndex(i).toUpperCase match {
                      case "PCC" =>
                        if (CRCResult(i).contentEquals("Pass")) {
                          pDSCHpccModType = getModulationType(modulationType(0))
                        }
                      case "PCELL" =>
                        if (CRCResult(i).contentEquals("Pass")) {
                          pDSCHpccModType = getModulationType(modulationType(0))
                        }
                  }
                }
             }
          }
     }catch{
        case e: Exception =>
            logger.error("Error: Exception during phy throughput calculation: " + row.getString(row.fieldIndex("fileName")))
            //logger.error("ERRORRRRRRRRR!!!!!!!!",e)
     }
    val ltePccDlMcs:Int= if (pDSCHpccModType != null) pDSCHpccModType else Constants.DEFAULT_KPI_INTEGER_VAL
    
    val numPRB0xB173: List[Int] = if (!row.isNullAt(row.fieldIndex("numPRB0xB173"))) row.getSeq(row.fieldIndex("numPRB0xB173")).toList else null
    
    val ltePdschPrb:String = if(numPRB0xB173 != null && numPRB0xB173.size > 0) {
      logger.info(">>>>>>> numPRB0xB173##########"+numPRB0xB173+" numPRB0xB173 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+numPRB0xB173.getClass)
      logger.info(">>>>>>> numPRB0xB173############"+numPRB0xB173.head+">>>>>>> numPRB0xB173############"+numPRB0xB173.head.getClass)
      numPRB0xB173.head.toString() 
    }else {
      "Not found"
    }
    
    val tbsize0xB139: List[Int] = if (!row.isNullAt(row.fieldIndex("TB_Size0xB139"))) row.getSeq(row.fieldIndex("TB_Size0xB139")).toList else null  
    val currentSFNSF: List[Int] = if (!row.isNullAt(row.fieldIndex("currentSFNSF"))) row.getSeq(row.fieldIndex("currentSFNSF")).toList else null
    var pUSCHSumPcc: Int = 0
    var pUSCHPccTPUT: Double = 0D
    var pUSCHSumScc1: Int = 0
    var pUSCHScc1TPUT: Double = 0D
    var overallPUSCHTPUT: Double = 0D
     if (tbsize0xB139 != null && currentSFNSF != null) {

          if (currentSFNSF.nonEmpty) {
            

            for (i <- 0 to tbsize0xB139.size - 1) {
              if (ulcarrierIndex != null) {

                if (ulcarrierIndex == "PCC" || ulcarrierIndex == "PCELL") {
                  pUSCHSumPcc += tbsize0xB139(i)
                } else if (ulcarrierIndex == "SCC1") {
                  pUSCHSumScc1 += tbsize0xB139(i)
                }
              }

              val firstSfn = currentSFNSF.head / 10
              val firstSfnSubFN = currentSFNSF.head % 10

              val lastSfn = currentSFNSF(numRecords0xB139 - 1) / 10
              val lastSfnSubFN = currentSFNSF(numRecords0xB139 - 1) % 10
              var durationMs = 0D

              durationMs = ParseUtils.CalculateMsFromSfnAndSubFrames(firstSfn, firstSfnSubFN, lastSfn, lastSfnSubFN)

              if (durationMs > 0) {
                val PUSCHSum = pUSCHSumPcc + pUSCHSumScc1
                pUSCHPccTPUT = (pUSCHSumPcc * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
                pUSCHScc1TPUT = (pUSCHSumScc1 * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
                overallPUSCHTPUT = (PUSCHSum * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
              }
            }
          
          }
     }    
    
    val ltePuschThroughput:Double=if (overallPUSCHTPUT > 0)  overallPUSCHTPUT else Constants.DEFAULT_KPI_DOUBLE_VAL
    
    val lteEarfcnUL: Int=if (ulFreq != 0) ulFreq else Constants.DEFAULT_KPI_INTEGER_VAL 
		val ltePccEarfcnDL: Int= if (dlFreq != 0) dlFreq else Constants.DEFAULT_KPI_INTEGER_VAL
		//val fileName: String= if (!row.isNullAt(row.fieldIndex("filename"))) row.getString(row.fieldIndex("filename")) else null
		val version_B193: Int = if (!row.isNullAt(row.fieldIndex("version_B193"))) row.getInt(row.fieldIndex("version_B193")) else 0
          	val SubPacketSize: Int = if (!row.isNullAt(row.fieldIndex("SubPacketSize_B193"))) {
            		if (row.getList(row.fieldIndex("SubPacketSize_B193")).size() > 0) row.getList(row.fieldIndex("SubPacketSize_B193")).get(0).toString.toInt else 0
          	} else 0
   
		val RSRP_b193: String = if (!row.isNullAt(row.fieldIndex("InstMeasuredRSRP"))) row.getString(row.fieldIndex("InstMeasuredRSRP")) else null
		val PhysicalCellID_b193: String = if (!row.isNullAt(row.fieldIndex("PhysicalCellID_B193"))) row.getString(row.fieldIndex("PhysicalCellID_B193")) else null
		val servingCellIndex_B193: String = if (!row.isNullAt(row.fieldIndex("ServingCellIndex_B193"))) row.getString(row.fieldIndex("ServingCellIndex_B193")) else null
		
		val FTLSNRRx0_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_0"))) row.getString(row.fieldIndex("FTLSNRRx_0")) else null
    val FTLSNRRx1_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_1"))) row.getString(row.fieldIndex("FTLSNRRx_1")) else null
    
    val RSRQ_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQ"))) row.getString(row.fieldIndex("InstRSRQ")) else null
    val RSSI_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSI"))) row.getString(row.fieldIndex("InstRSSI")) else null
    val earfcn_b193: String = if (!row.isNullAt(row.fieldIndex("EARFCN"))) row.getString(row.fieldIndex("EARFCN")) else null
		if (servingCellIndex_B193 != null && (SubPacketSize > 0 || (version_B193 <= 18 && version_B193 > 0))) {
		  
		  val earfcn=if (servingCellIndex_B193 != null && earfcn_b193 != null) {
           getServingCellPCI(servingCellIndex_B193, earfcn_b193)
		  }else{
		    null
		  }
       val  bandIndicator: Int = if (earfcn != null) {
          ParseUtils.getBandIndicator(earfcn(0)._2)
       }else {
         -1
       }
       
		  
      val PCI: Array[(String,Int)] =if (PhysicalCellID_b193 != null)  getServingCellPCI(servingCellIndex_B193, PhysicalCellID_b193) else null
			val RSRP: Array[(String,String)] = if (RSRP_b193 != null)  getServingCellCaKPIs(servingCellIndex_B193, RSRP_b193) else null
			
			val FTLSNRRx_0: Array[(String, String)] = if (FTLSNRRx0_b193 != null)  getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx0_b193) else null
      val FTLSNRRx_1: Array[(String, String)] = if (FTLSNRRx1_b193 != null)  getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx1_b193) else null
			val RSRQ: Array[(String, String)] =if (RSRQ_b193 != null)  getServingCellCaKPIs(servingCellIndex_B193, RSRQ_b193) else null
			val RSSI: Array[(String, String)] =if (RSSI_b193 != null)  getServingCellCaKPIs(servingCellIndex_B193, RSSI_b193) else null
			
			if(PCI != null) {
             			 for (i <- 0 to (PCI.size - 1)) {
                			//logger.info("0xB193: (Map) PCI(" + i + ").1 = " +PCI(i)._1 + "PCI(" + i + ").2 = " +PCI(i)._2)
                			var Index: String = ""
                			var validRxIndex: String = ""
                			try {
                 				 if (PCI(i)._1 == "PCell") {
                    					Index = "Pcc"
                 				 } else if (PCI(i)._1 == "SCC1") {
                   					 Index = "Scc1"
                    					validRxIndex = "Scc1"
                  				 } else if (PCI(i)._1 == "SCC2") {
                    					Index == "Scc2"
                    					validRxIndex = "Scc2"
                  				} else if (PCI(i)._1 == "SCC3") {
                    					Index = "Scc3"
                   					validRxIndex = "Scc3"
                  				} else if (PCI(i)._1 == "SCC4") {
                    					Index = "Scc4"
                    					validRxIndex = "Scc4"
                  				} else if (PCI(i)._1 == "SCC5") {
                    					Index == "Scc5"
                    					validRxIndex = "Scc5"
                  				}
             val FTLSNR: String = if (FTLSNRRx_0 != null && FTLSNRRx_1 != null && FTLSNRRx_0.size > i && FTLSNRRx_1.size > i) {
                                    if (FTLSNRRx_0(i)._2 > FTLSNRRx_1(i)._2) FTLSNRRx_0(i)._2 else FTLSNRRx_1(i)._2
                                  }else null
                                  
						 if (!PCI(i)._1.isEmpty && Index.nonEmpty) {
							ltePccRsrp = if (RSRP != null && RSRP.size > i && RSRP(i)._2 != null && Index == "Pcc") Try(RSRP(i)._2.toDouble).getOrElse(Constants.DEFAULT_KPI_DOUBLE_VAL) else Constants.DEFAULT_KPI_DOUBLE_VAL
							ltePccSinr=if (FTLSNR != null && Index == "Pcc")  Try(FTLSNR.toDouble).getOrElse(Constants.DEFAULT_KPI_DOUBLE_VAL) else  Constants.DEFAULT_KPI_DOUBLE_VAL
							ltePccRsrq=if (RSRQ != null && RSRQ.size > i && RSRQ(i)._2 != null && Index == "Pcc")  Try(RSRQ(i)._2.toDouble).getOrElse(Constants.DEFAULT_KPI_DOUBLE_VAL) else Constants.DEFAULT_KPI_DOUBLE_VAL
							ltePccRssi=if (RSSI != null && RSSI.size > i && RSSI(i)._2 != null && Index == "Pcc") Try(RSSI(i)._2.toDouble).getOrElse(Constants.DEFAULT_KPI_DOUBLE_VAL) else  Constants.DEFAULT_KPI_DOUBLE_VAL
							ltePccBandInd=if (bandIndicator != -1 && Index == "Pcc")  bandIndicator else Constants.DEFAULT_KPI_INTEGER_VAL
             }	
					}
          catch{
						case ge: Exception =>
                    				//logger.error("ES Exception occured while executing getCaModeKPIs for Index : " + Index + " Message : " + ge.getMessage)
                    				println("ES Exception occured while executing getCaModeKPIs : " + ge.getMessage)
                    				//logger.error("ES Exception occured while executing getCaModeKPIs Cause: " + ge.getCause())
                    				//logger.error("ES Exception occured while executing getCaModeKPIs StackTrace: " + ge.printStackTrace())
					}
				}
			}
		}
		//
	
	 //not done ltePdschTput
	 ReportKpis(fileName=fileName, ltePccRsrp=ltePccRsrp, lTEPccSinr=ltePccSinr, ltePccRsrq=ltePccRsrq, ltePccRssi=ltePccRssi,
	           lteBand=ltePccBandInd, 
                      ltePccEarfcnDL=ltePccEarfcnDL, ltePccEarfcnUL=lteEarfcnUL,
                      ltePccPuschTxPwr=ltePucchActTxPwr, ltePucchActTxPwr=ltePucchActTxPwr, ltePuschTxPwr=ltePuschTxPwr, lteMacTputDl=lteMacTputDl,
                      lteMacTputUl=lteMacTputDl,
                      ltePdschTput=0.0D, loc_2= "Not Found", lteCellID=lteCellId, LTERlcTputDl=lteRlcTputDl, LTERlcTputUl=lteRlcTputUl,
                      NCell10RSRP=NCell10RSRP,
                      NCell10RSRQ=NCell10RSRQ, NCell10RSSI=NCell10RSSI, LTERrcState=lteRrcState,
                      lteRrcConRej=LTERrcConRej, lteRrcConRestRej=LTERrcConRestRej, lteRrcConRelCnt=lteRrcConRelCnt, lteActDefContAcptCnt=lteActDefContAcptCnt, 
                      lteActDedContAcptCnt=lteActDedContAcptCnt, lteBearerModAcptCnt=lteBearerModAcptCnt, lteHoFailure=lteHoFailure, lteIntraHoFail=lteIntraHoFail,
                      lteAttRej=lteAttRej, lteActDefContRejCnt=lteActDefContRejCnt, lteActDedContRejCnt=lteActDedContRejCnt, lteBearerModRejCnt=lteBearerModRejCnt, 
                      lteReselFromGsmUmtsFail=lteReselFromGsmUmtsFail, lteRrcConSetupCnt=lteRrcConSetupCnt, lteRrcConReqCnt=lteRrcConReqCnt, 
                      ltePdnConntReqCnt=ltePdnConntReqCnt, lteAttReqCnt=lteAttReqCnt,
                      lteRrcConSetupCmpCnt=lteRrcConSetupCmpCnt, lteAttAcptCnt=lteAttAcptCnt, lteRrcConeconfCmpCnt=0,
                      lteRrcConReconfigCnt=lteRrcConReconfigCnt, lteDetachReqCnt=lteDetachReqCnt, lteRrcConRestReqCnt=lteRrcConRestReqCnt, 
                      lteTaAcptCnt=lteTaAcptCnt,
                      lteTaReqCnt=lteTaReqCnt, lteTaRej=LTETaRej, lteAttCmpCnt=lteAttCmpCnt,ltePccTputDL = -999999.0D, ltePccCqiCw0=lteAttCmpCnt,
                      ltePccDlMcs=ltePccDlMcs,ltePdschPrb = -9999999.0,ltePuschThroughput=ltePuschThroughput, lteModUl=lteModUl,ltePuschPrb=ltePuschPrb)
	})
	
})

  val reportDF=reportRDD.toDF()
  reportDF
 
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val conf = spark.sparkContext.getConf
    conf.setAppName("DMAT-PREPOST-DATA-PARSER")
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "10000000")
    val reportDf=parseLogData(spark,10L,"'datawenatchecity_5099801077_12_14_2018_18_19_21_PART6.seq'","pre",1234L,"")
    reportDf.show
    
 }

  def getServingCellCaKPIs(servingCellIndex_B193: String, LteKPIParams: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var LTEKpi: Array[(String, String)] = null
    if (!servingCells.isEmpty && !LteKPIParams.isEmpty) {
      try {
        val LteKPI: List[String] = LteKPIParams.split(",").map(_.trim).toList
        LTEKpi = ((servingCells zip LteKPI).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + LteKPIParams + ". Details : " + ge.getMessage)
          //logger.error("ES Exception occured while executing getServingCellPCI for Index : " + LteKPIParams + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
        //logger.error("ES Exception occured while executing getServingCellPCI Cause: " + ge.getCause())
        //logger.error("ES Exception occured while executing getServingCellPCI StackTrace: " + ge.printStackTrace())
      }
    }
    LTEKpi
  }

  def getServingCellPCI(servingCellIndex_B193: String, PhysicalCellID_b193: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var PCI: Array[(String, Int)] = null
    if (!servingCells.isEmpty && !PhysicalCellID_b193.isEmpty) {
      try {
        val PhysicalCellID: List[Int] = PhysicalCellID_b193.split(",").map(_.trim).toList.map(_.toInt)
        PCI = ((servingCells zip PhysicalCellID).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + PhysicalCellID_b193 + ". Details : " + ge.getMessage)
          //logger.error("ES Exception occured while executing getServingCellPCI for Index : " + PhysicalCellID_b193 + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
        //logger.error("ES Exception occured while executing getServingCellPCI Cause: " + ge.getCause())
        //logger.error("ES Exception occured while executing getServingCellPCI StackTrace: " + ge.printStackTrace())
      }
    }
    PCI
  }
  
  def getModulationType(input: String): Integer = {
    var modType: Integer = null
    try {
      if (input != null && !input.trim.isEmpty) {
        val modTypeDB: String = input
        if (!modTypeDB.isEmpty) {
          modTypeDB match {
            case "QPSK" => modType = 2
            case "16QAM" => modType = 4
            case "64QAM" => modType = 6
            case "256QAM" => modType = 8
          }
        }
      }
    }
    catch {
      case ge: Exception =>
        logger.info("Unable to flatten Modulation Type : " + ge.getMessage)
    }
    modType
  }
  
   def getThrouput(Thruput: Double, timeSpan: Double): Double = {
        var throughput: Double = 0
        if (timeSpan > 0) {
          throughput = (Thruput * 8) * (1000 / timeSpan)
          throughput /= Constants.Mbps
        }
        throughput
   }
}




                      

