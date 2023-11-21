package com.verizon.oneparser.common

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.google.gson.Gson
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants.{GeometryDto, LocalTransportAddress, PciRequestInfo, StateRegionCounty, gNbidRequestInfo, gNodeIdInfo}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.schema.LogRecordQComm2
import net.liftweb.json.parse
import doobie.{Fragment, Update}
import doobie.free.ConnectionIO
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.util.{ExecutionContexts, transactor}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.HttpStatus
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.action.search.SearchRequest
import org.json4s.DefaultFormats

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.Map


object CommonDaoImpl extends LazyLogging{
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)
  var _db: transactor.Transactor.Aux[IO, Unit] = _
  var _sde: transactor.Transactor.Aux[IO, Unit] = _

  def db(commonConfigParams: CommonConfigParameters): Aux[IO, Unit] = {
    if (_db != null) return _db
    _db = Transactor.fromDriverManager[IO](
      commonConfigParams.POSTGRES_DRIVER,
      commonConfigParams.POSTGRES_DB_URL,
      commonConfigParams.POSTGRES_DB_USER,
      commonConfigParams.POSTGRES_DB_PWD
    )
    _db
  }
  def sde(commonConfigParams: CommonConfigParameters): Aux[IO, Unit] = {
    if (_sde != null) return _sde
    _sde = Transactor.fromDriverManager[IO](
      commonConfigParams.POSTGRES_DRIVER,
      commonConfigParams.POSTGRES_CONN_SDE_URL,
      commonConfigParams.POSTGRES_DB_USER,
      commonConfigParams.POSTGRES_DB_PWD
    )
    _sde
  }

  private def wrapOptional[T](value: T): Option[T] = {
    Option(value)
  }

  def updateHiveFailureStatusInLogs(logsTblDs: Dataset[Row], commonConfigParams: CommonConfigParameters): Unit = {
    if (logsTblDs != null) {
      try {
        logsTblDs.foreachPartition(
          iter => {
            iter.foreach(
              row => {
                val cause = Constants.CONST_SPACE + row.getString(row.fieldIndex("exceptionCause"))
                val filename = Constants.getDlfFromSeq(row.getString(row.fieldIndex("fileName")))
                val id = row.getInt(row.fieldIndex("id"))
                sql"update logs set failure_info = concat(failure_info, $cause) where file_name= $filename and id=$id"
                  .update.run.transact(db(commonConfigParams)).unsafeRunSync()
              }
            )
          }
        )
      } catch {
        case e: Throwable =>
          logger.error("Exception Occured while updating 'hive_status' for status into logs table:" + e.getMessage)
        //println(e.printStackTrace())
      }
    } else {
      logger.info("No Data Found while updating 'hive_status' for status into logs table:")
    }
  }

  def updateUserDeviceDetailsInLogs(logsTblDs: Dataset[Row], status: String, commonConfigParams: CommonConfigParameters): Unit = {
    if (logsTblDs != null) {
      try {
        import org.apache.spark.sql.functions
        val logTblDS_updated = logsTblDs.withColumn("state_new", functions.lit(status)).withColumn("qcomm2_status_new", functions.lit(status))
        logTblDS_updated.foreachPartition(
          iter => {
            iter.foreach(
              row => {
                val mdn = wrapOptional(row.getString(row.fieldIndex("mdn")))
                val imeiQComm2 = wrapOptional(row.getString(row.fieldIndex("imei_QComm2")))
                val firstName = wrapOptional(row.getString(row.fieldIndex("firstName")))
                val lastName = wrapOptional(row.getString(row.fieldIndex("lastName")))
                val email = wrapOptional(row.getString(row.fieldIndex("email")))
                val modelName = wrapOptional(row.getString(row.fieldIndex("modelName")))
                val dmUser = if (row.isNullAt(row.fieldIndex("dmUser")) || row.getInt(row.fieldIndex("dmUser")) == 0) commonConfigParams.DM_USER_DUMMY else row.getInt(row.fieldIndex("dmUser"))
                val isInBld = if (!row.isNullAt(row.fieldIndex("isInBuilding"))) row.getString(row.fieldIndex("isInBuilding")).toLowerCase else "false"
                val fileName = wrapOptional(Constants.getDlfFromSeq(row.getString(row.fieldIndex("fileName"))))
                val inBld = if (isInBld == "true") 1 else 0
                val id = row.getInt(row.fieldIndex("id"))
                val query = sql"""
                      update logs
                      set mdn=$mdn, imei=$imeiQComm2, firstname=$firstName, lastname=$lastName, emailid=$email, model_name=$modelName, dm_user=$dmUser, isinbuilding=$inBld
                      where file_name=$fileName and id=$id
                    """
                logger.warn(s"Executing query $query")
                query.update.run.transact(db(commonConfigParams)).unsafeRunSync()
              }
            )
          }
        )
      } catch {
        case e: Throwable =>
          logger.error("Exception Occured while updating user and device info for status:'" + status + "' into logs table:" + e.getMessage)
        //println(e.printStackTrace())
      }
    } else {
      logger.info("No Data Found while updating user and device info for status:'" + status + "' into logs table")
    }
  }

  def getUserInfo(email:String, commonConfigParams:CommonConfigParameters):UserInfo = {
    var userInfo:UserInfo = UserInfo()
    try {
      val userRows = sql"""
                     select user_id, first_name, last_name, email from users where email = lower($email) order by user_id desc limit 1
                     """
        .query[(Int, String, String, String)].to[List].transact(db(commonConfigParams)).unsafeRunSync()
      for (user <- userRows) {
        userInfo = userInfo.copy(user_id = user._1, firstName = user._2, lastName = user._3, email = user._4)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    userInfo
  }

  def getUserInfoByLogUpload(fileName: String, id: Int, commonConfigParams: CommonConfigParameters): UserInfo = {
    logger.info("getUserInfoByLogUpload(" + fileName + ", " + id + ")")
    var userInfo: UserInfo = UserInfo()
    try {
      val filename = Constants.getDlfFromSeq(fileName)
      val userRows = sql"""
                     select user_id,first_name,last_name,email from users
                     where user_id = (select dm_user from logs where file_name=$filename and id=$id)
                     order by user_id desc limit 1
                     """
        .query[(Int, String, String, String)].to[List].transact(db(commonConfigParams)).unsafeRunSync()
      for (user <- userRows) {
        userInfo = userInfo.copy(user_id = user._1, firstName = user._2, lastName = user._3, email = user._4)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    logger.info("userInfo: "+ userInfo)
    userInfo
  }

  def getInbuildingHeight(spark: SparkSession, ids: String, commonConfigParams: CommonConfigParameters): Dataset[Row] = {
    val inbldTblMap = mutable.Map.empty[Int, String]
    var inBldDs: Dataset[Row] = null
    try {
      val input = if (ids.trim.isEmpty) "0" else ids
      logger.info("Query for Inbuilding Image>>>>>>>>>")
      val inbldRows = sql"select id, image_height from inbuilding where id in ($input) and image_height is not null"
        .query[(Int, String)].to[List].transact(db(commonConfigParams)).unsafeRunSync()
      for (row <- inbldRows) {
        val imageHeight = row._2
        val inbldId = row._1
        inbldTblMap(inbldId) = imageHeight
      }
      import spark.implicits._
      inBldDs = inbldTblMap.toSeq.toDF("inbldImgId", "inbldImgHeight")
    } catch {
      case e: Exception => e.printStackTrace()
    }
    inBldDs
  }

  def insertEventsInformation(logsTblDs: Dataset[Row], commonConfigParams: CommonConfigParameters): Unit = {
    try {
      logsTblDs.foreachPartition(
        iter => {
          try {
            val xa = db(commonConfigParams)
            iter.foreach(
              row => {
                val filename = Constants.getDlfFromSeq(row.getString(row.fieldIndex("fileName")))
                val id = row.getInt(row.fieldIndex("id"))

                val lteOos = if (!row.isNullAt(row.fieldIndex("lteOos"))) row.getInt(row.fieldIndex("lteOos")) else 0
                val lteRlf = if (!row.isNullAt(row.fieldIndex("lteRlf"))) row.getInt(row.fieldIndex("lteRlf")) else 0
                val lteIntraReselFail = if (!row.isNullAt(row.fieldIndex("lteIntraReselFail"))) row.getInt(row.fieldIndex("lteIntraReselFail")) else 0
                val lteIntraHoFail = if (!row.isNullAt(row.fieldIndex("lteIntraHoFail"))) row.getInt(row.fieldIndex("lteIntraHoFail")) else 0
                val lteMobilityFromEutraFail = if (!row.isNullAt(row.fieldIndex("lteMobilityFromEutraFail"))) row.getInt(row.fieldIndex("lteMobilityFromEutraFail")) else 0
                val lteSibReadFailure = if (!row.isNullAt(row.fieldIndex("lteSibReadFailure"))) row.getInt(row.fieldIndex("lteSibReadFailure")) else 0
                val lteReselFromGsmUmtsFail = if (!row.isNullAt(row.fieldIndex("lteReselFromGsmUmtsFail"))) row.getInt(row.fieldIndex("lteReselFromGsmUmtsFail")) else 0
                val ltePdnRej = if (!row.isNullAt(row.fieldIndex("ltePdnRej"))) row.getInt(row.fieldIndex("ltePdnRej")) else 0
                val lteAttRej = if (!row.isNullAt(row.fieldIndex("lteAttRej"))) row.getInt(row.fieldIndex("lteAttRej")) else 0
                val lteAuthRej = if (!row.isNullAt(row.fieldIndex("lteAuthRej"))) row.getInt(row.fieldIndex("lteAuthRej")) else 0

                val count = lteOos + lteRlf + lteIntraReselFail + lteIntraHoFail + lteMobilityFromEutraFail + lteSibReadFailure + lteReselFromGsmUmtsFail + ltePdnRej + lteAttRej + lteAuthRej
                sql"update logs set events_count=(events_count + $count) where id=$id and file_name=$filename"
                  .update.run.transact(xa).unsafeRunSync()
              }
            )
          } catch {
            case e: Exception =>
              logger.error("Exception Occured @ iterator level while inserting into logs table:" + e.getMessage)
              e.printStackTrace()
          }
        }
      )
    } catch {
      case e: Throwable =>
        logger.error("Exception Occured while inserting events into logs table:" + e.getMessage)
    }
  }

  def getStateCountyRegionsForXY(mx: String, my: String, commonConfigParams: CommonConfigParameters): StateRegionCounty = {
    var stateRegionCounty: StateRegionCounty = StateRegionCounty()
    if (mx == "0.0" || my == "0.0") {
      return stateRegionCounty
    }
    try {
      val xa = sde(commonConfigParams)
//      val stCnty = sql"""
//                  SELECT s.stusps, c.COUNTYNS, c.name, s.VZ_Regions, (case when s.ID_Country=244 then 'USA' else s.VZ_REGIONS end) as country
//                  FROM counties_wm AS c JOIN states_wm s ON c.STATEFP = s.STATEFP
//                  WHERE ST_Intersects(c.shape, st_geometry(' point(' ||$mx||' '||$my||') ', 3857))
//                  """
//      val stCntRows = stCnty.query[(String,String,String,String)].to[List].transact(xa).unsafeRunSync()
//      for (row <- stCntRows) {
//        stateRegionCounty = stateRegionCounty.copy(stusps = row._1, countyns = row._2, name = row._3, country = row._4)
//      }
      val region = sql"""
                    SELECT vz_regions, name
                    FROM vzw_regions_wm
                    WHERE ST_Intersects(shape, st_geometry(' point(' ||$mx||' '||$my||') ', 3857)) and cur_reg = 1
                    """
      val regRows = region.query[(String, String)].to[List].transact(xa).unsafeRunSync()
      for (row <- regRows) {
        if (stateRegionCounty.vz_regions.isEmpty) {
          stateRegionCounty = stateRegionCounty.copy(vz_regions = row._1, name = row._2)
        } else {
          stateRegionCounty = stateRegionCounty.copy(vz_regions1 = row._1, name = row._2)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    stateRegionCounty
  }

  def getNrconnecityModeInfo(testId:String,commonConfigParams:CommonConfigParameters):Array[LVFileMetaInfo] = {

    val logFileMetaInfoList = mutable.ListBuffer.empty[LVFileMetaInfo]
    try{
      val xa = db(commonConfigParams)
      val sql = sql"""select nr_connectivity_mode from logs where id = ${testId.toInt}"""
      val resultRows = sql.query[String].to[List].transact(xa).unsafeRunSync()
      for (row <- resultRows) {
        logFileMetaInfoList += LVFileMetaInfo(nrConnecitivityMode = row)
      }
    }
    catch {
      case e:Exception =>
        logger.error("Exception Occurred while retrieving NR connectivity mode from  logs table: "+e.getMessage)
    }
    logFileMetaInfoList.toArray
  }

  def updateProcessParams(processParams: ProcessParams, commonConfigParams: CommonConfigParameters): Unit = {
    try {
      val chng = if (processParams.struct_chng == "") "Y" else processParams.struct_chng
      val chk = if (processParams.cnt_chk == "") "Y" else processParams.cnt_chk
      val id: Int = processParams.id
      val ps =
        sql"""update process_param_values
               set date_modified = now(), ${processParams.recordType}_struct_chng = $chng,
                    ${processParams.recordType}_cnt_chk = $chk
               where id = $id
               """
      ps.update.run.transact(db(commonConfigParams)).unsafeRunSync()
    } catch {
      case e: Throwable =>
        logger.error("Exception Occured while updating process_param_values table:" + e.getMessage)
    }
  }

  def deleteSeqFiles(logsTblDs: Dataset[Row], commonConfigParams: CommonConfigParameters): Unit = {
    val filesList = logsTblDs.select("fileName", "fileLocation", "updatedTime").rdd.map(row => {
      val updatedTime = row.getLong(row.fieldIndex("updatedTime"))
      val fileLocation = row.getString(row.fieldIndex("fileLocation"))
      val logsUpdatedTime =
        if (updatedTime == null)
          Constants.getDateFromFileLocation(fileLocation)
        else
          new SimpleDateFormat("MMddyyyy").format(new Date(updatedTime))
      Constants.getSeqFilePathFromDlf(row(0).toString, fileLocation, logsUpdatedTime, commonConfigParams.HDFS_FILE_DIR_PATH)
    }).collect().toList
    filesList.foreach(
      row => {
        val conf = new Configuration()
        conf.addResource(new Path(commonConfigParams.PATH_TO_CORE_SITE))
        conf.addResource(new Path(commonConfigParams.PATH_TO_HDFS_SITE))
        val fs: FileSystem = FileSystem.get(conf)
        println("File Path for Deletion on Hive Success....." + row)
        fs.delete(new Path(row), true)
      }
    )
  }

  def updateMissingVersionsInLogs(logsTblDs: Dataset[Row], commonConfigParams: CommonConfigParameters): Unit = {
    if (logsTblDs != null) {
      try {
        logsTblDs.foreachPartition(iter => {
          iter.foreach(row => {
            val str = row.getString(row.fieldIndex("hexLogCode")) + Constants.CONST_DELIMITER + row.getInt(row.fieldIndex("missingVersion")) + Constants.CONST_COMMA
            val id = row.getInt(row.fieldIndex("id"))
            sql"update logs set missing_versions = concat(missing_versions, $str) where id= $id"
              .update.run.transact(db(commonConfigParams)).unsafeRunSync()
          })
        })
      } catch {
        case e: Throwable =>
          logger.error("Exception Occurred while updating 'missing version info' into logs table:" + e.getMessage)
      }
    } else {
      logger.info("No Data Found while updating 'missing version info' into logs table")
    }
  }

  def insertEventsInfoWhileES(commonConfigParams:CommonConfigParameters, evtInfo: LteIneffEventInfo) : Unit = {
    try {
      val sxa = sde(commonConfigParams)
      val filename:String = evtInfo.fileName
      val id:Int = evtInfo.testId
      val evtTimestamp = evtInfo.evt_timestamp
      val trimmedPath:String = evtInfo.fileLocation.substring(1, evtInfo.fileLocation.length)

      val (evtVal, evtName): (Int, String) =
        if (evtInfo.ltecallinefatmpt != null) {
          (evtInfo.ltecallinefatmpt.intValue(), "ltecallinefatmpt")
        } else if (evtInfo.beamRecoveryFailEvt != null) {
          (evtInfo.beamRecoveryFailEvt.intValue(), "nrbeamrecoveryfailureevt")
        } else if (evtInfo.nrRachAbortEvt != null) {
          (evtInfo.nrRachAbortEvt.intValue(), "nrrachabortevt")
        } else if (evtInfo.lteRrcCnnSetupFailEvt != null) {
          (evtInfo.lteRrcCnnSetupFailEvt.intValue(), "lterrcconsetupfailure")
        } else if (evtInfo.nrSetupFailure != null) {
          (evtInfo.nrSetupFailure.intValue(), "nrsetupfailure")
        } else null

      if ((evtVal, evtName) != null)
        (fr0"insert into event_lvanalysis(objectid, creation_time, filename, testid, evt_timestamp," ++ Fragment.const(evtName) ++
          fr0", file_path) values (nextval('lvSequence'), now(), $filename, $id, $evtTimestamp, $evtVal, $trimmedPath)")
            .update.withUniqueGeneratedKeys[Long]("objectid").transact(sxa).unsafeRunSync()
    }
    catch {
      case e: Exception =>
        logger.error("insertEventsInfoWhileES -> Exception Occured @ iterator level while inserting into event_lvanalysis  table:"+e.getMessage)
        e.printStackTrace()
    }
  }

  def updateEventsCountWhileES(commonConfigParams:CommonConfigParameters, evtInfo: LteIneffEventInfo) : Unit = {
    try {
      val xa = db(commonConfigParams)
      val count:Int =
        if (evtInfo.ltecallinefatmpt != null) {
          evtInfo.ltecallinefatmpt
        } else if (evtInfo.beamRecoveryFailEvt != null) {
          evtInfo.beamRecoveryFailEvt
        } else if (evtInfo.lteRrcCnnSetupFailEvt != null) {
          evtInfo.lteRrcCnnSetupFailEvt
        } else if (evtInfo.nrSetupFailure != null) {
          evtInfo.nrSetupFailure
        } else if (evtInfo.nrRachAbortEvt != null) {
          evtInfo.nrRachAbortEvt
        } else if (evtInfo.evtsCount != null) {
          evtInfo.evtsCount
        } else 0
      val id:Int = evtInfo.testId
      val filename:String = evtInfo.fileName
      val vzRegionsOption = Option(evtInfo.vzRegions)
      val marketAreaOption = Option(evtInfo.marketArea)
      val nrConnectivityModeOption = Option(evtInfo.nrconnectivitymode)

      val updateCount = sql"update logs set events_count=events_count + $count, vz_regions=$vzRegionsOption, market_area=$marketAreaOption, nr_connectivity_mode=$nrConnectivityModeOption where id=$id and file_name=$filename"
        .update.run.transact(xa).unsafeRunSync()
      if (updateCount > 0) {
        logger.info(""+Constants.PG_OPLOGS_TBL_NAME+" table updated with 'Events count' LogFile Information while in ES for File : " + evtInfo.fileName);
      }
      else {
        logger.info(""+Constants.PG_OPLOGS_TBL_NAME+" table could not be with 'Events count' LogFile Information while in ES for File : " + evtInfo.fileName+", File is missing");
      }

    }
    catch {
      case e: Exception =>
        logger.error("updateEventsCountWhileES -> Exception Occured @ iterator level while updating events count into logs  table:"+e.getMessage)
        e.printStackTrace()
    }
  }

  def insertEventsCountInES(commonConfigParams: CommonConfigParameters, evtInfo: LteIneffEventInfo): Boolean = {
    var isEventCntUpdate = false
    try {
      val xa = db(commonConfigParams)
      val updateCount = sql"update logs set events_count=(events_count + ${evtInfo.evtsCount.toInt}) where id=${evtInfo.testId.toInt} and file_name=${evtInfo.fileName}"
        .update.run.transact(xa).unsafeRunSync()

      if(evtInfo.evtsCount != null) println("events count: " + evtInfo.evtsCount)
      if(evtInfo.fileName != null) println("file name for events count update: " + evtInfo.fileName)

      if (updateCount > 0) {
        isEventCntUpdate = true
        logger.info(""+Constants.PG_OPLOGS_TBL_NAME+" table updated with 'Events count' LogFile Information for File : " + evtInfo.fileName)
      }
      else {
        logger.info(""+Constants.PG_OPLOGS_TBL_NAME+" table could not be with 'Events count' LogFile Information for File : " + evtInfo.fileName+", File is missing")
      }
    }
    catch {
      case e: Exception =>
        logger.error("updateEventsCountInExecuteSqlQuery -> Exception Occured @ iterator level while updating events count into logs  table:" + e.getMessage)
        e.printStackTrace()
    }
    isEventCntUpdate
  }

  /** *
   * The method stores provided list of lvFileMetainfo as bulk insert.
   *
   * @param lvFileMetaInfos lvFileMetainfos to insert
   * @param commonConfigParams params to support db interaction
   */
  def insertLvFileMetainfo(lvFileMetaInfos: List[LVFileMetaInfo], commonConfigParams: CommonConfigParameters): Unit = {
    val lvFileMetaInfosToInsert = lvFileMetaInfos.map(lvFileMetainfo => {
      (lvFileMetainfo.fileName.split("\\.")(0), lvFileMetainfo.testId.toInt, lvFileMetainfo.logCodesList, lvFileMetainfo.triggerCount.toInt)
    })
    try {
      val xa = db(commonConfigParams)

      def insertLvFileMetainfo(lvFileMetainfos: List[(String, Int, List[String], Int)]): ConnectionIO[Int] = {
        val sql = "insert into lv_filemetainfo (file_name,id,logcodes,trigger_count) values (?, ?, ?, ?) on conflict(file_name) do update set trigger_count=excluded.trigger_count, logcodes = excluded.logcodes, id = excluded.id"
        Update[(String, Int, List[String], Int)](sql).updateMany(lvFileMetainfos)
      }

      insertLvFileMetainfo(lvFileMetaInfosToInsert).transact(xa).unsafeRunSync()

    }
    catch {
      case e: Exception =>
        logger.error("insertLvFileMetainfo -> Exception Occured while inserting data into lv_filemetainfo:", e)
    }
  }

  def gNodeBidBasedOnPci(pciReq: gNbidRequestInfo, localTransportAddress: LocalTransportAddress, commonConfigParameters: CommonConfigParameters) = {


    var gNBidInfoMap: scala.collection.immutable.Map[Integer, gNodeIdInfo] = scala.collection.immutable.Map()
    var dateRangeQuery = ""
    var termQuery = ""
    var distanceQuery = ""
    var cellsiteFilewildcardQuery = ""
    var airInterface: Int = 5

    //    if (EsClient.client == null) {
    //      try {
    //        EsClient.build(LocalTransportAddress(localTransportAddress.host, localTransportAddress.port, localTransportAddress.clusterName))
    //      }
    //      catch {
    //        case e: UnknownHostException =>
    //          logger.info("Unable to Connect to host : " + e.getMessage)
    //      }
    //
    //    }


    //    if (EsClient.highLevelClient == null) {
    //      try {
    //        EsClient.buildEsHighLevelRestClient(commonConfigParameters)
    //      }
    //      catch {
    //        case e: UnknownHostException =>
    //          logger.info("Unable to Connect to host for high level Rest Client: " + e.getMessage)
    //      }
    //
    //    }
    //    var distance:String = "1km"
    //    if(pciReq.cellType=="mmWave") {
    //      distance ="1km"
    //    } else if(pciReq.cellType=="SUB6")  {
    //      distance ="8km"
    //    }

    try {
      if (pciReq != null) {
        if (commonConfigParameters.ONP_GNBID_URL.nonEmpty) {
          val finalgnbidUrl = commonConfigParameters.ONP_GNBID_URL + "?pn=" +
            pciReq.pn + "&cellType=" + pciReq.cellType +
            "&lat=" + pciReq.Lat + "&lon=" + pciReq.Lon +
            "&cellSiteIndx=" + pciReq.cellSiteIndx +
            "&startDt=" + pciReq.startDt +
            "&endDt="+pciReq.endDt+"&NRArfcn="+pciReq.nrarfcn
          logger.info("onpgnbid URL formed...." + finalgnbidUrl)
          val get = new HttpGet(finalgnbidUrl)
          val client = new DefaultHttpClient()
          val response = EntityUtils.toString(client.execute(get).getEntity)
          logger.info("Response from onpgnbid url -> " + response)
          val result = (parse(response) \\ pciReq.pn.toString).values.getOrElse(pciReq.pn.toString, "").toString
          gNBidInfoMap += (pciReq.pn -> gNodeIdInfo(enodebSectorId = result))
        }
      }


      //      val sourceBuilder = new SearchSourceBuilder
      //      val bqBuilder = new BoolQueryBuilder
      //
      //      bqBuilder.must(QueryBuilders.existsQuery("azimuth"))
      //        .must(QueryBuilders.existsQuery("ant_bw_deg"))
      //        .must(QueryBuilders.existsQuery("bts"));
      //
      //
      //      sourceBuilder.fetchSource(Array[String]("pn", "sitename", "band","ant_bw_deg",
      //        "loc_mx","loc_my","enodeb_sector_id","bts","azimuth"), Array[String]())
      //
      //      if (pciReq.startDt != null && pciReq.endDt  != null) {
      //        bqBuilder.must(QueryBuilders.rangeQuery("endDate").gte(pciReq.startDt))
      //        bqBuilder.must(QueryBuilders.rangeQuery("startDate").lte(pciReq.endDt))
      //      }
      //
      //      if (pciReq.pn != null)
      //        bqBuilder.must(QueryBuilders.termQuery("air_interface",airInterface.toString))
      //
      //      bqBuilder.must(QueryBuilders.wildcardQuery("objectid","*national_cellver_0000-*"))
      //
      //      bqBuilder.filter(QueryBuilders.geoDistanceQuery("loc")
      //        .point(pciReq.Lat,pciReq.Lon).distance(distance,DistanceUnit.KILOMETERS))
      //
      //      sourceBuilder.query(bqBuilder)
      //      sourceBuilder.size(1)
      //      var searchRequestClient = new SearchRequest()
      //      searchRequestClient.indices(pciReq.cellSiteIndx)
      //      searchRequestClient.source(sourceBuilder)
      //      var searchResponseClient = EsClient.highLevelClient.search(searchRequestClient, RequestOptions.DEFAULT)
      //
      //      var enodeb_sector_id = ""
      //      val endSearchHits = searchResponseClient.getHits.getHits
      //      println("get the search count>>>>>" + endSearchHits.length)
      //      for (i <- 0 until endSearchHits.length) {
      //        if (endSearchHits(i).getSourceAsMap.get("enodeb_sector_id") != null) {
      //          enodeb_sector_id = endSearchHits(i).getSourceAsMap.get("enodeb_sector_id").toString
      //         println("get the enodeb_sector_id>>>>>" + enodeb_sector_id)
      //          gNBidInfoMap += (pciReq.pn -> gNodeIdInfo(enodebSectorId=enodeb_sector_id))
      //        }
      //      }

    }


      //    if (pciReq.startDt != null && pciReq.endDt  != null) {
      //         dateRangeQuery = "{ \"range\": { \"endDate\": { \"gte\": \"" + pciReq.startDt + "\" }}}," + "{ \"range\": { \"startDate\": { \"lte\": \"" + pciReq.endDt + "\" } } }"
      //    }

      //if (pciReq.pn != null) termQuery = ",{ \"terms\": { \"pn\": [" + pciReq.pn + "] },{ \"terms\": { \"air_interface\": [\" + 5 + \"] }} "

      //    if (pciReq.pn != null) termQuery = ",{ \"terms\": { \"air_interface\": [\"" + airInterface + "\"] }} "


      //    if (pciReq.Lat != null && pciReq.Lon  != null) distanceQuery = "{" + "\"geo_distance\": {" + "\"distance\":\"" + distance + "km\"," + "\"loc\": {" + "\"lat\": " + pciReq.Lat + "," + "\"lon\": " + pciReq.Lon + "}" + "}" + "}"


      //    cellsiteFilewildcardQuery = "{\"wildcard\": {\"objectid\": \"*national_cellver_0000-*\"}}"
      //
      //
      //
      //    val queryBuilder = "{ " + "\"size\" : 1, " + "\"_source\": {" + "\"includes\": [" + "\"pn\", " + "\"sitename\"," + "\"band\", " + "\"ant_bw_deg\"," + "\"loc_mx\"," + "\"loc_my\"," + "\"enodeb_sector_id\"," + "\"bts\", " + "\"azimuth\"]},	" +
      //      "\"query\": {     " + "\"bool\": {       " + "\"must\": [ {	" + "\"bool\": { " + "\"must_not\": { " + "\"match_phrase\": { " + "\"ant_bw_deg\": { " + "\"query\": 0 , " + "\"slop\": 0, \"boost\": 1 } } } } }, " +
      //      "{\"exists\": {\"field\": \"azimuth\" } }, " + "{\"exists\": {\"field\": \"ant_bw_deg\" } }, " + "{\"exists\": {\"field\": \"bts\" } }, " + dateRangeQuery + termQuery + "," + cellsiteFilewildcardQuery + "], \"filter\": [" + distanceQuery + "]" +
      //      " }" + "}" + ", \"sort\": [{\"enodeb_sector_id\": {\"order\": \"asc\"}}] " + "  } "
      //


      //    logger.info("get es query for gNBid: " + queryBuilder)
      //    val searchRequest = new SearchRequest
      //    searchRequest.indices(pciReq.cellSiteIndx)
      //    try {
      //      val searchResponse = new SearchTemplateRequestBuilder(EsClient.client).setRequest(searchRequest).setScript(queryBuilder).setScriptType(ScriptType.INLINE).get
      //      logger.info("Get search response for gNBid: " + searchResponse)
      //      val search_hits = searchResponse.getResponse.getHits.getHits
      //      if (search_hits != null) {
      //        import org.elasticsearch.search.aggregations.metrics.tophits.TopHits
      //        import scala.collection.JavaConversions._
      //        for (a <- search_hits) {
      //          val enodeb_sector_id:String = a.getSourceAsMap.get("enodeb_sector_id").toString
      //          gNBidInfoMap += (pciReq.pn -> gNodeIdInfo(enodebSectorId=enodeb_sector_id))
      //
      //        }
      //      }
      //    }
    catch {
      case e: Exception =>
        logger.error("Error Occurred while fetching data from elastic search for gNbid search request : " + e.getMessage)
        e.printStackTrace()
    }
    //    finally {
    //      if(EsClient.client!=null){
    //        //EsClient.client.close()
    //      }
    //    }
    //    finally {
    //      closeESHighLevelClient();
    //    }
    gNBidInfoMap
  }

  def getPCIBasedLatLon(pciReq:PciRequestInfo, localTransportAddress: LocalTransportAddress,commonConfigParameters: CommonConfigParameters)  = {
    var pciInfoMap: scala.collection.immutable.Map[Int,GeometryDto] = scala.collection.immutable.Map()
    //    if (EsClient.client == null) {
    //      try {
    //        EsClient.build(LocalTransportAddress(localTransportAddress.host, localTransportAddress.port, localTransportAddress.clusterName))
    //      }
    //      catch {
    //        case e: UnknownHostException =>
    //          logger.info("Unable to Connect to host : " + e.getMessage)
    //      }
    //
    //    }
    //    val query = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"pn\":[" + pciReq.pn + "]}},{\"terms\":{\"bts\":[" + pciReq.bts + "]}},{\"geo_bounding_box\":{\"loc\":{\"top_left\":" +
    //      "{\"lat\":" + (pciReq.tl_lat + 0.4) + ",\"lon\":" + (pciReq.tl_lon - 0.4) + "},\"bottom_right\":{\"lat\":" + (pciReq.br_lat - 0.4) + ",\"lon\":" + (pciReq.br_lon + 0.4) + "}}}}," +
    //      "{\"range\":{\"endDate\":{\"gte\":\""+pciReq.startDate+"\"}}},{\"range\":{\"startDate\":{\"lte\":\""+pciReq.endDate+"\"}}}," +
    //      "{\"wildcard\":{\"objectid\":\"*cellver_0000-*\"}}],\"must_not\":[{\"wildcard\":{\"objectid\":\"*national_cellver*\"}},{\"wildcard\":{\"objectid\":\"*0000_0001*\"}}]}},\"aggs\":{\"pn\":{\"terms\":{\"field\":\"pn\",\"size\":\"50000\"},\"aggs\":{\"top_loc\":{\"top_hits\":{\"_source\":{\"includes\":[\"pn\",\"lat\",\"lon\",\"enodeb_sector_id\",\"sector\",\"band\"]},\"size\":1}}}}}}"

    //
    //    logger.info("get es query: " + query)
    //    println("host:" + localTransportAddress.host + "port:" + localTransportAddress.port + "cluster name:" + localTransportAddress.clusterName)

    //    val RestClientBuilder  = RestClient.builder(new HttpHost("10.20.40.28", 9202, "http"));
    //    val restHighLevelclient = new RestHighLevelClient(RestClientBuilder);
    //    var searchRequestClient = new SearchRequest("cellsite-2021-6")
    //    var  matchQueryBuilder = QueryBuilders.matchQuery("enodeb_sector_id", "1090001_1000")
    //    import org.elasticsearch.search.builder.SearchSourceBuilder
    //    val sourceBuilder = new SearchSourceBuilder
    //    sourceBuilder.query(matchQueryBuilder)
    //    searchRequestClient.source(sourceBuilder)
    //    sourceBuilder.from(0)
    //    sourceBuilder.size(1)

    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.client.RequestOptions

    val searchRequest = new SearchRequest
    searchRequest.indices(pciReq.index)
    try {
      //
      //      val searchResponse = new SearchTemplateRequestBuilder(EsClient.client).setRequest(searchRequest).setScript(query).setScriptType(ScriptType.INLINE).get
      //     //val searchBuilder = new SearchTemplateRequestBuilder(EsClient.client).setRequest(searchRequest).setScript(query).setScriptType(ScriptType.INLINE).get
      //
      //
      //      logger.info("Get search response: " + searchResponse)
      //      val aggregations: Aggregations = searchResponse.getResponse.getAggregations
      //      if (aggregations != null) {
      //        import org.elasticsearch.search.aggregations.metrics.tophits.TopHits
      //        import scala.collection.JavaConversions._
      //
      //        for (aggregation <- searchResponse.getResponse.getAggregations.asList) {
      //          val aggName = aggregation.getName
      //          val terms: Terms = searchResponse.getResponse.getAggregations.get(aggName)
      //          if (terms.getBuckets.size > 0) {
      //            var i = 0
      //            while ( {i < terms.getBuckets.size}) {
      //              //val pn: Terms = terms.getBuckets.get(i).getAggregations.get("top_loc")
      //              val top_loc: TopHits = terms.getBuckets.get(i).getAggregations.get("top_loc")
      //              for (hit <- top_loc.getHits.getHits) {
      //                pciInfoMap += (hit.getSourceAsMap.get("pn").asInstanceOf[Integer] -> GeometryDto(lat = hit.getSourceAsMap.get("lat").asInstanceOf[Double], lon = hit.getSourceAsMap.get("lon").asInstanceOf[Double], enodeb_sector_id = hit.getSourceAsMap.get("enodeb_sector_id").asInstanceOf[String], sector = hit.getSourceAsMap.get("sector").asInstanceOf[Integer], band = hit.getSourceAsMap.get("band").asInstanceOf[Integer]))
      //              }
      //              /*if (pn.getBuckets.size > 0) {
      //                var j = 0
      //                while ( {
      //                  j < pn.getBuckets.size
      //                }) {
      //                  val top_loc: TopHits = pn.getBuckets.get(j).getAggregations.get("top_loc")
      //                  for (hit <- top_loc.getHits.getHits) {
      //                    pciInfoMap += (hit.getSourceAsMap.get("pn").asInstanceOf[Integer] -> GeometryDto(lat = hit.getSourceAsMap.get("lat").asInstanceOf[Double], lon = hit.getSourceAsMap.get("lon").asInstanceOf[Double]))
      //                  }
      //                  j += 1
      //                }
      //              }*/
      //              i += 1
      //            }
      //          }
      //        }
      //      }
      if(commonConfigParameters.ONP_PCIDISTANCE_URL.nonEmpty) {

        val post = new HttpPost(commonConfigParameters.ONP_PCIDISTANCE_URL)
        // send the post request
        val httpclient = HttpClientBuilder.create.build

        post.addHeader("Content-Type", "application/json")
        val pciDistReqAsJson = new Gson().toJson(pciReq)
        println("onpPciDistance request>>>>" + pciDistReqAsJson)
        post.setEntity(new StringEntity(pciDistReqAsJson))
        val response = httpclient.execute(post)
        val entity = response.getEntity
        val str = EntityUtils.toString(entity,"UTF-8")

        if(response.getStatusLine.getStatusCode != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
          implicit val formats = DefaultFormats
          val result =  (parse(str) \ "data").values.asInstanceOf[List[Map[String,String]]]

          for (i<- 0 to result.size-1) {
            pciInfoMap += (result(i)("pn").asInstanceOf[BigInt].intValue() -> GeometryDto(lat = result(i)("lat").asInstanceOf[Double], lon = result(i)("lon").asInstanceOf[Double], enodeb_sector_id = result(i)("enodebSectorId").asInstanceOf[String], sector = result(i)("sector").asInstanceOf[BigInt].intValue(), band = result(i)("band").asInstanceOf[BigInt].intValue()))
          }
        }

      }

    }
    catch {
      case e: Exception =>
        logger.error("Error Occurred while fetching data from elastic search : " + e.getMessage)
        e.printStackTrace()
    }
    //    finally {
    //      if(EsClient.client!=null){
    //        //EsClient.client.close()
    //      }
    //    }
    pciInfoMap
  }

  def closeESClient()= {
    if (EsClient.client != null) {
      EsClient.client.close()
    }
  }

}

case class UserInfo(user_id:Integer=0, firstName:String="", lastName:String="", email:String="")

case class ProcessParams(id:Integer=0, recordType:String="", struct_chng:String="", cnt_chk:String="")

case class LteIneffEventInfo(testId: Integer = 0, fileName: String = "", evt_timestamp: Timestamp = null, ltecallinefatmpt: Integer = null, beamRecoveryFailEvt: Integer = null, nrRachAbortEvt: Integer = null, lteRrcCnnSetupFailEvt: Integer = null, nrSetupFailure: Integer = null, fileLocation: String = "", evtsCount: Integer = null, vzRegions:String="",marketArea:String=null,nrconnectivitymode:String="")

case class LVFileMetaInfo(testId: Integer = 0, fileName: String = null, logCodesList: List[String] = null, triggerCount: Integer = 0, nrConnecitivityMode:String=null)

case class ncellKpiInfo(key:String=null,value:Float=0)

case class SipInfoDto(from:String="",subtitle:String="",direction:String="")