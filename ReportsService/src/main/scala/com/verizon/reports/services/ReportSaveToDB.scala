package com.verizon.reports.services

import java.sql._

import javax.inject.Inject
import com.typesafe.scalalogging.LazyLogging
import com.verizon.reports.common.CommonConfigParameters
import com.verizon.reports.dto.GetInputReportDto
import org.springframework.core.env.Environment
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component


@Component
class ReportSaveToDB extends LazyLogging {
  @Inject private val environment: Environment = null

  @Inject
  val processReportRecordsClass:ProcessReports = null

  @Inject
  val threadPool : ThreadPoolTaskExecutor = null


  @Inject
  private val commonConfigParams: CommonConfigParameters = null
  def SaveToDB(dto:GetInputReportDto): Long = {

    var jobId: Long= -1
    val sql = "insert into logfile_report (pre_filename,post_filename,submit_daterequested,dm_user,report_submit_status,pre_from_date,pre_to_date,post_from_date,post_to_date) values " + 
    "(?,?,to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')::timestamp,?,?,"+
    "to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')::timestamp,to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')::timestamp,"+
    "to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')::timestamp,to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS')::timestamp )"
    val connection:Connection = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)
    val ps: PreparedStatement = connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS)
    //var jobId: Int= -1
    try {
    /*  val sortedPrefiles= dto.prefileName.sorted
      val prefiles_concate:String= sortedPrefiles.mkString(",")

      val sortedPostfiles= dto.postfileName.sorted
      val postfiles_concate:String= sortedPostfiles.mkString(",")*/

      ps.setString(1,  dto.prefileName )
      ps.setString(2,  dto.postfileName )
      ps.setString(3, dto.dmTimeStamp )
      ps.setLong(4, dto.userId)
      ps.setString(5,"job_submitted")
      ps.setString(6, dto.preTimeFrom )
      ps.setString(7, dto.preTimeTo )
      ps.setString(8, dto.postTimeFrom )
      ps.setString(9, dto.postTimeTo )
      ps.executeUpdate()
      val rs: ResultSet = ps.getGeneratedKeys();
      if (rs.next()) {
        jobId = rs.getLong(1);
      }
    } catch{
      case e:Throwable =>
        logger.info("======>SOME ERROR IN SAVETODB-------->" )
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
        logger.info("======>error to report-------->" + ps )
    } finally {
      ps.close
      connection.close
    }
    /*processReportRecords.processReportData(dto )
    logger.info("======>Inserting into logfile_report-------->" + jobId)*/
    //this.getProcessReportRecord(dto, jobId)
   processReportRecordsClass.getProcessReportRecord(dto,jobId)
    logger.info("======>Inserting into logfile_report-------->" + jobId)
    jobId
  }

  /*@Async("asyncExecutor")
  def getProcessReportRecord(dto:GetInputReportDto, jobId :Long): Unit =
  {

      processReportRecords.processReportData(dto)
      logger.info("======>call end to process data report-------->" + jobId)

  }*/

}
