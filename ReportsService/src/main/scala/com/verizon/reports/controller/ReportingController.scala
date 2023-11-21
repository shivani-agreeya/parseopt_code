package com.verizon.reports.controller

import javax.inject.Inject

import com.typesafe.scalalogging.LazyLogging
import com.verizon.reports.dto.{GetInputReportDto, ResponseDto}
import com.verizon.reports.services.ReportSaveToDB
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.web.bind.annotation._

@RestController
class ReportingController extends LazyLogging {

  @Inject
  val reportSaveToDb  : ReportSaveToDB=null

 /* private var service: ScheduledExecutorService =  Executors.newScheduledThreadPool(1)

  service.submit(new Runnable() {
    override def run(): Unit = {
      while(true){
        val metrics: Collection[GlobalMetric] =
          PhoenixRuntime.getGlobalPhoenixClientMetrics
        for (m <- metrics) {
          logger.info("Phoenix Gloabal Metrics :" + m.getCurrentMetricState)
        }
        Thread.sleep(30000)
      }
    }
  }
  )*/
  @PostMapping(path = Array("/getJobId"))
  def SubmitReportingFoPrePost(@RequestBody getInputDto: GetInputReportDto): ResponseEntity[ResponseDto] = {
    var response = ResponseDto(null, "", "")
    logger.info("======>Inserting into logfile_report-------->" + getInputDto)

    try {
      response = ResponseDto(reportSaveToDb.SaveToDB(getInputDto).toString, "", "")
    }
    catch {
      case e: Exception =>
        logger.info("======>SOME ERROR IN CONTROLLER-------->" )
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
    }
    new ResponseEntity[ResponseDto](response, HttpStatus.OK)

  }

}
