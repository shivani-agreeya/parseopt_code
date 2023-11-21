package com.verizon.reports.services

import com.verizon.reports.dto.GetInputReportDto
import com.verizon.reports.ProcessReportRecords
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import javax.inject.Inject
import com.typesafe.scalalogging.LazyLogging
import com.verizon.reports.common.CommonConfigParameters

@Component
class ProcessReports extends LazyLogging {
  @Inject private val environment: Environment = null

  @Inject
 val processReportRecords:ProcessReportRecords = null

  @Inject
  val threadPool : ThreadPoolTaskExecutor = null

  @Inject
  private val commonConfigParams: CommonConfigParameters = null

  @Async("asyncExecutor")
   def getProcessReportRecord(dto:GetInputReportDto, jobId :Long): Unit =
  {

    processReportRecords.processReportData(dto, jobId)
      logger.info("======>call end to process data report-------->" + jobId)

  }

}
