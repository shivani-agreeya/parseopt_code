package com.verizon.oneparser.odlv

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants
import com.verizon.oneparser.dto.{Logs, LogsData}
import net.liftweb.json.parse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

object LogViewerHandler extends Serializable with LazyLogging {
  val inputSdf:SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
  val outputSdf:SimpleDateFormat = new SimpleDateFormat("YYYY-MM-dd")
  val finalResult = "SUCCESS"
  def initiateODLV(lvUrl:String, log: Logs): String ={
    try {
      if(log.odlvUpload != null && log.odlvUpload){
        val finalLVUrl = lvUrl+"?fileName="+Constants.getDlfFromSeq(log.fileName)+"&testId="+log.id+"&userId="+log.emailid/*odlvUserEmail*/+"&odlvTrigger="+log.odlvUpload+"&uploadedDate="+outputSdf.format(new Date(log.creationTime))
        logger.info("LVUrl URL formed...."+finalLVUrl)
        val get = new HttpGet(finalLVUrl)
        val client = new DefaultHttpClient()
        val response = EntityUtils.toString(client.execute(get).getEntity)
        logger.info("Response from Log Viewer -> "+response)
        val result =  (parse(response) \\ "result").values.getOrElse("result", "").toString
        logger.info("Result Value From LogViewer Response -> "+result)
      }
    } catch {
      case ge:Exception =>
        logger.error("Exception Occurred while Connecting LogViewer : "+ge.getMessage + " AND stack trace : "+ge.printStackTrace().toString);
    }
    finalResult
  }

}
