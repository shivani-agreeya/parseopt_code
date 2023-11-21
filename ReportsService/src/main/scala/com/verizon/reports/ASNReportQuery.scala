package com.verizon.reports

object ASNReportQuery {
  val query: String = """sum(lteRrcConRestReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConRestReqCnt,
              last_value(lteRrcConRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRej, 
              last_value(lteRrcConRestRej, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRestRej, 
              sum(lteRrcConRel) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConRelCnt,
              sum(lteRrcConSetup) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConSetupCnt,
              sum(lteRrcConReq) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConReqCnt, 
              sum(lteRrcConSetupCmp) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConSetupCmpCnt,
              sum(lteRrcConReconfig) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteRrcConReconfigCnt"""
  def getQuery: String=query
  
  val asnReportColumns: String =
    "lteRrcConRel, lteRrcConSetupCmp, lteRrcConRel, pdschSum_0xB887"

  def getAsnReportColumns: String = asnReportColumns

}