package com.verizon.reports

object Qcomm2ReportQuery {
  val query: String ="""last_value(MacDlNumSamples, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlNumSamples,
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
      last_value(rlcDlTotalBytes, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTotalBytes,  
      last_value(rlcDlTimeStamp, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTimeStamp,
      last_value(rlcUlTotalBytes, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcUlTotalBytes,
      last_value(rlcUlTimeStamp, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcUlTimeStamp
      """
      def getQuery: String=query
}