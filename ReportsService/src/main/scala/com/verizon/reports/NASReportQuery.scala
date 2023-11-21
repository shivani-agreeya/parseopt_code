package com.verizon.reports

object NASReportQuery {
  val query: String="""sum(lteActDefContAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteActDefContAcptCnt,     
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
    sum(lteTaAcpt) over(partition by filename, dmTimeStamp rows between unbounded preceding and unbounded following) as lteTaAcptCnt
    """
  def getQuery: String = query
}