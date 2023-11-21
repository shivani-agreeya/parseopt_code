package com.verizon.oneparser.eslogrecord

object NasQuery {

  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills = "dateWithoutMillis"
  val dateWithMills = "dmTimeStamp"

  val query: String =
    "last_value(NAS_MsgType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NAS_MsgType, " +
      "last_value(pdnApnName, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdnApnName, " +
      "last_value(pdnType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdnType, " +
      "last_value(pdnReqType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdnReqType, " +
      "last_value(qci, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as qci, " +
      "last_value(esmCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as esmCause, " +
      "last_value(attRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as attRej, " +
      "last_value(authRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as authRej, " +
      "last_value(serviceRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as serviceRej, " +
      "last_value(taRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as taRej, " +
      "last_value(tatype, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as tatype, " +
      "last_value(attRejCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as attRejCause, " +
      "last_value(servRejCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as servRejCause, " +
      "last_value(emmTaRejCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as emmTaRejCause, " +
      "last_value(emmAuthRejCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as emmAuthRejCause, " +
      "last_value(ltePdnRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ltePdnRej, " +
      "last_value(lteAuthRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteAuthRej, " +
      "last_value(lteAttRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteAttRej, " +
      "last_value(lteServiceRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteServiceRej, " +
      "last_value(lteTaRej, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteTaRej, " +
      "last_value(imsi_NAS, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imsi_NAS, " +
      // "last_value(imei, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imei, " +
      "last_value(epsMobileIdType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as epsMobileIdType, " +
      "last_value(attachType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as attachType, " +
      "last_value(voPSStatus, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as voPSStatus, " +
      "last_value(dcnr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dcnr, " +
      "last_value(n1Mode, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as n1Mode, " +
      "sum(lteActDefContAcpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDefContAcptCnt, " +
      "sum(lteDeActContAcpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteDeActContAcptCnt, " +
      "sum(lteEsmInfoRes) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEsmInfoResCnt, " +
      "sum(ltePdnConntReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as ltePdnConntReqCnt, " +
      "sum(ltePdnDisconReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as ltePdnDisconReqCnt, " +
      "sum(lteActDefBerContReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDefBerContReqCnt, " +
      "sum(lteDeActBerContReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteDeActBerContReqCnt, " +
      "sum(lteEsmInfoReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEsmInfoReqCnt, " +
      "sum(lteAttCmp) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteAttCmpCnt, " +
      "sum(lteAttReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteAttReqCnt, " +
      "sum(lteDetachReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteDetachReqCnt, " +
      "sum(lteSrvReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteSrvReqCnt, " +
      "sum(lteAttAcpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteAttAcptCnt, " +
      "sum(lteEmmInfo) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEmmInfoCnt, " +
      "sum(lteTaReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteTaReqCnt, " +
      "sum(lteTaAcpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteTaAcptCnt, " +
      "sum(lteEsmModifyEpsReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEsmModifyEpsReqCnt, " +
      "sum(lteEsmModifyEpsRej) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEsmModifyEpsRejCnt, " +
      "sum(lteEsmModifyEpsAccpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteEsmModifyEpsAccptCnt, " +
      "sum(lteActDefContRej) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDefContRejCnt, " +
      "sum(lteActDedContRej) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDedContRejCnt, " +
      "sum(lteActDedBerContReq) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDedBerContReqCnt, " +
      "sum(lteActDedContAcpt) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteActDedContAcptCnt, " +
      "row_number() over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc) as rn "

  def getNasQuery(reportFlag: Boolean): String = {
    if (!reportFlag) {
      query.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      query.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }
}
