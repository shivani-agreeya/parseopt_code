package com.verizon.oneparser.eslogrecord

object IpQuery {
  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills = "dateWithoutMillis"
  val dateWithMills = "dmTimeStamp"

  val sipQuery: String =
  //"last_value(voLTEDropEvt,true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as VoLTEDropEvt, " +
      "last_value(voLTEAbortEvt,true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as VoLTEAbortEvt, " +
      "last_value(voLTETriggerEvt,true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as VoLTETriggerEvt, " +
      "last_value(voLTECallEndEvt,true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as VoLTECallEndEvt, " +
      "last_value(voLTECallNormalRelEvt,true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as VoLTECallNormalRelEvt, " +
     "sum(NrRelease) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as nrreleasecnt, " +
     "last_value(NrRelease, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as nrReleaseEvt, "
  // "last_value(rtpDlSn) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as rtpDlSn "
//    "row_number() over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rn, "

  def getIpQuery(reportFlag: Boolean): String = {
    if (!reportFlag) {
      sipQuery.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      sipQuery.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }
}
