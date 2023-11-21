package com.verizon.oneparser.eslogrecord

object QualCommQueryForSingle {
  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills = "dateWithoutMillis"
  val dateWithMills = "dmTimeStamp"
  val query: String =
    "last_value(carrierIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as carrierIndex, " +
      "last_value(carrierIndex0xB14D, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as carrierIndex0xB14D, " +
      "last_value(carrierIndex0xB14E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as carrierIndex0xB14E, " +
      "last_value(carrierIndex0xB126, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as carrierIndex0xB126, " +
      "sum(countLogcodeB126) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as countLogcodeB126, " +
      "last_value(numRecords0xB139, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB139, " +
      "last_value(numRecords0xB172, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB172, " +
      "last_value(numRecords0xB173, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB173, " +
      "last_value(numRecords0xB16E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB16E, " +
      "last_value(numRecords0xB16F, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB16F, " +
      "last_value(numRecords0xB18A, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB18A, " +
      "last_value(cQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW0, " +
      "last_value(cQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW1, " +
      "last_value(scc1CQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc1CQICW0, " +
      "last_value(scc1CQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc1CQICW1, " +
      "last_value(scc2CQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc2CQICW0, " +
      "last_value(scc2CQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc2CQICW1, " +
      "last_value(scc3CQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc3CQICW0, " +
      "last_value(scc3CQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc3CQICW1, " +
      "last_value(scc4CQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc4CQICW0, " +
      "last_value(scc4CQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc4CQICW1, " +
      "last_value(scc5CQICW0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc5CQICW0, " +
      "last_value(scc5CQICW1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc5CQICW1, " +
      "last_value(pccRankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pccRankIndex, " +
      "last_value(scc1RankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc1RankIndex, " +
      "last_value(scc2RankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc2RankIndex, " +
      "last_value(scc3RankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc3RankIndex, " +
      "last_value(scc4RankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc4RankIndex, " +
      "last_value(scc5RankIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc5RankIndex, " +
      "last_value(rankIndexStr0xB14E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rankIndexStr0xB14E, " +
      "last_value(cQICW1Show0xB14E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW1Show0xB14E, " +
      "last_value(cQICW00xB14E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW00xB14E, " +
      "last_value(cQICW10xB14E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW10xB14E, " +
      "last_value(rankIndexInt, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rankIndexInt, " +
      "last_value(rankIndexStr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rankIndexStr, " +
      "last_value(trackingAreaCode, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as trackingAreaCode, " +
      // "last_value(lteeNBId, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteeNBId, " +
      "last_value(ulFreq, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ulFreq, " +
      "last_value(dlFreq, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dlFreq, " +
      "last_value(ulBandwidth, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ulBandwidth, " +
      "last_value(dlBandwidth, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dlBandwidth, " +
      "last_value(modStream0, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modStream0, " +
      "last_value(modStream1, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modStream1, " +
      "last_value(pmiIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pmiIndex, " +
      "last_value(spatialRank, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as spatialRank, " +
      "last_value(numTxAntennas, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numTxAntennas, " +
      "last_value(numRxAntennas, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRxAntennas, " +
      "last_value(version, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as version, " +
      "last_value(modUL, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modUL, " +
      // "last_value(pUSCHTxPower, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower, " +
      "last_value(prb, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as prb, " +
      "last_value(ulcarrierIndex, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ulcarrierIndex, " +
      "last_value(timingAdvance, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as timingAdvance, " +
      "last_value(timingAdvanceIncl, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as timingAdvanceIncl, " +
      "last_value(srsTxPower, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as srsTxPower, " +
      "last_value(DLPathLoss0xB16E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as DLPathLoss0xB16E, " +
      "last_value(DLPathLoss0xB16F, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as DLPathLoss0xB16F, " +
      "last_value(PUCCH_Tx_Power0xB16E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as PUCCH_Tx_Power0xB16E, " +
      "last_value(PUCCH_Tx_Power0xB16F, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as PUCCH_Tx_Power0xB16F, " +
      "last_value(channel0x1017, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as channel0x1017, " +
      "last_value(channel0x119A, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as channel0x119A, " +
      "last_value(bandClass_QComm, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as bandClass_QComm, " +
      "last_value(pilotPN, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pilotPN, " +
      "last_value(pdnRat, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdnRat, " +
      "last_value(racId, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as racId, " +
      "last_value(lacId, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lacId, " +
      "last_value(psc, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as psc, " +
      "last_value(l2State, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as l2State, " +
      "last_value(rrState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rrState, " +
      "last_value(txPower, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as txPower, " +
      "last_value(rxLev, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rxLev, " +
      "last_value(bsicBcc, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as bsicBcc, " +
      "last_value(bsicNcc, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as bsicNcc, " +
      "last_value(Lai, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Lai, " +
      "last_value(physicalCellID_QComm, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as physicalCellID_QComm, " +
      // "last_value(lteCellID, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteCellID, " +
      "last_value(wcdmaCellID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wcdmaCellID, " +
      "last_value(gsmCellID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as gsmCellID, " +
      "last_value(frameNum, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as frameNum, " +
      "last_value(subFrameNum, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as subFrameNum, " +
      "last_value(currentSFNSF, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as currentSFNSF, " +
      "first_value(ServingCellIndex, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as ServingCellIndex, " +
      "lead(first_value(ServingCellIndex,true) over(partition by fileName, "+dateTimePlaceHolder+" order by dmTimeStamp asc),1) over(partition by fileName, "+dateTimePlaceHolder+"  order by dmTimeStamp asc) as LogPacket2_b173ServingCell, " +
      "lead(first_value(ServingCellIndex,true) over(partition by fileName, "+dateTimePlaceHolder+" order by dmTimeStamp asc),2) over(partition by fileName, "+dateTimePlaceHolder+"  order by dmTimeStamp asc) as LogPacket3_b173ServingCell, " +
      "last_value(TB_Size0xB139, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB139, " +
      "last_value(TB_Size0xB173, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB173, " +
      "last_value(numRecords0xB173, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB173, " +
      "last_value(CrcResult, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CrcResult, " +
      "last_value(numPRB0xB173, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numPRB0xB173, " +
      "last_value(Longitude, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Longitude, " +
      "last_value(Latitude, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Latitude, " +
      "last_value(Longitude2, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Longitude2, " +
      "last_value(Latitude2, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Latitude2, " +
      "last_value(pilotPNasList, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pilotPNasList, " +
      "last_value(cdmaTxStatus, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdmaTxStatus, " +
      "last_value(cdmaRfTxPower, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdmaRfTxPower, " +
      "last_value(txGainAdj, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as txGainAdj, " +
      "last_value(numLayers, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numLayers, " +
      "last_value(Gi, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Gi, " +
      "last_value(bcchArfcn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as bcchArfcn, " +
      "last_value(hrpdState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as hrpdState, " +
      "last_value(powerHeadRoom, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as powerHeadRoom, " +
      "last_value(evdoRxPwr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as evdoRxPwr, " +
      "last_value(evdoTxPwr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as evdoTxPwr, " +
      "last_value(modulationType, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modulationType, " +
      "last_value(vvqJitter, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as vvqJitter, " +
      "last_value(vvq, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as vvq, " +
      "last_value(rtpLoss, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rtpLoss, "+
      "first_value(distServingCellIndex_0xB173, true) over(partition by filename, "+dateTimePlaceHolder+ " order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as distServingCellIndex_0xB173, "
  /*+
      "row_number() over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rn "
   */
  def getQCommQuery(reportFlag: Boolean): String = {
    if (!reportFlag) {
      query.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      query.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }

  def main(args: Array[String]): Unit = {
    val strQuery = getQCommQuery(false)
    println(strQuery)
  }
  val queryPdcpRlcTput: String =
    "last_value(pdcpDlTotalBytes, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdcpDlTotalBytes, " +
      "last_value(pdcpDlTimeStamp, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdcpDlTimeStamp, " +
      "last_value(pdcpUlTotalBytes, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdcpUlTotalBytes, " +
      "last_value(pdcpUlTimeStamp, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pdcpUlTimeStamp, " +
      "last_value(rlcDlTotalBytes, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTotalBytes, " +
      "last_value(rlcDlTimeStamp, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rlcDlTimeStamp, " +
    /*  "first_value(rlcDlDataBytes, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as rlcDlBytes, " +
      "lag(first_value(rlcDlDataBytes, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc),1) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as prerlcDlBytes, " +
      "first_value(rlcRbConfigIndex, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as rlcRbConfigIndex, " +
      "lag(first_value(rlcRbConfigIndex, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc),1) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as prerlcRbConfigIndex, " +
      "first_value(rlcDlNumRBs, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as rlcDlNumRBs, " +
      "lag(first_value(rlcDlNumRBs, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc),1) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as prerlcDlNumRBs, " +
      "first_value(rlcDlTimeStamp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as rlcDlTimeStamp0xb087v49, " +
      "lag(first_value(rlcDlTimeStamp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc),1) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp asc) as preRlcDlTimeStamp0xb087v49, " +
*/      "last_value(rlcUlTotalBytes, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rlcUlTotalBytes, " +
      "last_value(rlcUlTimeStamp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rlcUlTimeStamp, "
     // "row_number() over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rn "

  def getQueryPdcpRlcTput(reportFlag: Boolean): String = {
    if (!reportFlag) {
      queryPdcpRlcTput.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      queryPdcpRlcTput.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }

  val query4gTput: String =
    "sum(pcellTBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pcellTBSizeSum0xB173, " +
      "sum(scc1TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc1TBSizeSum0xB173, " +
      "sum(scc2TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc2TBSizeSum0xB173, " +
      "sum(scc3TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc3TBSizeSum0xB173, " +
      "sum(scc4TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc4TBSizeSum0xB173, " +
      "sum(scc5TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc5TBSizeSum0xB173, " +
      "sum(scc6TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc6TBSizeSum0xB173, " +
      "sum(scc7TBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as scc7TBSizeSum0xB173, " +
      "sum(totalTBSize_0xB173) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as totalTBSizeSum0xB173, " +
      "first(firstFN_0xB173, true) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as firstFn_0xB173, " +
      "first(firstSFN_0xB173, true) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as firstSfn_0xB173, " +
      "last(lastFN_0xB173, true) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as lastFn_0xB173, " +
      "last(lastSFN_0xB173, true) over(partition by filename, " + dateTimePlaceHolder + "  order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as lastSfn_0xB173, "

  def getQuery4gTput(reportFlag: Boolean): String = {
    if (!reportFlag) {
      query4gTput.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      query4gTput.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }

  /*
carrierIndex  carrierIndex
carrierIndex0xB14D  carrierIndex0xB14D
carrierIndex0xB14E  carrierIndex0xB14E
carrierIndex0xB126  carrierIndex0xB126
numRecords0xB139  numRecords0xB139
numRecords0xB172  numRecords0xB172
numRecords0xB173  numRecords0xB173
numRecords0xB16E  numRecords0xB16E
numRecords0xB16F  numRecords0xB16F
numRecords0xB18A  numRecords0xB18A
cQICW0  cQICW0
cQICW1  cQICW1
rankIndexStr0xB14E  rankIndexStr0xB14E
cQICW1Show0xB14E  cQICW1Show0xB14E
cQICW00xB14E  cQICW00xB14E
cQICW10xB14E  cQICW10xB14E
rankIndexInt  rankIndexInt
rankIndexStr  rankIndexStr
trackingAreaCode  trackingAreaCode
ulFreq  ulFreq
dlFreq  dlFreq
ulBandwidth  ulBandwidth
dlBandwidth  dlBandwidth
modStream0  modStream0
modStream1  modStream1
pmiIndex  pmiIndex
spatialRank  spatialRank
numTxAntennas  numTxAntennas
numRxAntennas  numRxAntennas
version  version
modUL  modUL
pUSCHTxPower  pUSCHTxPower
prb  prb
ulcarrierIndex  ulcarrierIndex
timingAdvance  timingAdvance
timingAdvanceIncl  timingAdvanceIncl
srsTxPower  srsTxPower
DLPathLoss0xB16E  DLPathLoss0xB16E
DLPathLoss0xB16F  DLPathLoss0xB16F
PUCCH_Tx_Power0xB16E  PUCCH_Tx_Power0xB16E
PUCCH_Tx_Power0xB16F  PUCCH_Tx_Power0xB16F
channel0x1017  channel0x1017
channel0x119A  channel0x119A
bandClass  bandClass
pilotPN  pilotPN
pdnRat  pdnRat
racId  racId
lacId  lacId
psc  psc
l2State  l2State
rrState  rrState
txPower  txPower
rxLev  rxLev
bsicBcc  bsicBcc
bsicNcc  bsicNcc
Lai  Lai
physicalCellID  physicalCellID
lteCellID  lteCellID
wcdmaCellID  wcdmaCellID
gsmCellID  gsmCellID
frameNum  frameNum
subFrameNum  subFrameNum
currentSFNSF  currentSFNSF
ServingCellIndex  ServingCellIndex
TB_Size0xB139  TB_Size0xB139
TB_Size0xB173  TB_Size0xB173
numRecords0xB173  numRecords0xB173
CrcResult  CrcResult
numPRB0xB173  numPRB0xB173
Longitude  Longitude
Latitude  Latitude
Longitude2  Longitude2
Latitude2  Latitude2
mx  mx
my  my
mx2  mx2
my2  my2
pilotPNasList  pilotPNasList
rfTxPower  rfTxPower
txGainAdj  txGainAdj
numLayers  numLayers
Gi  Gi
bcchArfcn  bcchArfcn
hrpdState  hrpdState
powerHeadRoom  powerHeadRoom
evdoRxPwr  evdoRxPwr
evdoTxPwr  evdoTxPwr
modulationType  modulationType
vvqJitter  vvqJitter
vvq  vvq
rtpLoss  rtpLoss

   *    *
   * */

  /*def main(args: Array[String]): Unit  ={
    val searchPattern="last_value("
    var searchIndexStart = 0
    var expr=true
    while (expr){
      val lvalIndex=query.indexOf(searchPattern,searchIndexStart)
      if(lvalIndex<0){
        expr=false
      }else{
        searchIndexStart=query.indexOf(")",lvalIndex+searchPattern.length())
        val columnName=query.substring(lvalIndex+searchPattern.length,query.indexOf(",",lvalIndex+searchPattern.length))
        //val alias=query.substring(searchIndexStart,query.indexOf("as",lvalIndex+searchPattern.length))
        val asIndex=query.indexOf("as",searchIndexStart)+3
        val alias=query.substring(asIndex,query.indexOf(",",asIndex))
        searchIndexStart=searchIndexStart+asIndex
        println(columnName+"  "+alias)

      }

    }

  }*/
}
