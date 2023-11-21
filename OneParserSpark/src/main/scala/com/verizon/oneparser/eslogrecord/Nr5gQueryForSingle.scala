package com.verizon.oneparser.eslogrecord

import org.apache.spark.sql.functions.sum

object Nr5gQueryForSingle {

  val query: String =
    "version,nrdlbler,nrresidualbler,nrdlmodtype,nrsdlfrequency, is5GTechnology, NRServingBeamIndex, PUSCHDLPathLoss_0xB8D2, PUCCHDLPathLoss_0xB8D2, " +
      "SRSDLPathLoss_0xB8D2, PRACPathLoss_0xB8D2, PUSCHTxPwr_0xB8D2, nrsbrsrp, nrsbrsrq, nrsbsnr, NRSBSNR_0xB992, nrbeamcount, ccindex, nrpci, NRSBRSRP_0xB97F, CCIndex_0xB97F, NRSBRSRQ_0xB97F," +
      "NRSNR_0xB8D8, NRSBSNR_0xB8DD, NRSBSNRRx0_0xB8DD, NRSBSNRRx1_0xB8DD, NRSBSNRRx2_0xB8DD, NRSBSNRRx3_0xB8DD, " +
      "NRBEAMCOUNT_0XB97F, NRPCI_0XB97F, NRServingBeamIndex_0xB97F, NRServingCellPci_0xB97F, isLogCode0xB97F, NRBandType_0xB97F, NRConnectivityType_0xB825,NRConnectivityMode_0xB825,NRBandInd_0xB97F, nrpdschtput, pdschSum_0xB887, pdschSumPcc_0xB887, " +
      "pdschSumScc1_0xB887, pdschSumScc2_0xB887, pdschSumScc3_0xB887, pdschSumScc4_0xB887, pdschSumScc5_0xB887, pdschSumScc6_0xB887, pdschSumScc7_0xB887, " +
      "firstSfn_15kHz_0xB887, firstSfnSlot_15kHz_0xB887, firstSfn_30kHz_0xB887, firstSfn_30kHz_0xB887, firstSfn_60kHz_0xB887, firstSfnSlot_60kHz_0xB887, firstSfn_120kHz_0xB887, " +
      "firstSfnSlot_120kHz_0xB887, firstSfn_240kHz_0xB887, firstSfnSlot_240kHz_0xB887, lastSfn_15kHz_0xB887, lastSfnSlot_15kHz_0xB887, lastSfn_30kHz_0xB887, lastSfnSlot_30kHz_0xB887, " +
      "lastSfn_60kHz_0xB887, lastSfnSlot_60kHz_0xB887, lastSfn_120kHz_0xB887, lastSfnSlot_120kHz_0xB887, lastSfn_240kHz_0xB887, lastSfnSlot_240kHz_0xB887, TB_Size_0xB888, TB_SizePcc_0xB888, TB_SizeScc1_0xB888, TB_SizeScc2_0xB888, TB_SizeScc3_0xB888," +
      "TB_SizeScc4_0xB888, TB_SizeScc5_0xB888, TB_SizeScc6_0xB888, TB_SizeScc7_0xB888, CarrierID_0xB887, numerology_0xB887, NRMIMOType_0xB887, NRTxBeamIndexPCC, NRTxBeamIndexSCC1, " +
      "rsrp1_0xB891,resource1_0xB891,RachReason,rachAttempted_0xb88a,rachResult_0xb88a,rachTriggered_0xb889,txNewTbSum_0xB883,txReTbSum_0xB883,tbNewTxBytesB881,numNewTxTbB881,numReTxTbB881,nr5GPhyULInstTputPCC_0XB883, nr5GPhyULInstTputSCC1_0XB883," +
      "nr5GPhyULInstTput_0XB883, nr5GPhyULInstRetransRatePCC_0XB883, nr5GPhyULInstRetransRateSCC1_0XB883, nr5GPhyULInstRetransRate_0XB883, txTotalSum_0xB883,numerology_0xB883, carrierId_0XB883, harq_0XB883, mcs_0XB883, numrbs_0XB883, tbsize_0XB883, txmode_0XB883, txtype_0XB883, bwpidx_0XB883, totalTxBytes_0xB868,totalNumRxBytes_0xB860,rlcDataBytesVal_0xB84D, " +
      "numNEW_TX_0XB883, numRE_TX_0XB883, numNEW_TXPCC_0XB883, numNEW_TXSCC1_0XB883, numRE_TXPCC_0XB883, numRE_TXSCC1_0XB883, numRE_TXRV_0_0XB883, numRE_TXRV_1_0XB883, numRE_TXRV_2_0XB883, numRE_TXRV_3_0XB883, NRPUSCHWaveformPCC, NRPUSCHWaveformSCC1, " +
      "dataPDUBytesReceivedAllRBSum_0xB842, controlPDUBytesReceivedAllRBSum_0xB842, crcstatus_0xb887, nrSpcellPci, NRCellId_0xB825,NumReTx_cnt0_0xB887, NumReTx_cnt1_0xB887,NumReTx_cnt2_0xB887,NumReTx_cnt3_0xB887, NumReTx_cnt4_0xB887, CRCFail_cnt0_0xB887,CRCFail_cnt1_0xB887,CRCFail_cnt2_0xB887, CRCFail_cnt3_0xB887, CRCFail_cnt4_0xB887,CellId_0xB887, cdrxkpi0xb890list,slot0xB890,sfn0xB890,numerology0xB890, " +
      "numMissPDUToUpperLayerSum_0xB842, numDataPDUReceivedSum_0xB842, totalNumDiscardPacketsSum_0xB860, numRXPacketsSum_0xB860, totalReTxPDUsSum_0x0xB868, totalTxPDUsSum_0x0xB868, " +
      "numReTxPDUSum_0xB84D, numMissedUMPDUSum_0xB84D, numDroppedPDUSum_0xB84D, numDataPDUSum_0xB84D, " +
      "NRServingPCCPCI_0xB97F, NRServingSCC1PCI_0xB97F, NRServingSCC2PCI_0xB97F, NRServingSCC3PCI_0xB97F, NRServingSCC4PCI_0xB97F, NRServingSCC5PCI_0xB97F, " +
      "NRServingPCCRxBeamId0_0xB97F, NRServingSCC1RxBeamId0_0xB97F, NRServingSCC2RxBeamId0_0xB97F, NRServingSCC3RxBeamId0_0xB97F, NRServingSCC4RxBeamId0_0xB97F, NRServingSCC5RxBeamId0_0xB97F, " +
      "NRServingPCCRxBeamId1_0xB97F, NRServingSCC1RxBeamId1_0xB97F, NRServingSCC2RxBeamId1_0xB97F, NRServingSCC3RxBeamId1_0xB97F, NRServingSCC4RxBeamId1_0xB97F, NRServingSCC5RxBeamId1_0xB97F, " +
      "NRServingPCCRxBeamRSRP0_0xB97F, NRServingSCC1RxBeamRSRP0_0xB97F, NRServingSCC2RxBeamRSRP0_0xB97F, NRServingSCC3RxBeamRSRP0_0xB97F, NRServingSCC4RxBeamRSRP0_0xB97F, NRServingSCC5RxBeamRSRP0_0xB97F, " +
      "NRServingPCCRxBeamRSRP1_0xB97F, NRServingSCC1RxBeamRSRP1_0xB97F, NRServingSCC2RxBeamRSRP1_0xB97F, NRServingSCC3RxBeamRSRP1_0xB97F, NRServingSCC4RxBeamRSRP1_0xB97F, NRServingSCC5RxBeamRSRP1_0xB97F, " +
      "NRServingRxBeamSNR0_0xB992, NRServingRxBeamSNR1_0xB992, " +
      "NRServingPCCRSRP_0xB97F, NRServingSCC1RSRP_0xB97F, NRServingSCC2RSRP_0xB97F, NRServingSCC3RSRP_0xB97F, NRServingSCC4RSRP_0xB97F, NRServingSCC5RSRP_0xB97F, " +
      "NRServingPCCRSRQ_0xB97F, NRServingSCC1RSRQ_0xB97F, NRServingSCC2RSRQ_0xB97F, NRServingSCC3RSRQ_0xB97F, NRServingSCC4RSRQ_0xB97F, NRServingSCC5RSRQ_0xB97F, " +
      "NRRankIndexPCC, NRRankIndexSCC1, NRRankIndexSCC2, NRRankIndexSCC3, NRRankIndexSCC4, NRRankIndexSCC5, NRRankIndexSCC6, NRRankIndexSCC7, " +
      "NRCQIWBPCC, NRCQIWBSCC1, NRCQIWBSCC2, NRCQIWBSCC3, NRCQIWBSCC4, NRCQIWBSCC5, NRCQIWBSCC6, NRCQIWBSCC7,NRBandTypeSCC1_0xB97F,NRBandScc1_0xB97F,NrPCCARFCN_0XB97F,NrSCC1ARFCN_0XB97F  "
}


