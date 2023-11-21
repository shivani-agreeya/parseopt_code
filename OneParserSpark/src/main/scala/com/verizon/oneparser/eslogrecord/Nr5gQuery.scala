package com.verizon.oneparser.eslogrecord

import org.apache.spark.sql.functions.sum

object Nr5gQuery {

  val query: String =
    "last_value(version) as version_B975, " +
      "last_value(nrdlbler, true) as nrdlbler, " +
      "last_value(nrresidualbler, true) as nrresidualbler, " +
      "last_value(nrdlmodtype, true) as nrdlmodtype, " +
      "last_value(nrsdlfrequency, true) as nrsdlfrequency_B975,  " +
      "last_value(is5GTechnology, true) as is5GTechnology,  " +
      "last_value(NRServingBeamIndex, true) as NRServing_BeamIndex,  " +
      "last_value(PUSCHDLPathLoss_0xB8D2, true) as PUSCHDLPathLoss_0xB8D2,  " +
      "last_value(PUCCHDLPathLoss_0xB8D2, true) as PUCCHDLPathLoss_0xB8D2,  " +
      "last_value(SRSDLPathLoss_0xB8D2, true) as  SRSDLPathLoss_0xB8D2,  " +
      "last_value(PRACPathLoss_0xB8D2, true) as PRACPathLoss_0xB8D2,  " +
      "last_value( PUSCHTxPwr_0xB8D2, true) as  PUSCHTxPwr_0xB8D2,  " +
      "last_value(NRSNR_0xB8D8, true) as NRSNR_0xB8D8, " +
      "last_value(NRSBSNR_0xB8DD, true) as  NRSBSNR_0xB8DD, " +
      "last_value(NRSBSNRRx0_0xB8DD, true) as  NRSBSNRRx0_0xB8DD, " +
      "last_value(NRSBSNRRx1_0xB8DD, true) as  NRSBSNRRx1_0xB8DD, " +
      "last_value(NRSBSNRRx2_0xB8DD, true) as  NRSBSNRRx2_0xB8DD, " +
      "last_value(NRSBSNRRx3_0xB8DD, true) as  NRSBSNRRx3_0xB8DD, " +
      "collect_list(NRServingBeamIndex) as collectedB975_NRServingBeamIndex,"+
      /*"last_value( nr5grlculinstTput0xb868DiffSeconds, true) as  nr5grlculinstTput0xb868DiffSeconds,  " +
      "last_value( nr5grlculinstTput0xb868DiffTbs, true) as  nr5grlculinstTput0xb868DiffTbs,  " +
      "last_value( nr5gRxBytes0xB860DiffSeconds, true) as  nr5gRxBytes0xB860DiffSeconds,  " +
      "last_value( nr5gRxBytes0xB860DiffTbs, true) as  nr5gRxBytes0xB860DiffTbs,  " +
      "last_value(nr5gRlcDataBytes0xB84DDiffSeconds, true) as  nr5gRlcDataBytes0xB84DDiffSeconds,  " +
      "last_value( nr5gRlcDataBytes0xB84DDiff, true) as  nr5gRlcDataBytes0xB84DDiff,  " +
      "last_value(nr5GPdcpDl0xB842DiffSeconds, true) as  nr5GPdcpDl0xB842DiffSeconds,  " +
      "last_value(nr5gPdcpDl0xB842DataDiff, true) as  nr5gPdcpDl0xB842DataDiff,  " +*/
      "last_value(NRTxBeamIndexPCC, true) as NRTxBeamIndexPCC, " +
      "last_value(NRTxBeamIndexSCC1, true) as NRTxBeamIndexSCC1, " +
      "last_value(resource1_0xB891, true) as resource1_0xB891, " +
      "last_value(rachResult_0xb88a, true) as rachResult_0xb88a, " +
      "last_value(rachTriggered_0xb889, true) as rachTriggered_0xb889, " +
      "collect_list(nrsbrsrp) as collectedB975_nrsbrsrp, " +
      "collect_list(nrsbrsrq) as collectedB975_nrsbrsrq, " +
      "collect_list(nrsbsnr) as collectedB975_nrsbsnr, " +
      "first_value(NRSBSNR_0xB992, true) as NRSBSNR_0xB992, "+
      "last_value(NRServingRxBeamSNR0_0xB992, true) as NRServingRxBeamSNR0_0xB992, " +
      "last_value(NRServingRxBeamSNR1_0xB992, true) as NRServingRxBeamSNR1_0xB992, " +
      "collect_list(nrbeamcount) as collectedB975_nrbeamcount, " +
      "collect_list(ccindex) as collectedB975_ccindex, " +
      "collect_list(nrpci) as collectedB975_nrpci, " +
      "collect_list(NRSBRSRP_0xB97F) as collected0xb97f_nrsbrsrp, " +
      "last_value(NRSBRSRP_0xB97F, true) as nrsbrsrp_0xb97f, " +
      "last_value(CCIndex_0xB97F, true) as ccindex_0xb97f, " +
      "last_value(NRSBRSRQ_0xB97F, true) as nrsbrsq_0xb97f, " +
      "last_value(NRBEAMCOUNT_0XB97F, true) as nrbeamcount_0xb97f, " +
      "last_value(NRPCI_0XB97F, true) as nrpci_0xb97f, " +
      "last_value(NRServingBeamIndex_0xB97F, true) as nrservingbeamindex_0xb97f, " +
      "last_value(NRServingCellPci_0xB97F, true) as nrservingcellpci_0xb97f, " +
      "last_value(NRServingPCCPCI_0xB97F, true) as NRServingPCCPCI_0xB97F, " +
      "last_value(NRServingSCC1PCI_0xB97F, true) as NRServingSCC1PCI_0xB97F, " +
      "last_value(NRServingSCC2PCI_0xB97F, true) as NRServingSCC2PCI_0xB97F, " +
      "last_value(NRServingSCC3PCI_0xB97F, true) as NRServingSCC3PCI_0xB97F, " +
      "last_value(NRServingSCC4PCI_0xB97F, true) as NRServingSCC4PCI_0xB97F, " +
      "last_value(NRServingSCC5PCI_0xB97F, true) as NRServingSCC5PCI_0xB97F, " +
      "last_value(NRServingPCCRxBeamId0_0xB97F, true) as NRServingPCCRxBeamId0_0xB97F, " +
      "last_value(NRServingSCC1RxBeamId0_0xB97F, true) as NRServingSCC1RxBeamId0_0xB97F, " +
      "last_value(NRServingSCC2RxBeamId0_0xB97F, true) as NRServingSCC2RxBeamId0_0xB97F, " +
      "last_value(NRServingSCC3RxBeamId0_0xB97F, true) as NRServingSCC3RxBeamId0_0xB97F, " +
      "last_value(NRServingSCC4RxBeamId0_0xB97F, true) as NRServingSCC4RxBeamId0_0xB97F, " +
      "last_value(NRServingSCC5RxBeamId0_0xB97F, true) as NRServingSCC5RxBeamId0_0xB97F, " +
      "last_value(NRServingPCCRxBeamId1_0xB97F, true) as NRServingPCCRxBeamId1_0xB97F, " +
      "last_value(NRServingSCC1RxBeamId1_0xB97F, true) as NRServingSCC1RxBeamId1_0xB97F, " +
      "last_value(NRServingSCC2RxBeamId1_0xB97F, true) as NRServingSCC2RxBeamId1_0xB97F, " +
      "last_value(NRServingSCC3RxBeamId1_0xB97F, true) as NRServingSCC3RxBeamId1_0xB97F, " +
      "last_value(NRServingSCC4RxBeamId1_0xB97F, true) as NRServingSCC4RxBeamId1_0xB97F, " +
      "last_value(NRServingSCC5RxBeamId1_0xB97F, true) as NRServingSCC5RxBeamId1_0xB97F, " +
      "last_value(NRServingPCCRxBeamRSRP0_0xB97F, true) as NRServingPCCRxBeamRSRP0_0xB97F, " +
      "last_value(NRServingSCC1RxBeamRSRP0_0xB97F, true) as NRServingSCC1RxBeamRSRP0_0xB97F, " +
      "last_value(NRServingSCC2RxBeamRSRP0_0xB97F, true) as NRServingSCC2RxBeamRSRP0_0xB97F, " +
      "last_value(NRServingSCC3RxBeamRSRP0_0xB97F, true) as NRServingSCC3RxBeamRSRP0_0xB97F, " +
      "last_value(NRServingSCC4RxBeamRSRP0_0xB97F, true) as NRServingSCC4RxBeamRSRP0_0xB97F, " +
      "last_value(NRServingSCC5RxBeamRSRP0_0xB97F, true) as NRServingSCC5RxBeamRSRP0_0xB97F, " +
      "last_value(NRServingPCCRxBeamRSRP1_0xB97F, true) as NRServingPCCRxBeamRSRP1_0xB97F, " +
      "last_value(NRServingSCC1RxBeamRSRP1_0xB97F, true) as NRServingSCC1RxBeamRSRP1_0xB97F, " +
      "last_value(NRServingSCC2RxBeamRSRP1_0xB97F, true) as NRServingSCC2RxBeamRSRP1_0xB97F, " +
      "last_value(NRServingSCC3RxBeamRSRP1_0xB97F, true) as NRServingSCC3RxBeamRSRP1_0xB97F, " +
      "last_value(NRServingSCC4RxBeamRSRP1_0xB97F, true) as NRServingSCC4RxBeamRSRP1_0xB97F, " +
      "last_value(NRServingSCC5RxBeamRSRP1_0xB97F, true) as NRServingSCC5RxBeamRSRP1_0xB97F, " +
      "last_value(NRServingPCCRSRP_0xB97F, true) as NRServingPCCRSRP_0xB97F, " +
      "last_value(NRServingSCC1RSRP_0xB97F, true) as NRServingSCC1RSRP_0xB97F, " +
      "last_value(NRServingSCC2RSRP_0xB97F, true) as NRServingSCC2RSRP_0xB97F, " +
      "last_value(NRServingSCC3RSRP_0xB97F, true) as NRServingSCC3RSRP_0xB97F, " +
      "last_value(NRServingSCC4RSRP_0xB97F, true) as NRServingSCC4RSRP_0xB97F, " +
      "last_value(NRServingSCC5RSRP_0xB97F, true) as NRServingSCC5RSRP_0xB97F, " +
      "last_value(NRServingPCCRSRQ_0xB97F, true) as NRServingPCCRSRQ_0xB97F, " +
      "last_value(NRServingSCC1RSRQ_0xB97F, true) as NRServingSCC1RSRQ_0xB97F, " +
      "last_value(NRServingSCC2RSRQ_0xB97F, true) as NRServingSCC2RSRQ_0xB97F, " +
      "last_value(NRServingSCC3RSRQ_0xB97F, true) as NRServingSCC3RSRQ_0xB97F, " +
      "last_value(NRServingSCC4RSRQ_0xB97F, true) as NRServingSCC4RSRQ_0xB97F, " +
      "last_value(NRServingSCC5RSRQ_0xB97F, true) as NRServingSCC5RSRQ_0xB97F, " +
      "last_value(NRRankIndexPCC, true) as NRRankIndexPCC, " +
      "last_value(NRRankIndexSCC1, true) as NRRankIndexSCC1, " +
      "last_value(NRRankIndexSCC2, true) as NRRankIndexSCC2, " +
      "last_value(NRRankIndexSCC3, true) as NRRankIndexSCC3, " +
      "last_value(NRRankIndexSCC4, true) as NRRankIndexSCC4, " +
      "last_value(NRRankIndexSCC5, true) as NRRankIndexSCC5, " +
      "last_value(NRRankIndexSCC6, true) as NRRankIndexSCC6, " +
      "last_value(NRRankIndexSCC7, true) as NRRankIndexSCC7, " +
      "last_value(NRCQIWBPCC, true) as NRCQIWBPCC, " +
      "last_value(NRCQIWBSCC1, true) as NRCQIWBSCC1, " +
      "last_value(NRCQIWBSCC2, true) as NRCQIWBSCC2, " +
      "last_value(NRCQIWBSCC3, true) as NRCQIWBSCC3, " +
      "last_value(NRCQIWBSCC4, true) as NRCQIWBSCC4, " +
      "last_value(NRCQIWBSCC5, true) as NRCQIWBSCC5, " +
      "last_value(NRCQIWBSCC6, true) as NRCQIWBSCC6, " +
      "last_value(NRCQIWBSCC7, true) as NRCQIWBSCC7, " +
      "last_value(isLogCode0xB97F, true) as islogcode0xb97f, " +
      "last_value(NRBandType_0xB97F, true) as nrbandtype0xB97F, " +
      "last_value(NRConnectivityType_0xB825, true) as nrconnectivitytype0xB825, " +
      "last_value(NRConnectivityMode_0xB825, true) as nrconnectivitymode0xB825, " +
      "last_value(NRBandInd_0xB97F, true) as nrbandind0xB97F, " +
      "last_value(NRBandScc1_0xB97F, true) as nrbandScc10xB97F, " +
      "last_value(NRBandTypeSCC1_0xB97F, true) as nrbandtypeScc10xB97F, " +
      "last_value(NrPCCARFCN_0XB97F, true) as nrPccArfcn0xB97F, " +
      "last_value(NrSCC1ARFCN_0XB97F, true) as nrScc1Arfcn0xB97F, " +
      "collect_list(rachResult_0xb88a) as collectedb88a_rachResult,"+
      "collect_list(carrierid_0xb887) as collected0xb887_carrierid,"+
      "collect_list(CellId_0xB887) as collected0xb887_CellId,"+
      "collect_list(crcstatus_0xb887) as collected0xb887_crcstatus,"+
      "last_value(NRMIMOType_0xB887, true) as NRMIMOType_0xB887, " +
      "last_value(nrpdschtput, true) as nrpdschtput, " +
      "last_value(rsrp1_0xB891, true) as rsrp1_0xB891, " +
      "sum(pdschSum_0xB887) as pdschSum0xB887, " +
      "sum(pdschSumPcc_0xB887) as pdschSumPcc0xB887, " +
      "sum(pdschSumScc1_0xB887) as pdschSumScc10xB887, " +
      "sum(pdschSumScc2_0xB887) as pdschSumScc20xB887, " +
      "sum(pdschSumScc3_0xB887) as pdschSumScc30xB887, " +
      "sum(pdschSumScc4_0xB887) as pdschSumScc40xB887, " +
      "sum(pdschSumScc5_0xB887) as pdschSumScc50xB887, " +
      "sum(pdschSumScc6_0xB887) as pdschSumScc60xB887, " +
      "sum(pdschSumScc7_0xB887) as pdschSumScc70xB887, " +
      "sum(txNewTbSum_0xB883) as txNewTbSum0xB883," +
      "sum(txReTbSum_0xB883) as txReTbSum0xB883," +
      "sum(txTotalSum_0xB883) as txTotalSum0xB883,"+
      "last_value(numerology_0xB883, true) as numerology0xB883, " +
      "last_value(carrierId_0XB883, true) as carrierId0xB883, " +
      "last_value(harq_0XB883, true) as harq0xB883, " +
      "last_value(mcs_0XB883, true) as mcs0xB883, " +
      "last_value(numrbs_0XB883, true) as numrbs0xB883, " +
      "last_value(tbsize_0XB883, true) as tbsize0xB883, " +
      "last_value(txmode_0XB883, true) as txmode0xB883, " +
      "last_value(txtype_0XB883, true) as txtype0xB883, " +
      "last_value(bwpidx_0XB883, true) as bwpidx0xB883, " +
      "sum(numNEW_TX_0XB883) as numNEWTXSum0XB883, " +
      "sum(numRE_TX_0XB883) as numRETXSum0XB883, " +
      "sum(numNEW_TXPCC_0XB883) as numNEWTXPCCSum0XB883, " +
      "sum(numNEW_TXSCC1_0XB883) as numNEWTXSCC1Sum0XB883, " +
      "sum(numRE_TXPCC_0XB883) as numRETXPCCSum0XB883, " +
      "sum(numRE_TXSCC1_0XB883) as numRETXSCC1Sum0XB883, " +
      "sum(numRE_TXRV_0_0XB883) as numRETXRV0Sum0XB883, " +
      "sum(numRE_TXRV_1_0XB883) as numRETXRV1Sum0XB883, " +
      "sum(numRE_TXRV_2_0XB883) as numRETXRV2Sum0XB883, " +
      "sum(numRE_TXRV_3_0XB883) as numRETXRV3Sum0XB883, " +
      "last_value(NRPUSCHWaveformPCC, true) as NRPUSCHWaveformPCC, " +
      "last_value(NRPUSCHWaveformSCC1, true) as NRPUSCHWaveformSCC1, " +
      "first_value(firstSfn_15kHz_0xB887, true) as firstSfn15kHz_0xB887, " +
      "first_value(firstSfnSlot_15kHz_0xB887, true) as firstSfnSlot15kHz_0xB887, " +
      "first_value(firstSfn_30kHz_0xB887, true) as firstSfn30kHz_0xB887, " +
      "first_value(firstSfn_30kHz_0xB887, true) as firstSfnSlot30kHz_0xB887,  " +
      "first_value(firstSfn_60kHz_0xB887, true) as firstSfn60kHz_0xB887,  " +
      "first_value(firstSfnSlot_60kHz_0xB887, true) as firstSfnSlot60kHz_0xB887,  " +
      "first_value(firstSfn_120kHz_0xB887, true) as firstSfn120kHz_0xB887,  " +
      "first_value(firstSfnSlot_120kHz_0xB887, true) as firstSfnSlot120kHz_0xB887,  " +
      "first_value(firstSfn_240kHz_0xB887, true) as firstSfn240kHz_0xB887,  " +
      "first_value(firstSfnSlot_240kHz_0xB887, true) as  firstSfnSlot240kHz_0xB887,  " +
      "last_value(lastSfn_15kHz_0xB887, true) as lastSfn15kHz_0xB887,  " +
      "last_value( lastSfnSlot_15kHz_0xB887, true) as  lastSfnSlot15kHz_0xB887,  " +
      "last_value(lastSfn_30kHz_0xB887, true) as lastSfn30kHz_0xB887,  " +
      "last_value( lastSfnSlot_30kHz_0xB887, true) as  lastSfnSlot30kHz_0xB887,  " +
      "last_value(lastSfn_60kHz_0xB887, true) as lastSfn60kHz_0xB887,  " +
      "last_value( lastSfnSlot_60kHz_0xB887, true) as  lastSfnSlot60kHz_0xB887,  " +
      "last_value(lastSfn_120kHz_0xB887, true) as lastSfn120kHz_0xB887,  " +
      "last_value( lastSfnSlot_120kHz_0xB887, true) as  lastSfnSlot120kHz_0xB887,  " +
      "last_value(lastSfn_240kHz_0xB887, true) as lastSfn240kHz_0xB887,  " +
      "last_value( lastSfnSlot_240kHz_0xB887, true) as  lastSfnSlot240kHz_0xB887,  " +
      "last_value(nrSpcellPci, true) as  nrSpcellPci,  " +
      "last_value(NRCellId_0xB825, true) as  nrcellid,  " +
      "sum(NumReTx_cnt0_0xB887) as numReTxCnt0Sum0xB887, " +
      "sum(NumReTx_cnt1_0xB887) as numReTxCnt1Sum0xB887, " +
      "sum(NumReTx_cnt2_0xB887) as numReTxCnt2Sum0xB887, " +
      "sum(NumReTx_cnt3_0xB887) as numReTxCnt3Sum0xB887, " +
      "sum(NumReTx_cnt4_0xB887) as numReTxCnt4Sum0xB887, " +
      "sum(CRCFail_cnt0_0xB887) as CRCFailcnt0Sum0xB887, " +
      "sum(CRCFail_cnt1_0xB887) as CRCFailcnt1Sum0xB887, " +
      "sum(CRCFail_cnt2_0xB887) as CRCFailcnt2Sum0xB887, " +
      "sum(CRCFail_cnt3_0xB887) as CRCFailcnt3Sum0xB887, " +
      "sum(CRCFail_cnt4_0xB887) as CRCFailcnt4Sum0xB887, " +
      "last_value(cdrxkpi0xb890list, true) as cdrxkpilist_0xB890, " +
      "last_value(slot0xB890, true) as slot_0xB890, " +
      "last_value(sfn0xB890, true) as sfn_0xB890, " +
      "last_value(numerology0xB890, true) as numerology_0xB890 "

}


