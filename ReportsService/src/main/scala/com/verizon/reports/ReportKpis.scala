package com.verizon.reports

case class ReportKpis(fileName: String, ltePccRsrp: Double, lTEPccSinr: Double, ltePccRsrq: Double, ltePccRssi: Double, lteBand: Integer, 
                      ltePccEarfcnDL: Integer, ltePccEarfcnUL: Integer,
                      ltePccPuschTxPwr: Double, ltePucchActTxPwr: Double, ltePuschTxPwr: Double, lteMacTputDl: Double, lteMacTputUl: Double,
                      ltePdschTput: Double, loc_2: String, lteCellID: Int, LTERlcTputDl: Double, LTERlcTputUl: Double, NCell10RSRP: Double,
                      NCell10RSRQ: Double, NCell10RSSI: Double, LTERrcState: String,
                      lteRrcConRej: Integer, lteRrcConRestRej: Integer, lteRrcConRelCnt: Integer, lteActDefContAcptCnt: Integer, 
                      lteActDedContAcptCnt: Integer, lteBearerModAcptCnt: Integer, lteHoFailure: Integer, lteIntraHoFail: Integer,
                      lteAttRej: Integer, lteActDefContRejCnt: Integer, lteActDedContRejCnt: Integer, lteBearerModRejCnt: Integer, 
                      lteReselFromGsmUmtsFail: Integer, lteRrcConSetupCnt: Integer, lteRrcConReqCnt: Integer, ltePdnConntReqCnt: Int, lteAttReqCnt: Integer,
                      lteRrcConSetupCmpCnt: Integer, lteAttAcptCnt: Integer, lteRrcConeconfCmpCnt: Integer,
                      lteRrcConReconfigCnt: Integer, lteDetachReqCnt: Integer, lteRrcConRestReqCnt: Integer, lteTaAcptCnt: Integer,
                      lteTaReqCnt: Integer, lteTaRej: Integer, lteAttCmpCnt: Integer,ltePccTputDL: Double, ltePccCqiCw0: Integer,
                      ltePccDlMcs: Double,ltePdschPrb: Double,ltePuschThroughput: Double, lteModUl: String,ltePuschPrb: Double)
