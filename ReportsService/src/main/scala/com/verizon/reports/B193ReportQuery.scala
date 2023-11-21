package com.verizon.reports

object B193ReportQuery {
  val query="""collect_list(PhysicalCellID_B193) as collectedB193_pci, 
    collect_list(ServingCellIndex_B193) as collectedB193_servingCell,
    collect_list(InstMeasuredRSRP) as collectedB193_rsrp,
    last_value(version) as version_B193, 
    last_value(SubPacketSize) as SubPacketSize_B193,
    collect_list(FTLSNRRx_0) as collectedB193_snir0, 
    collect_list(FTLSNRRx_1) as collectedB193_snir1,
    collect_list(InstRSRQ) as collectedB193_rsrq,
    collect_list(InstRSSI) as collectedB193_rssi, 
    collect_list(EARFCN_B193) as collectedB193_earfcn,
    collect_list(lteeNBId) as collectedQcomm_lteeNBId"""
  
  //collectedB193_pci,collectedB193_servingCell,collectedB193_rsrp,version_B193,
  //SubPacketSize_B193,collectedB193_snir0,collectedB193_snir1,collectedB193_rsrq,
  //collectedB193_rssi,collectedB193_earfcn
  def getQuery: String = query
  
  val reportColumns: String ="""PhysicalCellID_B193,ServingCellIndex_B193, InstMeasuredRSRP, version,SubPacketSize,
    FTLSNRRx_0,FTLSNRRx_1,InstRSRQ,InstRSSI,EARFCN_B193"""
  
  def getReportColumns: String=reportColumns
}