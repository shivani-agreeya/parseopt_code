package com.verizon.reports

object B192ReportQuery {
  val reportQuery: String = """EARFCN_B192,PhysicalCellID_B192,InstMeasuredRSRP_B192,InstRSRQ_B192,
      InstRSSI_B192"""
  
  def getReportQuery: String = reportQuery
  // b192: collected_rsrp,collected_rsrq,collected_rssi,collected_pci
  
  
  
  
  def query: String="""
        collect_list(InstMeasuredRSRP_B192) as collected_rsrp,
        collect_list(InstRSRQ_B192) as collected_rsrq,
        collect_list(InstRSSI_B192) as collected_rssi,
        collect_list(PhysicalCellID_B192) as collected_pci  
    """
  
   def getQuery: String = query
  
}