package com.verizon.oneparser.eslogrecord

object B193Query {
  



  val query: String =
    " last_value(version, true) as version_B193," +
      " last_value(SubPacketSize, true) as SubPacketSize_B193," +
      " collect_list(EARFCN_B193) as collectedB193_earfcn," +
      " collect_list(PhysicalCellID_B193) as collectedB193_pci," +
      " collect_list(ServingCellIndex_B193) as collectedB193_servingCell," +
      " collect_list(InstMeasuredRSRP) as collectedB193_rsrp," +
      " collect_list(InstRSRPRx_0) as collectedB193_rsrp0," +
      " collect_list(InstRSRPRx_1) as collectedB193_rsrp1," + "" +
      " collect_list(InstRSRPRx_2) as collectedB193_rsrp2, " +
      " collect_list(InstRSRPRx_3) as collectedB193_rsrp3," +
      " collect_list(InstRSRQ) as collectedB193_rsrq," +
      " collect_list(InstRSRQRx_0) as collectedB193_rsrq0," +
      " collect_list(InstRSRQRx_1) as collectedB193_rsrq1," +
      " collect_list(InstRSRQRx_2) as collectedB193_rsrq2," +
      " collect_list(InstRSRQRx_3) as collectedB193_rsrq3," +
      " collect_list(InstRSSI) as collectedB193_rssi, " +
      " collect_list(InstRSSIRx_0) as collectedB193_rssi0," +
      " collect_list(InstRSSIRx_1) as collectedB193_rssi1, " +
      " collect_list(InstRSSIRx_2) as collectedB193_rssi2," +
      " collect_list(InstRSSIRx_3) as collectedB193_rssi3," +
      " collect_list(FTLSNRRx_0) as collectedB193_snir0," +
      " collect_list(FTLSNRRx_1) as collectedB193_snir1," +
      " collect_list(FTLSNRRx_2) as collectedB193_snir2," +
      " collect_list(FTLSNRRx_3) as collectedB193_snir3," +
      " collect_list(validRx) as collectedB193_validRx," +
      " collect_list(horxd_mode) as collectedB193_horxd_mode," +
      " collect_list(lteeNBId) as collectedQcomm_lteeNBId "
      
}
