package com.verizon.oneparser.eslogrecord

object B192Query {

  val query: String =
    " collect_list(EARFCN_B192) as collected_earfcn," +
    " collect_list(PhysicalCellID_B192) as collected_pci," +
    " collect_list(InstMeasuredRSRP_B192) as collected_rsrp," +
    " collect_list(InstRSRPRx_0_B192) as collected_rsrp0," +
    " collect_list(InstRSRPRx_1_B192) as collected_rsrp1," +
    " collect_list(InstRSRQ_B192) as collected_rsrq, " +
    " collect_list(InstRSRQRx_0_B192) as collected_rsrq0," +
    " collect_list(InstRSRQRx_1_B192) as collected_rsrq1," +
    " collect_list(InstRSSI_B192) as collected_rssi," +
    " collect_list(InstRSSIRx_0_B192) as collected_rssi0," +
    " collect_list(InstRSSIRx_1_B192) as collected_rssi1 "
}
