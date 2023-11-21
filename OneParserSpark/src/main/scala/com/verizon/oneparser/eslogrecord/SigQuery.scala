package com.verizon.oneparser.eslogrecord

object SigQuery {
  val query: String =
    " last_value(hexLogCode, null) as hexLogCode_sig," +
      /*" last_value(frequency_SIG, null) as frequency_sig," +
      " last_value(channelNumber_SIG,null) as channelNumber_sig," +*/
      /*  " last_value(Latitude,null) as Latitude_sig," +
      " last_value(Longitude,null) as Longitude_sig," +*/
      /*" last_value(bandwidth_SIG, null) as bandwidth_sig," +*/
      " last_value(logRecordName, null) as logRecordName_sig," +
      /*" last_value(frequency0x2406_SIG, null) as frequency0x2406_sig," +
      " last_value(bandwidth0x2406_SIG, null) as bandwidth0x2406_sig," +
      " last_value(numSignals0x2406_SIG, null) as numSignals0x2406_sig," +
      " last_value(channelNumber0x2406_SIG, null) as channelNumber0x2406_sig," +
      " last_value(power0x2406_SIG, null) as power0x2406_sig," +*/
      " last_value(frequency0x2408_SIG, null) as frequency0x2408_sig," +
      " last_value(bandwidth0x2408_SIG, null) as bandwidth0x2408_sig," +
      " last_value(carrierRSSI0x2408_SIG, null) as channelPower0x2408_sig," +
      " last_value(numSignals0x2408_SIG, null) as numSignals0x2408_sig," +
      " last_value(frequency0x240D_SIG, null) as frequency0x240D_sig," +
      " last_value(numSignals0x240D_SIG, null) as numSignals0x240D_sig," +
      " last_value(bandwidth0x240D_SIG, null) as bandwidth0x240D_sig," +
      " last_value(channelNumber0x240D_SIG, null) as channelNumber0x240D_sig," +
      " last_value(carrierRSSI0x240D_SIG, null) as carrierRSSI0x240D_sig," +
      /* " collect_list(CINR0x2406_SIG) as collected_CINR0x2406_sig," +
      " collect_list(cpType0x2406_SIG) as collected_cpType0x2406_sig," +
      " collect_list(cellId0x2406_SIG) as collected_cellId0x2406_sig,"+
      " collect_list(SecondarySyncQuality0x2406_SIG) as collected_SecondarySyncQuality0x2406_sig," +
      " collect_list(PrimarySyncQuality0x2406_SIG) as collected_PrimarySyncQuality0x2406_sig," +
      " collect_list(PrimarySyncPower0x2406_SIG) as collected_PrimarySyncPower0x2406_sig," +
      " collect_list(SecondarySyncPower0x2406_SIG) as collected_SecondarySyncPower0x2406_sig," +*/
      " collect_list(CINR0x2408_SIG) as collected_CINR0x2408_sig," +
      " collect_list(cellId0x2408_SIG) as collected_cellId0x2408_sig," +
      " collect_list(totalRsPower0x2408_SIG) as collected_totalRsPower0x2408_sig," +
      " collect_list(overallPower0x2408_SIG) as collected_overallPower0x2408_sig," +
      " collect_list(overallQuality0x2408_SIG) as collected_overallQuality0x2408_sig," +
      " collect_list(cpType0x2408_SIG) as collected_cpType0x2408_sig," +
      " collect_list(antQuality0x2408_SIG) as collected_antQuality0x2408_sig," +
      " collect_list(antPower0x2408_SIG) as collected_antPower0x2408_sig," +
      " collect_list(numberOfTxAntennaPorts0x2408_SIG) as collected_numberOfTxAntennaPorts0x2408_sig," +
      " collect_list(CINR0x240D_SIG) as collected_CINR0x240D_sig," +
      " collect_list(PCI0x240D_SIG) as collected_pci0x240D_sig," +
      " collect_list(RSRP0x240D_SIG) as collected_Rsrp0x240D_sig," +
      " collect_list(RSRQ0x240D_SIG) as collected_Rsrq0x240D_sig," +
      " collect_list(numberOfTxAntennaPorts0x240D_SIG) as collected_numberOfTxAntennaPorts0x240D_sig," +
      " collect_list(frequencyRSRP0x2157_SIG) as collected_frequencyRSRP0x2157_SIG"
  var query0x2501:String =
    " first_value(numSSBurstDataBlocks0x2501_SIG, true) as numSSBurstDataBlocks0x2501_sig," +
    " first_value(operatingBand0x2501_SIG, true) as operatingBand0x2501_sig," +
    " first_value(ssbRSSI0x2501_SIG, true) as ssbRSSI0x2501_sig," +
    " first_value(channelNumber0x2501_SIG, true) as channelNumber0x2501_sig," +
    " first_value(PCI0x2501_SIG, true) as pci0x2501_sig," +
    " first_value(frequency0x2501_SIG, true) as frequency0x2501_sig," +
    " first_value(subCarrierSpacing0x2501_SIG, true) as subcarrierspacing0x2501_sig," +
    " first_value(numBeams0x2501_SIG, true) as numbeams0x2501_sig," +
    " first_value(beamIndex0x2501_SIG, true) as beamIndex0x2501_sig," +
    " first_value(pssBRSRP0x2501_SIG, true) as pssBrsrp0x2501_sig," +
    " first_value(pssBRSRQ0x2501_SIG, true) as pssBrsrq0x2501_sig," +
    " first_value(pssCINR0x2501_SIG, true) as pssCinr0x2501_sig," +
    " first_value(sssBRSRP0x2501_SIG, true) as sssBrsrp0x2501_sig," +
    " first_value(sssBRSRQ0x2501_SIG, true) as sssBrsrq0x2501_sig," +
    " first_value(sssCINR0x2501_SIG, true) as sssCinr0x2501_sig," +
    " first_value(sssDelay0x2501_SIG, true) as sssDelay0x2501_sig," +
    " first_value(ssbBRSRP0x2501_SIG, true) as ssbBrsrp0x2501_sig," +
    " first_value(ssbBRSRQ0x2501_SIG, true) as ssbBrsrq0x2501_sig," +
    " first_value(ssbCINR0x2501_SIG, true) as ssbCinr0x2501_sig"
}
