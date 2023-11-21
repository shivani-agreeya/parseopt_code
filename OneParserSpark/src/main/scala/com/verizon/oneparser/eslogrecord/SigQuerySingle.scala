package com.verizon.oneparser.eslogrecord

object SigQuerySingle {
  val query: String =
    "logRecordName,dmTimeStamp, version, logCode, hexLogCode, " +
//      "last_value(Latitude, true) over(partition by filename, split(dmTimeStamp, '[\\.]')[0] order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Latitude_sig, " +
//      "last_value(Longitude, true) over(partition by filename, split(dmTimeStamp, '[\\.]')[0] order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Longitude_sig, " +
//      "row_number() over(partition by filename, split(dmTimeStamp, '[\\.]')[0] order by dmTimeStamp desc) as rn, " +
      "frequency_SIG, channelNumber_SIG, bandwidth_SIG, numSignals_SIG,cellId_SIG, RefPower_SIG,RefQuality_SIG,CINR_SIG,freqKpiList0x240b,frequency0x2406_SIG,bandwidth0x2406_SIG,numSignals0x2406_SIG, channelNumber0x2406_SIG," +
      "CINR0x2406_SIG,power0x2406_SIG,cpType0x2406_SIG,cellId0x2406_SIG,SecondarySyncQuality0x2406_SIG,PrimarySyncQuality0x2406_SIG,PrimarySyncPower0x2406_SIG,SecondarySyncPower0x2406_SIG,freqKpiList0x2406," +
      "frequency0x2408_SIG,bandwidth0x2408_SIG,numSignals0x2408_SIG,CINR0x2408_SIG,cellId0x2408_SIG,totalRsPower0x2408_SIG,overallPower0x2408_SIG,overallQuality0x2408_SIG,cpType0x2408_SIG,antQuality0x2408_SIG,antPower0x2408_SIG,carrierRSSI0x2408_SIG,numberOfTxAntennaPorts0x2408_SIG," +
      "frequency0x240D_SIG,numSignals0x240D_SIG,CINR0x240D_SIG,bandwidth0x240D_SIG, channelNumber0x240D_SIG, carrierRSSI0x240D_SIG, PCI0x240D_SIG, RSRP0x240D_SIG, RSRQ0x240D_SIG, numberOfTxAntennaPorts0x240D_SIG, " +
      "channelNumber0x2501_SIG, operatingBand0x2501_SIG, ssbRSSI0x2501_SIG, numSSBurstDataBlocks0x2501_SIG, PCI0x2501_SIG, frequency0x2501_SIG, subCarrierSpacing0x2501_SIG," +
      "numBeams0x2501_SIG, beamIndex0x2501_SIG, pssBRSRP0x2501_SIG, pssBRSRQ0x2501_SIG, pssCINR0x2501_SIG, sssBRSRP0x2501_SIG, sssBRSRQ0x2501_SIG, sssCINR0x2501_SIG, sssDelay0x2501_SIG, ssbBRSRP0x2501_SIG," +
      "ssbBRSRQ0x2501_SIG, ssbCINR0x2501_SIG, frequencyRSRP0x2157_SIG,pciKpi0x240C_SIG"
}
