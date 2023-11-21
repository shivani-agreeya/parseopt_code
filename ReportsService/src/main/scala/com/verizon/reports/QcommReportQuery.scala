package com.verizon.reports

object QcommReportQuery {
  
  val query="""
      last_value(ulFreq, true) over (partition by filename,dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) ulFreq,
      last_value(dlFreq, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dlFreq,
      last_value(ulcarrierIndex, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ulcarrierIndex,
      last_value(TB_Size0xB139, true) over(partition by filename, dmTimeStamp  order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB139,
      last_value(currentSFNSF, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as currentSFNSF,
      last_value(pUSCHTxPower, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as pUSCHTxPower,
      last_value(PUCCH_Tx_Power0xB16F, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as PUCCH_Tx_Power0xB16F,
      last_value(numRecords0xB16F, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB16F,
      last_value(lteCellID, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteCellID,       
      last_value(cQICW0, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cQICW0,
      last_value(numRecords0xB173, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB173,
      last_value(ServingCellIndex, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ServingCellIndex, 
      last_value(CrcResult, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CrcResult,
      last_value(modulationType, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modulationType,
      last_value(numPRB0xB173, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numPRB0xB173,
      last_value(TB_Size0xB139, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as TB_Size0xB139,
      last_value(numRecords0xB139, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as numRecords0xB139,    
      last_value(modUL, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modUL, 
      last_value(prb, true) over(partition by filename, dmTimeStamp order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as prb
    """
  
  def getQuery: String = query
  
}