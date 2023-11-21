package com.verizon.oneparser.dto

case class ProcessParamValues(
                               id: Int = 33,
                               paramCode: String = "ONE_PARSER",
                               key: String = "DEV",
                               value: String = "Y",
                               description: String = "Parameter to control file processing",
                               creationTime: Long = 0L,
                               dateModified: Long = 0L,
                               currentWeek: String = "_2020_07Part2",
                               hiveStructChng: String = "N",
                               hiveCntChk: String = "N",
                               qcommCntChk: String = "Y",
                               qcomm2CntChk: String = "N",
                               qcomm5gCntChk: String = "N",
                               b192CntChk: String = "N",
                               b193CntChk: String = "N",
                               asnCntChk: String = "N",
                               nasCntChk: String = "N",
                               ipCntChk: String = "N",
                               qcommStructChng: String = "Y",
                               qcomm2StructChng: String = "N",
                               qcomm5gStructChng: String = "N",
                               b192StructChng: String = "N",
                               b193StructChng: String = "N",
                               asnStructChng: String = "N",
                               nasStructChng: String = "N",
                               ipStructChng: String = "N"
                             )

