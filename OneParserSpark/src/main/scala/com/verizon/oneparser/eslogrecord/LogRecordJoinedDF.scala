package com.verizon.oneparser.eslogrecord

import org.apache.spark.sql.DataFrame

case class LogRecordJoinedDF(kpiisRachExists:Boolean = false,
                             kpiisRlcUlInstTputExists:Boolean = false,
                             kpiPdcpUlInstTputExists:Boolean = false,
                             kpiRlcInstTputExists:Boolean = false,
                             kpiPdcpDlExists:Boolean = false,
                             kpiDmatIpExists: Boolean = false,
                             kpiRlcDLTputExists:Boolean = false,
                             kpiNrDlScg5MsTputExists: Boolean = false,
                             kpiNrPhyULThroughputExists: Boolean = false,
                             kpi5gDlDataBytesExists: Boolean = false,
                             kpiGeoDistToCellsQuery: Boolean = false,
                             kpiisinbuilding: Boolean = false,
                             gNbidGeoDistToCellsQuery:Boolean=false,
                             joinedAllTables: DataFrame = null,
                             cachedDf: DataFrame = null
                            )
