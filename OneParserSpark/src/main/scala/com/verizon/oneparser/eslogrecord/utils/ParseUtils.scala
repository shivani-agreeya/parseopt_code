package com.verizon.oneparser.eslogrecord.utils

import java.util.{Calendar, GregorianCalendar}

import com.esri.hex.HexGrid
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessDMATRecords.{DistanceCalculatorImpl, Location}

import scala.collection.immutable.{List, Nil}
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.Try

object ParseUtils extends Serializable with LazyLogging {

  def getPhychanBitMask(phyBitMask: String): String = {
    var PhyBitMask: String = ""
    if (phyBitMask.nonEmpty) {
      if (phyBitMask.indexOf(":") > 0) {
        PhyBitMask = phyBitMask.split(":")(0)
      } else if (phyBitMask.indexOf("_") > 0) {
        PhyBitMask = phyBitMask.split("_")(0)
      } else {
        PhyBitMask = phyBitMask
      }
    }
    PhyBitMask
  }

  def exponentialConvertor(input: String) = {
    var output: String = input
    if (input.nonEmpty) {
      import java.math.BigDecimal
      output = new BigDecimal(input).toPlainString
    }
    output
  }

  def getFemtoStatus(cellId: Int): Integer = {
    val eNBId: Int = (cellId & 0xFFFFFF00) >> 8
    var femtoStatus: Integer = 0
    if (eNBId >= 1024000 && eNBId <= 1048575) {
      femtoStatus = 1
    }
    femtoStatus
  }

  def isValidNumeric(input: String, dataType: String): Boolean = {
    var isValid: Boolean = false
    dataType.toUpperCase match {
      case "NUMBER" =>
        isValid = Try(input.toInt).isSuccess
      case "FLOAT" =>
        isValid = Try(input.toFloat).isSuccess
      case "DOUBLE" =>
        isValid = Try(input.toDouble).isSuccess
    }
    isValid
  }

  def getNrCarrierMcs(carrierId: List[String], NrMcs: List[String]) = {
    var servingCells: List[String] = null

    var Mcs: Array[(String, String)] = null
    if (carrierId != null && NrMcs != null) {
      try {
        Mcs = ((carrierId zip NrMcs).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + PhysicalCellID_b193 + ". Details : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrCarrierMcs for carrierId : " + carrierId + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getNrCarrierMcs : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrCarrierMcs Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing getNrCarrierMcs StackTrace: " + ge.printStackTrace())
      }
    }
    Mcs
  }

  def DistanceFrmCellsToLoc(CurrentLat: Double, CurrentLong: Double, PciLat: Double, PciLong: Double): Float = {
    new DistanceCalculatorImpl().calculateDistanceInMiles(Location(CurrentLat, CurrentLong), Location(PciLat, PciLong))
    //print(new DistanceCalculatorImpl().calculateDistanceInMiles(Location(CurrentLat, CurrentLong), Location(PciLocation.lat, PciLocation.lon)))
  }

  def getServingCellCaKPIs(servingCellIndex_B193: String, LteKPIParams: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var LTEKpi: Array[(String, String)] = null
    if (!servingCells.isEmpty && !LteKPIParams.isEmpty) {
      try {
        val LteKPI: List[String] = LteKPIParams.split(",").map(_.trim).toList
        LTEKpi = ((servingCells zip LteKPI).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + LteKPIParams + ". Details : " + ge.getMessage)
          logger.error("ES Exception occured while executing getServingCellPCI for Index : " + LteKPIParams + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
          logger.error("ES Exception occured while executing getServingCellPCI Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing getServingCellPCI StackTrace: " + ge.printStackTrace())
      }
    }
    LTEKpi
  }

  def getServingCellPCI(servingCellIndex_B193: String, PhysicalCellID_b193: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var PCI: Array[(String, Int)] = null
    if (!servingCells.isEmpty && !PhysicalCellID_b193.isEmpty) {
      try {
        val PhysicalCellID: List[Int] = PhysicalCellID_b193.split(",").map(_.trim).toList.map(_.toInt)
        PCI = ((servingCells zip PhysicalCellID).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + PhysicalCellID_b193 + ". Details : " + ge.getMessage)
          logger.error("ES Exception occured while executing getServingCellPCI for Index : " + PhysicalCellID_b193 + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
          logger.error("ES Exception occured while executing getServingCellPCI Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing getServingCellPCI StackTrace: " + ge.printStackTrace())
      }
    }
    PCI
  }

  def getPucchAdjTargetPwr(pucchTxPower: Int, pathLoss: Int, p0_NominalPUCCH: Integer, pucchType: String) = {
    var targetMobPUCCH: Int = 0
    var TxAdjustforPUCCH: Int = 0
    var pucchVal: java.lang.Float = null
    if (p0_NominalPUCCH != null && (p0_NominalPUCCH <= -96) && (pucchTxPower >= -60 && pucchTxPower <= 20)) {
      // Note: Following formulas are custom formula.
      try {
        targetMobPUCCH = pathLoss + p0_NominalPUCCH
        TxAdjustforPUCCH = pucchTxPower - targetMobPUCCH
        if (pucchType.toLowerCase == "adjtxpwr")
          pucchVal = TxAdjustforPUCCH.toFloat
        else
          pucchVal = targetMobPUCCH.toFloat
      }
      catch {
        case ge: Throwable =>
          logger.error("Exception occured while calulating " + pucchType + ". Details : " + ge.getMessage)
      }
    }
    pucchVal
  }

  def getNrServingCellKPIs(Nrs_ccindex: String, KPIParams: String) = {
    var servingCells: List[String] = null
    if (Nrs_ccindex != null) {
      servingCells = Nrs_ccindex.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
    }
    var Kpi: Array[(String, String)] = null
    if (!servingCells.isEmpty && !KPIParams.isEmpty) {
      try {
        val KPI: List[String] = KPIParams.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
        Kpi = ((servingCells zip KPI).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + LteKPIParams + ". Details : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrServingCell for Index : " + KPIParams + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getNrServingCell : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrServingCell Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing getNrServingCell StackTrace: " + ge.printStackTrace())
      }
    }
    Kpi
  }

  def maxDurationMsVal(a: Double, b: Double, c: Double, d: Double, e: Double): java.lang.Double = {
    var MaxDurationMs: java.lang.Double = 0.0
    try {
      var kpis = new ListBuffer[Double]()
      if (a >= 0.0) kpis += a.toDouble
      if (b >= 0.0) kpis += b.toDouble
      if (c >= 0.0) kpis += c.toDouble
      if (d >= 0.0) kpis += d.toDouble
      if (e >= 0.0) kpis += e.toDouble

      MaxDurationMs = kpis.toList.max.asInstanceOf[java.lang.Double]
    } catch {
      case ge: Throwable =>
        logger.error("Exception occured while determining Elapsed_Time: " + ge.getMessage)
    }
    MaxDurationMs
  }

  def getModulationType(input: String): Integer = {
    var modType: Integer = null
    try {
      if (input != null && !input.trim.isEmpty) {
        val modTypeDB: String = input
        if (!modTypeDB.isEmpty) {
          modTypeDB match {
            case "QPSK" => modType = 2
            case "16QAM" => modType = 4
            case "64QAM" => modType = 6
            case "256QAM" => modType = 8
          }
        }
      }
    }
    catch {
      case ge: Exception =>
        logger.info("Unable to flatten Modulation Type : " + ge.getMessage)
    }
    modType
  }

  def checkVolteCallFailure(subtitleMsg: String, sipCseq: String): java.lang.Integer = {
    var isVolteCallIneffective: java.lang.Integer = 1
    var subtitlePairs: List[String] = null
    var sipCseqPairs: List[String] = null

    if (subtitleMsg != null) {
      subtitlePairs = subtitleMsg.split(",").map(_.trim).toList
    }
    if (sipCseq != null) {
      sipCseqPairs = sipCseq.split(",").map(_.trim).toList
    }


    var sipSubtitlMsg_sCeq: Array[(String, String)] = null
    if (!subtitlePairs.isEmpty && !sipCseqPairs.isEmpty) {
      try {
        sipSubtitlMsg_sCeq = ((subtitlePairs zip sipCseqPairs).toMap).toArray

        var subititleandcSeqWithInvite: Boolean = false
        var subititleandcSeqWith_200andInvite: Boolean = false

        for (i <- 0 to sipSubtitlMsg_sCeq.size - 1) {
          sipSubtitlMsg_sCeq(i)._1 match {
            case "INVITE" =>
              if (sipSubtitlMsg_sCeq(i)._2 == "1 INVITE") {
                if (subititleandcSeqWith_200andInvite) {
                  isVolteCallIneffective = 0
                } else {
                  subititleandcSeqWithInvite = true
                }

              }
            case "200 OK" =>
              if (sipSubtitlMsg_sCeq(i)._2 == "1 INVITE") {
                if (subititleandcSeqWithInvite) {
                  isVolteCallIneffective = 0
                } else {
                  subititleandcSeqWith_200andInvite = true
                }

              }
            case _ =>
              isVolteCallIneffective = 1
          }
        }
      } catch {
        case ge: Exception =>
          logger.error("ES Exception occured while executing checkVolteCallFailure for seq : " + sipCseq + " Message : " + ge.getMessage)
          println("ES Exception occured while executing checkVolteCallFailure : " + ge.getMessage)
          logger.error("ES Exception occured while executing checkVolteCallFailure Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing checkVolteCallFailure StackTrace: " + ge.printStackTrace())
      }
    }
    isVolteCallIneffective
  }

  def getRtpGapType(timestampInSeconds: Float): java.lang.Integer = {
    var gapType: java.lang.Integer = null
    if (timestampInSeconds >= 0.5 && timestampInSeconds < 1) {
      gapType = 1
    } else if (timestampInSeconds >= 1 && timestampInSeconds < 3) {
      gapType = 2
    } else if (timestampInSeconds >= 3 && timestampInSeconds < 5) {
      gapType = 3
    } else if (timestampInSeconds >= 5 && timestampInSeconds < 10) {
      gapType = 4
    } else if (timestampInSeconds >= 10) {
      gapType = 5
    }
    gapType
  }

  def getLocValues(Mx: Double, My: Double, X: String, Y: String): Map[String, Any] = {
    val grid2 = HexGrid(2, -20000000.0, -20000000.0)
    val grid5 = HexGrid(5, -20000000.0, -20000000.0)
    val grid10 = HexGrid(10, -20000000.0, -20000000.0)
    val grid25 = HexGrid(25, -20000000.0, -20000000.0)
    val grid50 = HexGrid(50, -20000000.0, -20000000.0)
    val grid100 = HexGrid(100, -20000000.0, -20000000.0)
    val grid200 = HexGrid(200, -20000000.0, -20000000.0)
    val grid250 = HexGrid(250, -20000000.0, -20000000.0)
    val grid500 = HexGrid(500, -20000000.0, -20000000.0)
    val grid1000 = HexGrid(1000, -20000000.0, -20000000.0)
    val grid2000 = HexGrid(2000, -20000000.0, -20000000.0)
    val grid4000 = HexGrid(4000, -20000000.0, -20000000.0)
    val grid5000 = HexGrid(5000, -20000000.0, -20000000.0)
    val grid10000 = HexGrid(10000, -20000000.0, -20000000.0)
    val grid15000 = HexGrid(15000, -20000000.0, -20000000.0)
    val grid25000 = HexGrid(25000, -20000000.0, -20000000.0)
    val grid50000 = HexGrid(50000, -20000000.0, -20000000.0)
    val grid75000 = HexGrid(75000, -20000000.0, -20000000.0)
    val grid100000 = HexGrid(100000, -20000000.0, -20000000.0)

    Map("loc_2" -> grid2.convertXYToRowCol(Mx, My).toText, "loc_5" -> grid5.convertXYToRowCol(Mx, My).toText,
      "loc_10" -> grid10.convertXYToRowCol(Mx, My).toText, "loc_25" -> grid25.convertXYToRowCol(Mx, My).toText,
      "loc_50" -> grid50.convertXYToRowCol(Mx, My).toText, "loc_100" -> grid100.convertXYToRowCol(Mx, My).toText,
      "loc_200" -> grid200.convertXYToRowCol(Mx, My).toText,
      "loc_500" -> grid500.convertXYToRowCol(Mx, My).toText, "loc_1000" -> grid1000.convertXYToRowCol(Mx, My).toText,
      "loc_2000" -> grid2000.convertXYToRowCol(Mx, My).toText, "loc_4000" -> grid4000.convertXYToRowCol(Mx, My).toText,
      "loc_5000" -> grid5000.convertXYToRowCol(Mx, My).toText, "loc_10000" -> grid10000.convertXYToRowCol(Mx, My).toText,
      "loc_15000" -> grid15000.convertXYToRowCol(Mx, My).toText, "loc_25000" -> grid25000.convertXYToRowCol(Mx, My).toText,
      "loc_50000" -> grid50000.convertXYToRowCol(Mx, My).toText, "loc_75000" -> grid75000.convertXYToRowCol(Mx, My).toText,
      "loc_100000" -> grid100000.convertXYToRowCol(Mx, My).toText, "loc" -> f"${Y.toDouble}%.6f,${X.toDouble}%.6f",
      "loc_mx" -> Mx, "loc_my" -> My, "lat" -> Y.toDouble, "lon" -> X.toDouble)
  }

  import scala.collection.mutable._

  def getSysModeKPIs(mSysMode: Int, mStatusMode: Integer, HdrHybrid: Int): scala.collection.mutable.Map[String, Any] = {
    var systemMode: Array[String] = Array("No service", "AMPS", "CDMA", "GSM", "1xEV-DO", "WCDMA", "GPS", "GSM and WCDMA", "WLAN", "LTE", "GSM and WCDMA and LTE")
    var srvStatusMode: Array[String] = Array("No service", "Limited service", "Service available", "Limited regional service", "Power save")
    val sysModeKPIs = scala.collection.mutable.Map[String, Any]()
    try {
      if (mSysMode >= 0) {
        mSysMode match {
          case 0 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
          case 2 =>
            if (HdrHybrid == 0) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
            } else if (mStatusMode == 2) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> "1xEV-DO", "DataRATStatus" -> srvStatusMode(mStatusMode))
            } else if (mStatusMode >= 0) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> "No service", "DataRATStatus" -> srvStatusMode(0))
            }
          case 3 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(0), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
          case 4 =>
            sysModeKPIs += ("VoiceRAT" -> "No service", "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
          case 5 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
          case 9 =>
            sysModeKPIs += ("DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(mStatusMode))
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(mStatusMode))
        }
      }
    }
    catch {
      case ge: Exception =>
        logger.error("ES Exception occured while executing getSysModeKPIs for mSysMode : " + mSysMode + " Message : " + ge.getMessage)
        println("ES Exception occured while executing getSysModeKPIs : " + ge.getMessage)
        logger.error("ES Exception occured while executing getSysModeKPIs Cause: " + ge.getCause())
        logger.error("ES Exception occured while executing getSysModeKPIs StackTrace: " + ge.printStackTrace())
    }
    sysModeKPIs
  }

  def getSysModeKPIs0x134F(mSysMode0: Int, mSysMode1: Int, sysIDx184E: Int, sysModeOperational: Int, EmmSubState: String, mStatusMode0: Integer, mStatusMode1: Integer, HdrHybrid: Int): scala.collection.mutable.Map[String, Any] = {
    var systemMode: Array[String] = Array("No service", "AMPS", "CDMA", "GSM", "1xEV-DO", "WCDMA", "GPS", "GSM and WCDMA", "WLAN", "LTE", "GSM and WCDMA and LTE")
    var srvStatusMode: Array[String] = Array("No service", "Limited service", "Service available", "Limited regional service", "Power save")
    val sysModeKPIs = scala.collection.mutable.Map[String, Any]()
    try {
      if (mSysMode0 >= 0) {
        mSysMode0 match {
          case 0 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mStatusMode0), "VoiceRATStatus" -> srvStatusMode(mStatusMode0))
          case 2 => //System mode is CDMA
            sysModeKPIs += ("VoiceRAT" -> systemMode(mStatusMode0), "VoiceRATStatus" -> srvStatusMode(mStatusMode0))
          case 3 => //System mode is GSM
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
          case 4 => //System mode is EVDO
            sysModeKPIs += ("VoiceRAT" -> "No service", "VoiceRATStatus" -> srvStatusMode(mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
          case 5 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
          case 9 =>
            sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(mStatusMode0))
        }
      }

      if (mSysMode0 == 2) {
        if (sysModeOperational == 1) {
          mSysMode1 match {
            case 0 =>
              (if (mSysMode0 == 2) {
                sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
              } else Nil)
            case 4 =>
              sysModeKPIs += ("DataRat" -> systemMode(mSysMode1), "DataRATStatus" -> srvStatusMode(mStatusMode1))
            case 9 =>
              sysModeKPIs += ("DataRat" -> systemMode(mSysMode1), "DataRATStatus" -> srvStatusMode(mStatusMode1))
          }
        } else {
          sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
        }
      } else {
        sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(mStatusMode0))
      }
    }
    catch {
      case ge: Exception =>
        logger.error("ES Exception occured while executing getSysModeKPIs0x134F for mSysMode : " + mSysMode0 + " Message : " + ge.getMessage)
        println("ES Exception occured while executing getSysModeKPIs0x134F : " + ge.getMessage)
        logger.error("ES Exception occured while executing getSysModeKPIs0x134F Cause: " + ge.getCause())
        logger.error("ES Exception occured while executing getSysModeKPIs0x134F StackTrace: " + ge.printStackTrace())
    }
    sysModeKPIs
  }

  import java.math.BigDecimal

  def round(d: java.lang.Double, decimalPlace: Int): java.lang.Double = {
    var bd = new BigDecimal(d.toString)
    bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP)
    bd.doubleValue();
  }

  def getAvgRbUtilization(count: java.lang.Integer, numerology: String): java.lang.Double = {
    var avgRB: java.lang.Double = null
    if (numerology == "15kHz") {
      val slotDurationMs = 1
      avgRB = (count * slotDurationMs / 1000 * 100).toDouble
    } else if (numerology == "30kHz") {
      val slotDurationMs = 0.5
      avgRB = (count * slotDurationMs / 1000 * 100)
    } else if (numerology == "60kHz") {
      val slotDurationMs = 0.25
      avgRB = (count * slotDurationMs / 1000 * 100)
    } else if (numerology == "120kHz") {
      val slotDurationMs = 0.125
      avgRB = (count * slotDurationMs / 1000 * 100)
    }
    avgRB
  }

  def getCellSiteIndex(startDate: String, endDate: String, indexName: String): String = {
    var indexExpr: String = "";
    var i: Integer = 0
    var startWeek: Integer = 0
    var endWeek: Integer = 0

    var cal = new GregorianCalendar();
    var calEnd = new GregorianCalendar();
    var sStartDate = startDate.split("-")
    cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(sStartDate.apply(2)))
    cal.set(Calendar.MONTH, Integer.parseInt(sStartDate.apply(1)) - 1);
    cal.set(Calendar.YEAR, Integer.parseInt(sStartDate.apply(0)));
    var sEndDate = endDate.split("-")
    calEnd.set(Calendar.DAY_OF_MONTH, Integer.parseInt(sEndDate.apply(2)))
    calEnd.set(Calendar.MONTH, Integer.parseInt(sEndDate.apply(1)) - 1);
    calEnd.set(Calendar.YEAR, Integer.parseInt(sEndDate.apply(0)));
    startWeek = cal.get(Calendar.WEEK_OF_YEAR);
    endWeek = calEnd.get(Calendar.WEEK_OF_YEAR);
    if (endWeek >= startWeek && cal.getWeekYear() == calEnd.getWeekYear())
      for (i <- startWeek.toInt to endWeek.toInt)
        indexExpr = indexExpr + indexName + "-" + cal.getWeekYear() + "-" + i + ",";
    else {
      for (i <- startWeek.toInt to 52)
        indexExpr = indexExpr + indexName + "-"  + cal.get(Calendar.YEAR) + "-" + i + ","
      for (i <- 1 to endWeek.toInt)
        indexExpr = indexExpr + indexName + "-"  + calEnd.getWeekYear() + "-" + i + ",";
    }
    indexExpr.substring(0, indexExpr.length() - 1);
  }
}
