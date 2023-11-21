package com.verizon.oneparser

import java.util.Date

case class SequenceObj(index: Int = 0, bufPtr: Int = 0, endOfFile:Boolean = false, temp: Array[Byte] = new Array[Byte](0), LcTime:Date=null,
                       logCodesSet: scala.collection.mutable.HashSet[String]= scala.collection.mutable.HashSet.empty[String],
                       rmDeviceId:String="",rmMdn:String="",seqFileName:String="",is0xFFF3Exists:Boolean=false)
