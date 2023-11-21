package com.verizon.oneparser.writesequencetohdfs

import com.dmat.qc.parser.IpConstruct
import com.dmat.qc.parser.IpUtils
import com.vzwdt.parseutils.ParseUtils
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import java.nio.ByteBuffer
import java.util.*

object Parser {
    private val logger = getLogger(javaClass)

    fun nextRecord(din: FSDataInputStream, writer: SequenceFile.Writer, seqObj: SequenceObj) {
        val key = IntWritable()
        getNextBytes(2, din, seqObj)
        val numBytes = seqObj.temp ?: return

        val length = ParseUtils.intFromBytes(numBytes[1], numBytes[0])

        if (length > 4) {
            getNextBytes(2, din, seqObj)
            val numBytes1 = seqObj.temp
            if (numBytes1 != null) {
                val logCode = ParseUtils.intFromBytes(numBytes1[1], numBytes1[0])
                getNextBytes(length - 4, din, seqObj)
                val remainder = seqObj.temp
                val byteBuffer = ByteBuffer.allocate(length)
                byteBuffer.put(numBytes)
                byteBuffer.put(numBytes1)
                byteBuffer.put(remainder)
                var logRecord: ByteArray? = byteBuffer.array()
                val timeStamp: Date = ParseUtils.parseTimestamp(logRecord)
                if (logCode < 0xFFF0 && isValidDate(timeStamp)) {
                    seqObj.lcTime = timeStamp
                }
                if (logCode == 0x11EB) {
                    val mConstructIpPacket = IpConstruct(IpUtils.getInstance())
                    val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)

                    logRecord = ipPacket
                }
                logRecord?.let { saveRecord(it, seqObj, key, writer) }
            }
        } else seqObj.endOfFile = true
    }

    fun nextRecordSig(din: FSDataInputStream, writer: SequenceFile.Writer, seqObj: SequenceObj) {
        val key = IntWritable()
        // Gets first 2 bytes of inputstream which is record length
        getNextBytes(2, din, seqObj)
        val numBytes = seqObj.temp ?: return
        val deviceTypesSet = intArrayOf(2, 8, 9, 10, 15, 16, 17, 18, 20, 21, 23, 25, 31, 33, 34, 36, 37, 40, 41)
        val logCodesSigSet = intArrayOf(8449, 9227)

        val length = ParseUtils.intFromBytes(numBytes[1], numBytes[0])

        // The smallest legal value of length is 6(null header and 0-length data)
        if (length <= 6) {
            seqObj.endOfFile = true
            return
        }
        // length of previous record
        getNextBytes(2, din, seqObj)
        val numBytes1 = seqObj.temp
        // deviceType of the record in the navigation header
        getNextBytes(1, din, seqObj)
        val numBytes2 = seqObj.temp!!
        val deviceType = ParseUtils.intFromByte(numBytes2[0])
        val remainder: ByteArray?
        var numBytes3: ByteArray? = null
        var numBytes4: ByteArray? = null
        var numBytes5: ByteArray? = null
        var logCode = 0

        if (deviceType == 26) {
            // Unit number of the record in the navigation header
            getNextBytes(1, din, seqObj)
            numBytes3 = seqObj.temp
            // Record Length in Qcomm record
            getNextBytes(2, din, seqObj)
            numBytes4 = seqObj.temp
            // LogCode in Qcomm record
            getNextBytes(2, din, seqObj)
            numBytes5 = seqObj.temp!!
            logCode = ParseUtils.intFromBytes(numBytes5[1], numBytes5[0])
            // The remaining records apart from the previous read 5 bytes
            getNextBytes(length - 10, din, seqObj)
            remainder = seqObj.temp
        } else {
            // The remaining records apart from the previous read 5 bytes
            getNextBytes(length - 5, din, seqObj)
            remainder = seqObj.temp
        }

        val byteBuffer = ByteBuffer.allocate(length)
        byteBuffer.put(numBytes)
        byteBuffer.put(numBytes1)
        byteBuffer.put(numBytes2)

        if (deviceType == 26) {
            byteBuffer.put(numBytes3)
            byteBuffer.put(numBytes4)
            byteBuffer.put(numBytes5)
        }

        byteBuffer.put(remainder)
        var logRecord: ByteArray? = byteBuffer.array()

        if (deviceTypesSet.contains(deviceType)) {
            val sigSeqRecord = logRecord
            val sigSeqRecMod = modifyLogRecordByteArrayToDropHeaderForHBFlex(sigSeqRecord!!)
            val currentLogCode = ParseUtils.intFromBytes(sigSeqRecMod[2], sigSeqRecMod[3])
            if (logCodesSigSet.contains(currentLogCode)) {
                val dmTimeStamp: java.sql.Timestamp = java.sql.Timestamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod))
                if (isValidDate(dmTimeStamp)) {
                    seqObj.lcTime = dmTimeStamp
                }
            }
        }

        if (deviceType == 26 && logCode == 0x11EB) {
            val mConstructIpPacket = IpConstruct(IpUtils.getInstance())
            val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)

            logRecord = ipPacket
        }

        logRecord?.let { saveRecord(it, seqObj, key, writer) }

    }

    private fun saveRecord(logRecord: ByteArray, seqObj: SequenceObj, key: IntWritable, writer: SequenceFile.Writer) {
        val value = BytesWritable()
        value.set(logRecord, 0, logRecord.size)
        key.set(seqObj.index)
        writer.append(key, value)
        seqObj.index++
    }

    private fun modifyLogRecordByteArrayToDropHeaderForHBFlex(data: ByteArray): ByteArray {
        return if (data.size > 5) {
            ParseUtils.getSubPayload(data, 6, data.size)
        } else {
            data
        }
    }

    private fun getNextBytes(num: Int, din: FSDataInputStream, seqObj: SequenceObj) {
        if (din.available() < num) {
            seqObj.temp = null
            seqObj.endOfFile = true
            return
        }
        try {
            val tempLocal = ByteArray(num) { 0 }
            din.readFully(tempLocal)
            val bufPtrLocal = seqObj.bufPtr
            seqObj.temp = tempLocal
            seqObj.bufPtr = bufPtrLocal + num
        } catch (e: Exception) {
            seqObj.temp = null
            seqObj.endOfFile = true
            logger.error("Exception in Thread in getNextBytes :  - $e", e)
        }
    }

    private fun isValidDate(timeStamp: Date): Boolean {
        val calendarInstance1 = Calendar.getInstance()
        calendarInstance1.time = Date()
        calendarInstance1.add(Calendar.DATE, 1)
        val currentDate = calendarInstance1.time
        val calendarInstance2 = Calendar.getInstance()
        calendarInstance2.time = timeStamp
        val logCodeYear = calendarInstance2.get(Calendar.YEAR)
        return (logCodeYear >= 2016) && (timeStamp <= currentDate)
    }
}
