package com.verizon.oneparser.writesequencetohdfs

import com.vzwdt.parseutils.ParseUtils
import java.text.DateFormat
import java.text.SimpleDateFormat
import kotlin.math.abs

object OPutil {
    fun calculateHbflexDmTimestamp(data: ByteArray): Long {
        var localData = switchEndianOnOffsetAndLength(data, 32 / 8, 4)
        val timeSeconds = ParseUtils.GetIntFromBitArray(localData, 32, 32, false)

        localData = switchEndianOnOffsetAndLength(data, 64 / 8, 4)
        val timeNanos = ParseUtils.GetIntFromBitArray(localData, 64, 32, false)

        var nanosInt = abs(timeNanos)
        while (nanosInt >= 1000) nanosInt /= 10
        var timeNanosStr = timeNanos.toString()
        //see if you should round up from the fourth digit of the nano seconds
        if (timeNanosStr.startsWith("-")) {
            timeNanosStr = "000"
        } else {
            if (timeNanos.toString().length > 3) {
                if (Integer.parseInt(timeNanos.toString().substring(3, 4)) >= 5) {
                    nanosInt += 1
                    timeNanosStr = nanosInt.toString()
                }
            } else {
                while (timeNanosStr.length < 3) timeNanosStr += "0"
            }
        }

        val df: DateFormat = SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS")
        df.timeZone = java.util.TimeZone.getTimeZone("UTC")

        // val timeMillSecs = new java.sql.Date(new BigInteger((timeSeconds*1000).toString, 16).longValue() + 18)
        val timeMillSecs = java.sql.Date((timeSeconds + 18) * 1000L)
        timeNanosStr = if (timeNanosStr.length >= 3) timeNanosStr.substring(0, 3) else timeNanosStr
        return df.parse(df.format(timeMillSecs) + timeNanosStr).time
    }

    private fun switchEndianOnOffsetAndLength(data: ByteArray, index: Int, length: Int): ByteArray {
        val swapdata = ByteArray(length)
        System.arraycopy(data, index, swapdata, 0, length)
        for (i in 0 until length) {
            data[index + i] = swapdata[swapdata.size - i - 1]
        }
        return data
    }
}
