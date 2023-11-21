package com.verizon.oneparser.writesequencetohdfs

import java.util.*

data class SequenceObj(
        var index: Int = 0,
        var bufPtr: Int = 0,
        var endOfFile:Boolean = false,
        var temp: ByteArray? = byteArrayOf(),
        var lcTime: Date? = null
)
