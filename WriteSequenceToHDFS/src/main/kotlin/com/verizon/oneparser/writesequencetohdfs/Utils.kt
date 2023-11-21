package com.verizon.oneparser.writesequencetohdfs

import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun getLogger(forClass: Class<*>): Logger = LoggerFactory.getLogger(forClass)
fun String.isNumeric(): Boolean {
    return this.toDoubleOrNull() != null
}
