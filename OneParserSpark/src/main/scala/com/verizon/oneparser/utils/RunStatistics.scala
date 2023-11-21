package com.verizon.oneparser.utils

import com.typesafe.scalalogging.LazyLogging

/**
 * RunStatistics
 * Logs code block elapsed time
 */
case object RunStatistics extends LazyLogging {
  def time[R](block: => R, msg: String = null): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    logger.warn(s"$msg Elapsed time: ${t1 - t0}ns")
    result
  }
}
