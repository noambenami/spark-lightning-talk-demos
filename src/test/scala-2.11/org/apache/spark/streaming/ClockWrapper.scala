package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

/**
 * A simple hack to expose the package internal ManualClock for use by tests.
 */
object ClockWrapper {

  def manualClock(ssc: StreamingContext) = ssc.scheduler.clock.asInstanceOf[ManualClock]

}