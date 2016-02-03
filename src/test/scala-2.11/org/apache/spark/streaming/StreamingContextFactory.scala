package org.apache.spark.streaming

import org.apache.spark.util.ManualClock
import org.apache.spark.{ContextFactory, SparkConf, SparkContext}

/**
 * Responsible for creating a local spark streaming context which tests can use to
 * test spark streaming jobs.
 */
object StreamingContextFactory {

  /**
   * @param checkpointDir Checkpoint directory path. If null, checkpointing will be disabled.
   * @param sparkUI Whether the spark ui should be enabled. Disabled by default to speed up tests.
   *
   * @return A local spark streaming context and its manual clock
   */
  def build(checkpointDir: String = null,
            sparkUI: Boolean = false): (StreamingContext, ManualClock) = {

    val sc = ContextFactory.build(
      Map("spark.streaming.clock" -> "org.apache.spark.streaming.util.ManualClock")
    )

    // Then, build a streaming context over that context
    val batchDuration = Minutes(1)

    val ssc = new StreamingContext(sc, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir.toString)
    }

    (ssc, ClockWrapper.manualClock(ssc))

  }

}
