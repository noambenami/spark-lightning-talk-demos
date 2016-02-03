package com.mulesoft.meetup.lightning.spark.test.sensors

import scala.util.Random

/**
 * Generates a stream/batches of fake sensor signals, which are tuples of
 * sensor id and a random number representing, say, a voltage. The random
 * numbers range from 5 to 15.
 */
class SensorSignals(numSensors: Int = 10) {

  private val sensorIds = new StringPool("sensorIds", numSensors, () => {
    RandomUtils.str(5)
  })

  /**
   * @return Batch of batches of signals. Ideal for testing spark streaming
   */
  def makeBatches(batches: Int,
                  signalsPerBatch: Int): Seq[Seq[(String, Float)]] = {

    (1 to batches).map(
      i => (1 to signalsPerBatch).map(i => meep)
    )
  }

  /**
   * @return A new sensor signal
   */
  private def meep: (String, Float) = {

    (sensorIds.random, Random.nextFloat() * 10 + 5)
  }

}
