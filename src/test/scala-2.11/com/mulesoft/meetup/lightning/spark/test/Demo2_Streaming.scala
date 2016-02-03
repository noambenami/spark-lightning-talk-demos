package com.mulesoft.meetup.lightning.spark.test

import com.mulesoft.meetup.lightning.spark.test.sensors.SensorSignals
import org.apache.spark.streaming.{Minutes, TestInputStream, StreamingContextFactory}
import org.scalatest._
import org.scalatest.concurrent.Eventually

class Demo2_Streaming extends FlatSpec with Matchers with Eventually {

  "Spark" should "generate alerts for any sensor that exceeds 10v average for > 5 consecutive minutes" in {

    val maxSafeVoltage = 14.0

    val signalGenerator = new SensorSignals(1)
    val signalBatches   = signalGenerator.makeBatches(20, 20)
    val (ssc, clock)    = StreamingContextFactory.build()
    val dstream         = new TestInputStream(ssc, signalBatches, 4)

    // map and reduce the signals so that for every minute, we get:
    // signalId: (sum of voltages, count of signals)
    // From this we can derive averages over 5 minutes.
    val rdds = dstream
      .map(signal => (signal._1, (signal._2, 1)))
      .reduceByKeyAndWindow(
        (t1: (Float, Int), t2: (Float, Int)) => { (t1._1 + t2._1, t1._2 + t2._2) },
        Minutes(1), Minutes(1))
      .groupByKeyAndWindow(Minutes(5), Minutes(1))
      .mapValues(data => {
        // First, we calculate the average for each minute
        data.map(signals => signals._1 / signals._2)
      })

    // Now, every time a new RDD is generated, let's print out
    rdds.foreachRDD(rdd => {
      rdd.collect().foreach(tuple => {
        // If all minute averages are above our safe threshold, we trigger!
        val triggered = tuple._2.forall(_ > maxSafeVoltage)

        println("Sensor " + tuple._1 + ": " + tuple._2 + " - Triggered: " + triggered)
      })
    })

    ssc.start()
    clock.advance(1000 * 60 * 20)

    ssc.awaitTermination()
  }

}
