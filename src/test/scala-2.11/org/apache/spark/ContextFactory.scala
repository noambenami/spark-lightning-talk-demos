package org.apache.spark

import org.apache.log4j.{Level, Logger}

/**
 * Responsible for creating a local spark streaming context which tests can use to
 * test spark streaming jobs.
 */
object ContextFactory {

  private val master  = "local[4]"
  private val appName = "demo"

  /**
   * @param settings Extra context configuration settings
   * @param sparkUI Whether the spark ui should be enabled. Disabled by default to speed up tests.
   *
   * @return A local spark streaming context and its manual clock
   */
  def build(settings: Map[String, String] = null, sparkUI: Boolean = false): SparkContext = {

    // First set up a spark context
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.ui.enabled", sparkUI.toString)

    // For demo purposes, take down the log level to reduce noise
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Apply extra settings
    settings.keys.foreach(key => conf.set(key, settings(key)))

    new SparkContext(conf)
  }

}
