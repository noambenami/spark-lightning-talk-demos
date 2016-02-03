package com.mulesoft.meetup.lightning.spark.test.sensors

import scala.util.Random

/**
 * Utility classes for generating various objects
 *
 * TODO: Make use of ScalaCheck Gen https://github.com/rickynils/scalacheck/wiki/User-Guide
 */
object RandomUtils {

  /**
   * @param length Length of the string
   * @return Random string of given length
   */
  def str(length: Int): String = str(length, length)

  /**
   * @param maxStrLen Max length of string to generate
   * @param minStrLen Min length of string to generate
   * @return Random string of a random length between
   *         maxStrLen and minStrLen, inclusive.
   */
  def str(minStrLen: Int, maxStrLen: Int): String = {
    assert(minStrLen <= maxStrLen)
    val spread =  maxStrLen - minStrLen
    val length = if (spread == 0) minStrLen else Random.nextInt(spread) + minStrLen; // E.g. rnd(10 - 3) + 3
    Random
      .alphanumeric
      .take(length)
      .mkString
  }

}
