package com.mulesoft.meetup.lightning.spark.test.sensors

import scala.util.Random

/**
 * Generates a pool of strings from which random values can be pulled
 */
class StringPool(poolName: String, size: Int, gen: () => String) extends Iterable[String] {

  assert(poolName != null && poolName.length > 0)
  assert(size > 0)

  private var strings = Vector[String]()

  // pre-generate all the strings:
  for (i <- 1 to size) {
    strings = strings :+ gen()
  }

  def name: String = poolName

  /**
   * @return Iterator over all members of the pool
   */
  def iterator: Iterator[String] = strings.iterator

  /**
   * @return Random string from the pool
   */
  def random: String = {
    strings(Random.nextInt(size))
  }

}
