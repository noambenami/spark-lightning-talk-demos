package com.mulesoft.meetup.lightning.spark.test

import java.util.regex.Pattern

import org.apache.spark.ContextFactory
import org.scalatest._
import org.scalatest.concurrent.Eventually

class Demo1_WordCount extends FlatSpec with Matchers with Eventually {

  "Spark" should "compute word counts for all of Shakespeare's works with ease" in {

    val sc = ContextFactory.build()
    val lines     = sc.textFile("src/test/resources/shakespeare.txt")
    val pattern   = Pattern.compile("[,.?!;]")
    val stopWords = Array(
      "you", "in", "i", "my", "a", "of", "to", "and", "the", " ",
      "have", "for", "be", "your", "his", "with", "not", "that", "is", ""
    )

    // Note the use of a closure here, in a distributed program!
    // We can reference pattern inside the code and spark will
    // serialize the closure to all executors
    val wordCountsRDD = lines.flatMap(
      line => pattern.matcher(line).replaceAll("").split("\\W+")
    )
      .filter(word => !stopWords.contains(word.toLowerCase))
      .map(word => (word, 1))
      .reduceByKey((count1, count2) => count1 + count2)
      .sortBy(tuple => tuple._2)

    // Print the query plan
    println(wordCountsRDD.toDebugString)

    // Execute the action
    val wordCounts = wordCountsRDD.collect()

    // Print the results of the computation
    val wordCount = wordCounts.length
    val top10     = wordCounts.takeRight(10).reverse.map(tuple => tuple._1 + ": " + tuple._2)
    val bottom100 = wordCounts.take(100).map(tuple => tuple._1).grouped(10)

    println("Found " + wordCount + " distinct words")
    println("Top 10 words: " + top10.mkString(", "))
    for (group <- bottom100) {
      println("Bottom 100 words: " + group.mkString(", "))
    }
  }

}
