import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.{Logger, Level} 
import Entities.Review
import org.json4s.jackson.JsonMethods.parse
import org.json4s._


object ReviewsMain {
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    val file = "resources/eiffel-tower-reviews.json"
    val conf = new SparkConf().setAppName("Reviews Analyzer Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("Starting")
    val reviews = sc.textFile(file).map(mapReview)
  }
  
  def mapReview(line: String): Review = {
    implicit val formats = DefaultFormats
    val json = parse(line)
    json.camelizeKeys.extract[Review]
  }
}
