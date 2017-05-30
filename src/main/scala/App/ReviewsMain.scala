import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.{Logger, Level} 
import Entities.Review
import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import scala.io.Source


object ReviewsMain {
  val stopWords = Source.fromFile("resources/en.txt").getLines()
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    val file = "resources/data/eiffel-tower-reviews.json"
    val conf = new SparkConf().setAppName("Reviews Analyzer Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("\rStarting")
    val reviews = sc.textFile(file).map(mapReview)
    var reviewsCount = reviews.count()
    println(f"\r$reviewsCount reviews loaded")
    
    //Counting Reviews text words
    val words = reviews.flatMap(splitReviewText).filter(valid)

    val wordsFrequency = words
                        .map(word => (word,1))
                        .reduceByKey((a,b) => a+b)
                        .sortBy(tuple => tuple._2, ascending=false)
                        
    val wordsFrequencyCount = wordsFrequency.count()
    //println(f"\r$wordsFrequencyCount words mapped")
    
    println("\n\nMost used words:") 
    for(k <- wordsFrequency.take(15)){
      println("\r"+k._1)
    }
    //-----------------------------------------------
    
    //Time distribution
    val months = reviews.map(review => (getMonth(review),1))
    
    val monthsFrequency = months.reduceByKey((a,b) => a+b).sortBy(tuple => tuple._2, ascending=false)

    println("\n\nTime distribution:") 
    for(m <- monthsFrequency.collect()){
      println("\r"+m._1+" -> "+m._2)
    }
    //-----------------------------------------------
    
    //Main topics
    val topics = reviews.flatMap(splitReviewTitle).filter(valid)
    
    val topicsFrequency = topics
                        .map(topic => (topic,1))
                        .reduceByKey((a,b) => a+b)
                        .sortBy(tuple => tuple._2, ascending=false)
    
    println("\n\nMost used topics:")                      
    for(k <- topicsFrequency.take(15)){
      println("\r"+k._1)
    }                   
  }

  
  def mapReview(line: String): Review = {
    implicit val formats = DefaultFormats
    val json = parse(line)
    json.camelizeKeys.extract[Review]
  }
  
  def splitReviewText(r: Review): Array[String] = {
    try{
      r.text.toLowerCase().split(" ")
    }
    catch {
      case npe: NullPointerException => return new Array[String](0)       
    }
  }
  
  def splitReviewTitle(r: Review): Array[String]={
    r.title.toLowerCase().split(" ")
  }
  
  def valid(word: String): Boolean = {
    !stopWords.contains(word) && word.size>3
  }
  
  def getMonth(r:Review): String = {
    r.createdAt.split(" ")(0)
  }
}
