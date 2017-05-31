import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.{Logger, Level} 
import Entities.Review
import org.json4s.jackson.JsonMethods.parse
import org.json4s._

import edu.stanford.nlp.sentiment._
import Utils.SentimentAnalysisUtils.{detectSentiment,SENTIMENT_TYPE}

import scala.io.Source
import Utils.SentimentAnalysisUtils.SENTIMENT_TYPE


object ReviewsMain {
  val stopWords = Source.fromFile("resources/en.txt").getLines()
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    val file = "resources/data/eiffel-tower-reviews.json"
    val conf = new SparkConf().setAppName("Reviews Analyzer Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("\rStarting")
    
    val reviews = sc.textFile(file).map(mapReview).filter(hasText)
    var reviewsCount = reviews.count()
    println(f"\r$reviewsCount reviews loaded")
    
    //Counting Reviews text words
//    val words = reviews.flatMap(splitReviewText).filter(valid)
//
//    val wordsFrequency = words
//                        .map(word => (word,1))
//                        .reduceByKey((a,b) => a+b)
//                        .sortBy(tuple => tuple._2, ascending=false)
//                        
//    val wordsFrequencyCount = wordsFrequency.count()
//    println(f"\r$wordsFrequencyCount words mapped")
//    
//    println("\n\nMost used words:") 
//    for(k <- wordsFrequency.take(15)){
//      println("\r"+k._1)
//    }
    //-----------------------------------------------
    
    //Time distribution
//    val months = reviews.map(review => (getMonth(review),1))
//    
//    val monthsFrequency = months.reduceByKey((a,b) => a+b).sortBy(tuple => tuple._2, ascending=false)
//
//    println("\n\nTime distribution:") 
//    for(m <- monthsFrequency.collect()){
//      println("\r"+m._1+" -> "+m._2)
//    }
    //-----------------------------------------------
    
    //Main topics
//    val topics = reviews.flatMap(splitReviewTitle).filter(valid)
//    
//    val topicsFrequency = topics
//                        .map(topic => (topic,1))
//                        .reduceByKey((a,b) => a+b)
//                        .sortBy(tuple => tuple._2, ascending=false)
//    
//    println("\r\n\nMost used topics:")                      
//    for(k <- topicsFrequency.take(15)){
//      println("\r"+k._1)
//    }      
    //-----------------------------------------------
    
    //Sentiment Analisis
//    val sentiments = reviews.map(getSentiment)
//    val sentimentsFrequency = sentiments.reduceByKey((a,b) => a+b)
//    
//    println("\rSentiment Analysis:")
//    for(k <- sentimentsFrequency.collect()){
//      println("\r"+k._1+" -> "+k._2)
//    }
    
    //Sentence Analisis
    val sentences = reviews.flatMap(getSentences)
    val sentencesCount = sentences.count()
    println(f"\r$sentencesCount sentences to analyse...")
//    
//    val sentencesFrequency = sentences
//                             .map(sentence => (sentence,1))
//                             .reduceByKey((a,b) => a+b)
//                             .sortBy(tuple => tuple._2, ascending=false)
//    
//    println("\r\n\nMost used sentences:")
//    for(k <- sentencesFrequency.take(15)){
//      println("\r"+k._1+" -> "+k._2)
//    }
    
    //Sentence Semantic Analisis
    
    val sentencesSemanticFrequency = sentences
                             .map(sentence => (getSentiment(sentence)))
                             .reduceByKey((a,b) => a+b)
                             .sortBy(tuple => tuple._2, ascending=false)
    
    println("\r\n\nSentences' semantics:")
    for(k <- sentencesSemanticFrequency.collect()){
      println("\r"+k._1+" -> "+k._2)
    }
  }

  
  def mapReview(line: String): Review = {
    implicit val formats = DefaultFormats
    val json = parse(line)
    json.camelizeKeys.extract[Review]
  }
  
  def hasText(r: Review): Boolean = {
    try{
      r.text.toLowerCase()
      true
    }
    catch {
      case npe: NullPointerException => return false      
    }
  }
  
  def splitReviewText(r: Review): Array[String] = {
    r.text.toLowerCase().split(" ")
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
  
  def getSentiment(r: Review): (SENTIMENT_TYPE,Int) = {
    (detectSentiment(r.text),1)
  }
  
  def getSentiment(s: String): (SENTIMENT_TYPE,Int) = {
    (detectSentiment(s),1)
  }
  
  def getSentences(r: Review): Array[String] = {
    r.text.split(Array('.','?','!')).filter { s => s.length()>=3 }
  }
}
