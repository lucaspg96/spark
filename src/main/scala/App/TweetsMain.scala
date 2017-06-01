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
import Entities.Tweet


object TweetsMain {
  val stopWords = Source.fromFile("resources/ptbr.txt").getLines().filter { w => w.length()>0 }
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    val file = "resources/data/debate-tweets-002.tsv"
    //val file = "resources/data/tweets_sample.tsv"
    val conf = new SparkConf().setAppName("Twitters Analyzer Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("\rStarting")
    
    val tweets = sc.textFile(file).map(mapTweet).filter(validTweet)
    var tweetsCount = tweets.count()
    println(f"\r$tweetsCount tweets loaded")
    
    //Counting Reviews text words
    println("Starting word count")
    val words = tweets.flatMap(splitTweetText).filter(valid)

    val wordsFrequency = words
                        .map(word => (word,1))
                        .reduceByKey((a,b) => a+b)
                        .sortBy(tuple => tuple._2, ascending=false)
                        
    //val wordsFrequencyCount = wordsFrequency.count()
    //println(f"\r$wordsFrequencyCount words mapped")
    
    println("\n\nMost used words:") 
    for(k <- wordsFrequency.take(15)){
      println("\r"+k._1)
    }
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
//    println("\n\nMost used topics:")                      
//    for(k <- topicsFrequency.take(15)){
//      println("\r"+k._1)
//    }      
    //-----------------------------------------------
  }

  
  def mapTweet(line: String): Tweet = {
    val fields = line.split("\t")
    //println(fields.length)
    if(fields.length>=8){
      val text = fields(1).slice(1, fields(1).length()-1)
      //println(text)
      //println("Texto obtido")
      val date = fields(8).slice(1, fields(8).length()-1)
      //println("Data obtida")
      val tags = text.split(" ").filter { word => word.length()>0 && word(0)=="#" }
      
      new Tweet(text.toLowerCase(),date,tags)
    }
    else{
      println("Tweet incompleto")
      new Tweet("","",Array())
    }
    
  }
  
  def splitTweetText(t: Tweet): Array[String] = {
    t.text.replace(",","").replace(".","").replace("!","").replace("?","").replaceAll("[^\u0000-\uFFFF]", "").split(" ")
  }
  
  def validTweet(t: Tweet): Boolean = {
    t.text.length()>0
  }
  
  def valid(word: String): Boolean = {
    //println(word)
    word.size>4 && !stopWords.contains(word)
  }
  
  def getMonth(r:Review): String = {
    r.createdAt.split(" ")(0)
  }
  
  def getSentences(r: Review): Array[String] = {
    r.text.split(Array('.','?','!')).filter { s => s.length()>=3 }
  }
}
