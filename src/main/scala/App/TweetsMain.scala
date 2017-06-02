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
    
    //Counting Hashtags per time
    println("\rStarting hashtags per time count")
    val tagsPerTime = tweets.filter(tweet => tweet.tags.size>0).flatMap(getTagsByTime)
    println(tagsPerTime.count())
    val morningTags = tagsPerTime.filter(tag => tag._2=="MORNING")
    val afternoonTags = tagsPerTime.filter(tag => tag._2=="AFTERNOON")
    val nightTags = tagsPerTime.filter(tag => tag._2=="NIGHT")
    
    val morningTagsFrequency = morningTags
                               .map(tag => (tag._1,1))
                               .reduceByKey((a,b) => a+b)
                               .sortBy(tuple => tuple._2, ascending=false)
                               
    val afternoonTagsFrequency = afternoonTags
                               .map(tag => (tag._1,1))
                               .reduceByKey((a,b) => a+b)
                               .sortBy(tuple => tuple._2, ascending=false)
    
    val nightTagsFrequency = nightTags
                               .map(tag => (tag._1,1))
                               .reduceByKey((a,b) => a+b)
                               .sortBy(tuple => tuple._2, ascending=false)
    //val wordsFrequencyCount = wordsFrequency.count()
    //println(f"\r$wordsFrequencyCount words mapped")
    
    println("\n\n\rMost used tags on morning:") 
    println("\r"+morningTagsFrequency.take(1).mkString(","))
    
    println("\n\n\rMost used tags on morning:") 
    println("\r"+afternoonTagsFrequency.take(15).mkString(","))
//    
    println("\n\n\rMost used tags on morning:") 
    println("\r"+nightTagsFrequency.take(15).mkString(","))
//    
    //-----------------------------------------------
    
    //Days distribution
    val tweetsDates = tweets.map(tweet => (tweet.date,1))
    
    val tweetsDatesFrequency = tweetsDates.reduceByKey((a,b) => a+b).sortBy(tuple => tuple._1, ascending=false)

    println("\n\nTime distribution:") 
    println(tweetsDatesFrequency.collect().mkString(","))
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
      val time = fields(7).slice(1, fields(7).length()-1).split(" ")(3)
      val date = fields(8).slice(1, fields(8).length()-1)
      //println("Data obtida")
      //println(text)
      val tags = text.split(" ").filter { word => word.length()>0 && word.charAt(0)=='#' }
      
      new Tweet(text.toLowerCase(),date,time,tags)
    }
    else{
      println("Tweet incompleto")
      new Tweet("","","",Array())
    }
    
  }
  
  def getTagsByTime(t: Tweet): Array[(String,String)] = {
    //t.text.replace(",","").replace(".","").replace("!","").replace("?","").replaceAll("[^\u0000-\uFFFF]", "").split(" ")
    val time = getTime(t.time)
    val tags = t.tags.map { tag => (tag,time) }
    //println(tags.mkString(","))
    tags
  }
  
  def validTweet(t: Tweet): Boolean = {
    t.text.length()>0
  }
  
  def valid(word: String): Boolean = {
    //println(word)
    word.size>4 && !stopWords.contains(word)
  }
  
  def getTime(t: String): String = {
    var time ="NIGHT"
    
    if (t>="05:00:00" && t<"12:00:00") {
      time = "MORNING"
    }
    else if(t>="12:00:00" && t<"18:00:00"){
      time = "AFTERNOON"
    }
    
    time
  }
  
  def getSentences(r: Review): Array[String] = {
    r.text.split(Array('.','?','!')).filter { s => s.length()>=3 }
  }
}
