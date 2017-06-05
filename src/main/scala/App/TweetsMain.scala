import java.io.File
import java.io.Writer

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import Entities.Review
import Entities.Tweet
import Utils.SentimentAnalysisUtils.detectSentiment
import java.io.FileOutputStream
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.io.BufferedWriter


object TweetsMain {
  //val stopWords = Source.fromFile("resources/ptbr.txt").getLines().filter { w => w.length()>0 }
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    val file = "resources/data/debate-tweets-002.tsv"
    //val file = "resources/data/tweets_sample.tsv"
    val conf = new SparkConf().setAppName("Twitters Analyzer Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var writer:Writer = null
    
    println("\rStarting")
    val k = 20
    val tweets = sc.textFile(file).map(mapTweet).filter(validTweet)
    var tweetsCount = tweets.count()
    println(f"\r$tweetsCount tweets loaded")
    
//    //Counting Hashtags per time
//    println("\rStarting hashtags per time count")
//    val tagsPerTime = tweets.filter(tweet => tweet.tags.size>0).flatMap(getTagsByTime)
//    println(tagsPerTime.count())
//    val morningTags = tagsPerTime.filter(tag => tag._2=="MORNING")
//    val afternoonTags = tagsPerTime.filter(tag => tag._2=="AFTERNOON")
//    val nightTags = tagsPerTime.filter(tag => tag._2=="NIGHT")
//    
//    val morningTagsFrequency = morningTags
//                               .map(tag => (tag._1,1))
//                               .reduceByKey((a,b) => a+b)
//                               .sortBy(tuple => tuple._2, ascending=false)
//                               
//    val afternoonTagsFrequency = afternoonTags
//                               .map(tag => (tag._1,1))
//                               .reduceByKey((a,b) => a+b)
//                               .sortBy(tuple => tuple._2, ascending=false)
//    
//    val nightTagsFrequency = nightTags
//                               .map(tag => (tag._1,1))
//                               .reduceByKey((a,b) => a+b)
//                               .sortBy(tuple => tuple._2, ascending=false)
//
//    
//    //println("\n\n\rMost used tags on morning:") 
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/morningHashtags.txt")))
//    writer.write(morningTagsFrequency.take(k).mkString("\n"))
//    writer.close()
//    //println("\r"+morningTagsFrequency.take(k).mkString(","))
//    
//    //println("\n\n\rMost used tags on morning:")
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/afternoonHashtags.txt")))
//    writer.write(afternoonTagsFrequency.take(k).mkString("\n"))
//    writer.close()
//    //println("\r"+afternoonTagsFrequency.take(k).mkString(","))
//    
//    //println("\n\n\rMost used tags on morning:") 
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/nightHashtags.txt")))
//    writer.write(nightTagsFrequency.take(k).mkString("\n"))
//    writer.close()
//    //println("\r"+nightTagsFrequency.take(k).mkString(","))
//    
//    //-----------------------------------------------
//    
    //Days distribution
    val tweetsDates = tweets.map(tweet => (tweet.date,1))
    
    val tweetsDatesFrequency = tweetsDates.reduceByKey((a,b) => a+b).sortBy(tuple => tuple._1, ascending=false)

    //println("\n\nTime distribution:") 
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/tweetsDatesDistribution.txt")))
//    writer.write(tweetsDatesFrequency.collect().mkString("\n"))
//    writer.close()
    println("\r"+tweetsDatesFrequency.collect().mkString(","))
    //-----------------------------------------------
    
    for(date <- tweetsDatesFrequency.collect()){
      val day = date._1
      val tags = tweets.filter(tweet => tweet.date==day).flatMap {tweet => tweet.tags}
      val tagsFrequency = tags.map(tag => (tag,1)).reduceByKey((a,b) => a+b).sortBy(tuple => tuple._2, ascending=false)
      
      writer = new BufferedWriter(
                  new OutputStreamWriter(new FileOutputStream(f"results/tweets/$day-tweets.txt")))
      writer.write(tagsFrequency.take(k).mkString("\n"))
      writer.close()
      
      println("\r"+tagsFrequency.take(k).mkString(","))
    }
    
//    //Tweets per hour
//    val tweetsPerHour = tweetsDatesFrequency.map(tuple => (tuple._1,tuple._2/24))
//    
//    //println("\rTweets per hour:")
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/tweetsPerHour.txt")))
//    writer.write(tweetsPerHour.collect().mkString("\n"))
//    writer.close()
//    //println("\r"+tweetsPerHour.collect().mkString("\r", ",", ""))
//    //-----------------------------------------------
//    
//    //Dilma analysis
//    val dilmaTweets = tweets.filter { tweet => tweet.text.contains("dilma") }
//    val dilmaTweetsFrequency = dilmaTweets.flatMap { tweet => tweet.text.split(Array('.','!','?')) }
//                                          .map {sentence => sentence.replace("  ","").replace("   ","")}
//                                          .filter(sentence => sentence.length()>0 && !sentence.contains("http") && sentence.split(" ").size>2 && sentence.contains("dilma"))
//                                          .map { sentence => (sentence.replaceAll("\"",""),1) }
//                                          .reduceByKey((a,b) => a+b)
//                                          .sortBy(tuple => tuple._2, ascending=false)
//    
//    //println("\rDilma's main senteses:")
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/dilmaSentences.txt")))
//    writer.write(dilmaTweetsFrequency.take(k).mkString("\n"))
//    writer.close()
//    //println("\r"+dilmaTweetsFrequency.take(k).mkString("\r", ",", ""))
//    
//    //Aecio analysis
//    val aecioTweets = tweets.filter { tweet => tweet.text.contains("aecio") || tweet.text.contains("aécio") }
//    val aecioTweetsFrequency = aecioTweets.flatMap { tweet => tweet.text.split(Array('.','!','?')) }
//                                          .map {sentence => sentence.replace("  ","").replace("   ","")}
//                                          .filter(sentence => sentence.length()>0 && !sentence.contains("http") && sentence.split(" ").size>2 && (sentence.contains("aécio") || sentence.contains("aecio")))
//                                          .map { sentence => (sentence.replaceAll("\"",""),1) }
//                                          .reduceByKey((a,b) => a+b)
//                                          .sortBy(tuple => tuple._2, ascending=false)
//    
//    //println("\rAécio's main senteses:")
//    writer = new BufferedWriter(
//                  new OutputStreamWriter(new FileOutputStream("results/tweets/aecioSentences.txt")))
//    writer.write(aecioTweetsFrequency.take(k).mkString("\n"))
//    writer.close()
//    //println("\r"+aecioTweetsFrequency.take(k).mkString("\r", ",", ""))
    
    
    println("Done")
  }

  
  def mapTweet(line: String): Tweet = {
    val fields = line.split("\t")
    //println(fields.length)
    if(fields.length>=8){
      val text = fields(1).slice(1, fields(1).length()-1).replace("(","").replace(")","")
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
