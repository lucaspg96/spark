
package App

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Logger, Level}
 
object SampleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    //val txtFile = "file:///Users/lucasperes/Documents/Repositories/spark/src/main/scala/SampleApp.scala"
    val txtFile = "hdfs://localhost:54310/hdfs/SampleApp.scala"
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("Ambiente iniciado")
    val txtFileLines = sc.textFile(txtFile)
    println("Arquivo guardado")
    val numAs = txtFileLines.filter(line => line.contains("val")).count()
    println("Contagem feita")
    println("Lines with val: %s".format(numAs))
  }
}
