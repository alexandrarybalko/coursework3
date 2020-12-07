package coursework

import org.apache.spark.{SparkConf, SparkContext}

object WordsCounter {
  def main(args: Array[String]): Unit = {
    val alphabet = List('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
      'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    //create SparkConf and SparkContext objects to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Words Counter")
    val sc = new SparkContext(conf)
    //create RDD
    val rdd = sc.textFile("harrypotter.txt", 4)
    val count = rdd.flatMap(line => line.split(" "))
      .map(word => (word.toLowerCase().filter(x => alphabet contains x), 1)).reduceByKey(_+_)
    count.foreach(f => println(f))
  }
}