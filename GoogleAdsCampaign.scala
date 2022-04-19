import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.SparkContext

object GoogleAdsCampaign extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "GoogleAdsCampaign");
  
  val initial_rdd = sc.textFile("C:\\Users\\Akhil\\Desktop\\Datasets\\bigdatacampaigndata.csv");
  
  val mappedInput = initial_rdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  
  val words = mappedInput.flatMapValues(x => x.split(" "))
  
  val finalMapped = words.map(x => (x._2.toLowerCase(),x._1));
  
  val total = finalMapped.reduceByKey((x,y)=> x + y);
  
  val sorted = total.sortBy(x => x._2,false);
  
  sorted.take(20).foreach(println);

}