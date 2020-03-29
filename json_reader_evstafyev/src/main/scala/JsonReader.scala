import org.apache.spark.{SparkConf, SparkContext}
//import scala.reflect.io.Path
//import org.json4s._
//import org.json4s.jackson.JsonMethods.{parse}

object JsonReader extends App {

    // default case class
    case class Line(id: Int, country: String, points: Int, price: Double,
                    title: String, variety: String, winery: String)
    // default spark
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("my_app")
    val sc = new SparkContext(conf)

    // read file
    val rawDataFile = sc.textFile("data.json")
    rawDataFile.foreach(f => { println(f)})
}