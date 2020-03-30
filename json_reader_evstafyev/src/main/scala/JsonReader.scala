import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods.{parse}
import org.json4s.DefaultFormats


case class WineMag (id: Option[Int]
                , country: Option[String]
                , points: Option[Int]
                , price: Option[Double]
                , title: Option[String]
                , variety: Option[String]
                , winery: Option[String])

object JsonReader extends App {

    // data file name
    var fileName = ""
    if (args.length == 0) {
        fileName = "data.json"
    }
    else {
        fileName = args(0)
    }

    // create session
    val spark = SparkSession
      .builder()
      .appName("json_reader_evstafyev")
      .master("local[*]")
      .getOrCreate();

    implicit val formats = DefaultFormats

    // read file -> to WineMag class -> print
    val jsonFile = spark.sparkContext
      .textFile(fileName)
      .map(s => parse(s).extract[WineMag])
      .foreach(println)
}
