import io.circe.generic.auto._
import io.circe.parser
import org.apache.spark.sql.SparkSession


case class WineMag (id: Option[Int]
                , country: Option[String]
                , points: Option[Int]
                , price: Option[Double]
                , title: Option[String]
                , variety: Option[String]
                , winery: Option[String])

object JsonReader extends App {

    // data file name
    val fileName = args(0)

    // create session
    val spark = SparkSession
      .builder()
      .appName("json_reader_evstafyev")
      .master("local[*]")
      .getOrCreate();


    // read file -> to WineMag class -> print
    val jsonFile = spark.sparkContext
            .textFile(fileName)
            .foreach(s => {
                parser.decode[WineMag](s) match {
                    case Right(wineObj) => println(wineObj)
                    case Left(ex) => println(s"some errror: ${ex}, ${s}")
                    //println(parse(s).getOrElse(Json.Null))
            }
        })
}
