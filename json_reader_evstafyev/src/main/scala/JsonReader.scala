import org.apache.spark.sql.SparkSession
import io.circe.generic.auto._
import io.circe.syntax._
import cats.syntax.either._
import io.circe._
import io.circe.generic.extras.{Configuration, ConfiguredJsonCodec}
import io.circe.parser._

case class WineMag (id: Option[Int]
                , country: Option[String]
                , points: Option[Int]
                , price: Option[Double]
                , title: Option[String]
                , variety: Option[String]
                , winery: Option[String]) {
    def print(): Unit = {
        //println(this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator.to))
        println(this)
    }
}

object JsonReader extends App{

    // create session
    val spark = SparkSession
      .builder()
      .appName("json_reader_evstafyev")
      .master("local[*]")
      .getOrCreate();



    // read file
    val rawDataFile = spark.sparkContext.textFile("winemag-data.json")
        rawDataFile.foreach(s => {
            parser.decode[WineMag](s) match {
                case Right(wineObj) => wineObj.print()
                case Left(ex) => println(s"some errror: ${ex}, ${s}")
                //println(parse(s).getOrElse(Json.Null))
            }
    })
    }
