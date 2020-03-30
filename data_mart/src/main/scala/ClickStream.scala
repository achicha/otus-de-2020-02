import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class CategoryStats(GENDER_CD: String, category: String, count: Long)

object ClickStream extends App {
    // create session
    val spark = SparkSession
        .builder()
        .appName("ClickStream")
        .master("local[*]")
        .getOrCreate();

    import spark.implicits._

    // data files
    val dirName = args(0)

    val userEvents = spark.read.json(s"$dirName/omni_clickstream.json")
    val products = spark.read.json(s"$dirName/products.json")
    val users = spark.read.json(s"$dirName/users.json")

    // run from Dataframe
    userEvents
        .join(products, userEvents("url") === products("url"))
        .join(users, rtrim(ltrim(userEvents("swid"), "{"), "}") === users("SWID"))
        //.join(users, userEvents("swid") === concat(lit("{"), users("SWID"), lit("}")))
        .where($"GENDER_CD" =!= "U")
        .groupBy("GENDER_CD", "category")
        .count()
        // using DataFrame API
        .withColumn(
            "rn",
            row_number() over Window.partitionBy($"GENDER_CD").orderBy($"count".desc)
        )
        .where($"rn" < 4)
        // using DataSet API
//        .as[CategoryStats]
//        .groupByKey(x => x.GENDER_CD)
//        .flatMapGroups {
//            case (genderKey, elements) => elements.toList.sortBy(x => -x.count).take(3)
//        }
        .show(5)
}
