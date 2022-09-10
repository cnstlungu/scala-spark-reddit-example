import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkTestContext {

  private val master = "local[*]"
  private val appName = "testing"
  private val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = ss.sparkContext
  val sqlContext: SQLContext = ss.sqlContext

}
