object SparkTestJob extends App with SparkTestContext {

  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.catalyst.ScalaReflection

  import ss.implicits._

  var assetsPath = ""

  try {
    assetsPath = sys.env("ASSETS_PATH")
  }
  catch {
    case e: Exception => println("ASSETS_PATH environment variable has not been provided")
  }


  case class Submission(
                         selftext: String,
                         title: String,
                         permalink: String,
                         id: String,
                         created_utc: BigInt,
                         author: String,
                         retrieved_on: BigInt,
                         score: BigInt,
                         subreddit_id: String,
                         subreddit: String,
                       )

  case class Comment(
                      author: String,
                      body: String,
                      score: BigInt,
                      subreddit_id: String,
                      subreddit: String,
                      id: String,
                      parent_id: String,
                      link_id: String,
                      retrieved_on: BigInt,
                      created_utc: BigInt,
                      permalink: String
                    )

  val commentSchema = ScalaReflection.schemaFor[Comment].dataType.asInstanceOf[StructType]

  val submissionSchema = ScalaReflection.schemaFor[Submission].dataType.asInstanceOf[StructType]


  val submissions = ss.read.schema(submissionSchema)
    .option("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
    .json(s"$assetsPath/RS_2018-02-01.xz").as[Submission]

  val comments = ss.read.schema(commentSchema)
    .option("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
    .json(s"$assetsPath/RC_2018-02-01.xz").as[Comment]


  submissions.createOrReplaceTempView("submissions")

  comments.createOrReplaceTempView("comments")

  ss.sql(
    """
      |SELECT * FROM submissions s
      | join comments c on replace(c.link_id,"t3_","") = s.id
      | where s.subreddit='worldnews' and s.id = '7uktsn'
      |
      |
      |""".stripMargin).show()


  ss.sql(
    """
      |SELECT author, COUNT(score), AVG(score)
      |FROM submissions s
      |WHERE subreddit='worldnews'
      |GROUP BY author
      |ORDER BY 2 DESC
      |LIMIT 10
      |
      |""".stripMargin).show()



  //3194211
  //println(comments.count())

  //387140
  //println(submissions.count())

  println("Processing complete")

}
