import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by arvind on 21/4/17.
  *
  * References: https://aws.amazon.com/blogs/big-data/analyze-your-data-on-amazon-dynamodb-with-apache-spark/
  * https://github.com/awslabs/emr-dynamodb-connector
  */

object DynamoDB {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkPi").setMaster("local");
    val sc = new SparkContext(sparkConf)

    var jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("dynamodb.input.tableName", "mps_gm_permissions-dev")
    jobConf.set("dynamodb.region", "us-east-1")
    //Pick it from environment variable etc
    jobConf.set("dynamodb.awsAccessKeyId", "XXX")
    jobConf.set("dynamodb.awsSecretAccessKey", "YYY")


    jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

    var permissions = sc.hadoopRDD(jobConf, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])
    println("Total records:" + permissions.count())
  }
}
