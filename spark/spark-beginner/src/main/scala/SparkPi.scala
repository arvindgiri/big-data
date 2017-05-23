import org.apache.spark.{SparkConf, SparkContext};
/**
  * Reference : http://learningapachespark.blogspot.in/2015/03/12-how-to-run-spark-with-eclipse-and.html
  *
  */
object SparkPi {
  def main(args: Array[String]): Unit = {
    print( "hello world")
    val sparkConf = new SparkConf().setAppName("SparkPi").setMaster("local");
    val spark = new SparkContext(sparkConf)
    val slices = if(args.length >  0) args(0).toInt else 2
    val n = math.min(100000L * slices,Int.MaxValue).toInt
    val mapFunction = (x:Int) => x-1;
    val count = spark.parallelize(1 until n,slices).map(mapFunction).reduce((x:Int,y:Int) => 1)
    println("Count = " + count)
    spark.stop
  }
}
