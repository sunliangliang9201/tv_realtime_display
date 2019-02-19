import org.apache.spark.sql.SparkSession

/**
  * 可以读流 也可以读batch
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
object SDF_kafkaApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("1").master("local[2]").getOrCreate()
    import spark.implicits._
//    val sdf = spark.readStream.format("kafka")
//                                .option("kafka.bootstrap.servers", "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092")
//                                  .option("subscribe", "bf.bftv.tv_real_time").load()
//    val query = sdf.writeStream.outputMode("console").start()
//    query.awaitTermination()
    val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092")
                                          .option("subscribe", "bf.bftv.tv_real_time")
                                          .option("startingOffsets", "earliest")
                                          .option("endingOffsets", "latest").load()
    val query = df.write.format("condole").save()

  }
}
