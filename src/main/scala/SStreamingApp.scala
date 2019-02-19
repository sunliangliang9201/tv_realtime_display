import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.StructType

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
object SStreamingApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("1").master("local[2]").getOrCreate()
    import spark.implicits._
//    val lines = spark.readStream.format("socket").option("host", "150.138.123.194").option("port", 9999).load()
//    val listener = spark.streams.addListener(new StreamingQueryListener {
//      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
//        println("Query started: " + event.id + "," + event.name)
//    }
//
//      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
//        println("Query terminated: " + event.progress)
//      }
//
//      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
//        println("Query terminated: " + event.id + "," + event.exception)
//      }
//    })
//    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
//    println(lines.isStreaming)
//    lines.printSchema()
//    val words = lines.as[String].flatMap(_.split(" "))
//    val wordCount = words.groupBy("value").count()
//    val query = lines.writeStream.format("json").option("path", "e:\\test").option("checkpointLocation", "e:\\test\\cp").start()
//    val query = wordCount.writeStream.format("console").trigger(Trigger.ProcessingTime("2 second")).outputMode("complete").start()
//    println(query.name)
//    println(query.id)
//    println(query.runId)
//    println(query.lastProgress)
//    println(spark.streams.active)
    val sdf = spark.readStream.format("rate").option("rowsPersecond", "10").load()
    val query = sdf.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
//    val schema = new StructType().add("name", "string").add("age", "integer")
//    val text = spark.readStream.option("sep", ",").schema(schema).csv("E:\\test")
//    val query = text.writeStream.outputMode("update").format("console").start()
//    query.awaitTermination()
  }
}