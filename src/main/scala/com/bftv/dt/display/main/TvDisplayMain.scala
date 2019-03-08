package com.bftv.dt.display.main

import java.sql.Timestamp

import com.bftv.dt.display.Sinks.{MysqlSink, MysqlSink2}
import com.bftv.dt.display.formator.LogFormator
import com.bftv.dt.display.storage.mysql.MysqlDao
import com.bftv.dt.display.utils.Constant
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

/**
  * 主程序入口：包含获取&更新所有配置、获取外部匹配数据如ip解析、创建structured streaming+kafka(SDF)、调用具体的逻辑处理函数s.
  *
  * @author sunliangliang 2019-02-22 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object TvDisplayMain {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //从传入参数获取key以及是否启用根据offset来获取kafka数据
    //val ssKey = args(0)
    //val flag = Integer.valueOf(args(1))
    val ssKey = "TvDisplay"
    val flag = 0
    val ssKeyConf = MysqlDao.getSSConf(ssKey)
    if (null == ssKey){
      logger.error("No ssstreaming config im mysql ...")
      System.exit(1)
    }
    logger.info("Success load the sstreaming config from mysql !")

    //val spark = SparkSession.builder().appName(ssKeyConf.appName).config("spark.driver.cores", ssKeyConf.driverCores).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val spark  = SparkSession.builder().master("local[*]").appName(ssKeyConf.appName).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val sc = spark.sparkContext
    val bcConf = sc.broadcast(ssKeyConf)
    val bcIP = sc.broadcast(sc.textFile("e:/ip_area_isp.txt").filter(line => {
      null != line.stripMargin && "" != line.stripMargin
    }).map(line => {
      (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2), line.split("\t")(3), line.split("\t")(4).toLong, line.split("\t")(5).toLong)
    }).collect())
    val logFormator = Class.forName(Constant.FORMATOR_PACACKE_PREFIX + ssKeyConf.formator).newInstance().asInstanceOf[LogFormator]
    //引入隐式变换
    import spark.implicits._
    var sdf: DataFrame = null
    if (flag == 0){
      sdf = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bcConf.value.brolerList)
        .option("subscribe", bcConf.value.topics)
        .option("group.id", bcConf.value.groupID)
        //.option("startingoffsets", "earliest")
        //.option("maxOffsetsPerTrigger", "50")
        .load()
    }else{
      //后续添加根据offset消费的功能，目的是at least once消费
    }
    val sdf2 = sdf.selectExpr("CAST(value AS STRING)", "CAST(partition AS INT)", "CAST(offset AS INT)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Int, Int, Timestamp)]
    //创建schema与df中字段匹配
    val fields = bcConf.value.fields.map(field => {
      var structField = StructField(field, StringType, true)
      if(field == "itime"){
        structField = StructField(field, TimestampType, true)
      }
      structField
    })
    val schema = StructType(fields)
    //清洗日志，将所有字段全部洗出来，方便后续多个sql（query）的执行，这样就可以达到实现多个业务逻辑操作的目的
    //注意最后的select操作，其实select和selectExpr和spark.sql(需要先注册临时表)都可以操作列，比如这里的将value的json字段全部
    //展开的操作，select的关键操作是from_json 并指定schema，会自动提取字段并对应起来
    val finalSDF = sdf2.map(line => {
      val jsonStr = logFormator.format(line._1, bcIP.value, bcConf.value.fields)
      (jsonStr, line._2, line._3, line._4)
    }).select(from_json($"_1", schema) as "values", $"_2" as "partition", $"_3" as "offset", $"_4" as "timestamp")
      .select($"values.*", $"partition", $"offset", $"timestamp")
    //finalSDF.createOrReplaceTempView("heart")
    //val resPOS = spark.sql("select approx_count_distinct(uuid) from heart")

    //以下就是逻辑了
    //No.1 计算活跃用户，5分钟的窗口，1分钟的slide，只要有变化就修改mysql中的数据，mysql表设计有待优化
    val resUV = finalSDF.withWatermark("timestamp", "3 minutes").groupBy(window($"timestamp", "5 minutes", "1 minutes"), $"appkey").count()
    val queryUV = resUV.writeStream.outputMode("update").foreach(new MysqlSink).start()
    //val queryUV = resUV.writeStream.outputMode("update").foreach(new MysqlSink).option("checkpointLocation", "e:/checkpoint/queryUV/").start()

    //No.2 全国省市分布图，省份实时用户可以全部计算，但是城市太多，只能取topN
    val resPOS = finalSDF.withWatermark("timestamp", "3 minutes").groupBy(window($"timestamp", "5 minutes", "2 minutes"), $"appkey", $"province", $"city").count()
    val queryPOS = resPOS.writeStream.outputMode("update").foreach(new MysqlSink2).start()

    queryUV.awaitTermination()
    queryPOS.awaitTermination()
  }
}