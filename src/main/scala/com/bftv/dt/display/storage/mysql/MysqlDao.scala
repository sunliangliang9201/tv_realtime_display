package com.bftv.dt.display.storage.mysql

import java.sql.{Connection, PreparedStatement}
import com.bftv.dt.display.model.SSKeyConf
import org.slf4j.LoggerFactory

/**
  * 连接mysql执行各类sql操作
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object MysqlDao {

  val logger = LoggerFactory.getLogger(this.getClass)

  val ssSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"

  /**
    * 获取初始化的配置
    * @param ssKey key
    * @return 配置的case 对象
    */
  def getSSConf(ssKey: String): SSKeyConf ={
    var ssKeyConf: SSKeyConf = null
    var conn: Connection = null
    var ps: PreparedStatement = null

    try{
      conn  = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(ssSQL)
      ps.setNString(1, ssKey)
      val res = ps.executeQuery()
      res.last()
      val rows = res.getRow
      if (rows == 1){
        ssKeyConf = SSKeyConf(
          res.getString("streaming_key"),
          res.getString("app_name"),
          res.getString("driver_cores"),
          res.getString("formator"),
          res.getString("topics"),
          res.getString("group_id"),
          res.getString("table_name"),
          res.getString("fields").split(","),
          res.getString("broker_list")
        )
      }else{
        throw new Exception("Mysql sskey key set error ...")
      }
    }catch {
      case e: Exception => logger.error("Find the sskeyConf from mysql failed ..., " + e)
    }finally {
      if (null != ps){
        ps.close()
      }
      if (null != conn){
        conn.close()
      }
    }
    ssKeyConf
  }
}
