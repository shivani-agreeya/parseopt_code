package com.verizon.oneparser.common
import org.apache.commons.dbcp2._
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.config.CommonConfigParameters

object GeoDataSource  extends LazyLogging {
  
  var connectionPool:BasicDataSource = null
  
  def closeConnection: Unit = {
    try  {
      logger.info("Closing Connection Pool on ShutdownHook...")
      if(connectionPool!=null) {
        connectionPool.close()
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

  }
  def getConnectionPool(commonConfigParams: CommonConfigParameters)={
    @transient
    val dbUrl = commonConfigParams.POSTGRES_CONN_SDE_URL
    if(connectionPool==null){
      connectionPool = new BasicDataSource()
      connectionPool.setUsername(commonConfigParams.POSTGRES_DB_USER)
      connectionPool.setPassword(commonConfigParams.POSTGRES_DB_PWD)
      connectionPool.setDriverClassName(commonConfigParams.POSTGRES_DRIVER)
      connectionPool.setUrl(dbUrl)
      connectionPool.setInitialSize(1)
      connectionPool.setMaxIdle(1)
      if(commonConfigParams.CURRENT_ENVIRONMENT == "DEV")
        connectionPool.setMaxTotal(1)
      else if(commonConfigParams.CURRENT_ENVIRONMENT == "PREPROD")
        connectionPool.setMaxTotal(1)
      else if(commonConfigParams.CURRENT_ENVIRONMENT == "PROD")
        connectionPool.setMaxTotal(2)
    }
    connectionPool
  }
  sys addShutdownHook(closeConnection)
}
