package com.verizon.reports

import javax.sql.DataSource

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonInclude, PropertyAccessor}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.{Bean, Configuration, PropertySource}
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer
import org.springframework.core.env.Environment
import org.springframework.http.converter.HttpMessageConverter
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.jdbc.datasource.{DataSourceTransactionManager, DriverManagerDataSource}
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.{EnableTransactionManagement, TransactionManagementConfigurer}
import org.springframework.web.servlet.config.annotation.{EnableWebMvc, _}
import com.verizon.reports.common.CommonConfigParameters
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

@Configuration
@EnableWebMvc
@PropertySource(Array("classpath:properties/${spring.profiles.active}/config.properties"))
@EnableAutoConfiguration
@EnableTransactionManagement
@EnableAsync
class AppConfig extends WebMvcConfigurerAdapter with TransactionManagementConfigurer with LazyLogging {

  @transient
  protected override lazy val logger: Logger= Logger(LoggerFactory.getLogger(getClass.getName))  
  
  @Autowired private val environment:Environment=null
  
  @Value("${spring.profiles.active}") private val activeProfile:String =null

  @Value("${jdbc.driverClassName}") val driverClassName: String = null

  var args= new Array[String](1)
  args(0)="asyncExecutor"

  @Bean def transactionManager = new DataSourceTransactionManager(dataSource)

  override def annotationDrivenTransactionManager: PlatformTransactionManager = transactionManager
  /*val commonConfigParams:CommonConfigParameters = CommonConfigParameters(CURRENT_ENVIRONMENT=sparkSession.conf.get("spark.CURRENT_ENVIRONMENT"),POSTGRES_CONN_URL=sparkSession.conf.get("spark.POSTGRES_CONN_URL"), POSTGRES_CONN_SDE_URL = sparkSession.conf.get("spark.POSTGRES_CONN_SDE_URL"),
      POSTGRES_DRIVER=sparkSession.conf.get("spark.POSTGRES_DRIVER"),POSTGRES_DB_URL=sparkSession.conf.get("spark.POSTGRES_DB_URL"),POSTGRES_DB_SDE_URL=sparkSession.conf.get("spark.POSTGRES_DB_SDE_URL"),
      POSTGRES_DB_USER=sparkSession.conf.get("spark.POSTGRES_DB_USER"),POSTGRES_DB_PWD=sparkSession.conf.get("spark.POSTGRES_DB_PWD"),HIVE_HDFS_PATH=sparkSession.conf.get("spark.HIVE_HDFS_PATH"),
      HDFS_FILE_DIR_PATH=sparkSession.conf.get("spark.HDFS_FILE_DIR_PATH"), HDFS_URI=sparkSession.conf.get("spark.HDFS_URI"),ES_HOST_PATH=sparkSession.conf.get("spark.ES_HOST_PATH"),ES_PORT_NUM=sparkSession.conf.get("spark.ES_PORT_NUM").toInt,DM_USER_DUMMY=sparkSession.conf.get("spark.DM_USER_DUMMY").toInt)
   * */
  @Bean
  def commonConfigParams(): CommonConfigParameters ={
    logger.info("ACTIVE PROFILE="+activeProfile)
    logger.info("CURRENT_ENVIRONMENT="+environment.getProperty("spark.CURRENT_ENVIRONMENT"))
    
    logger.info("POSTGRES_CONN_URL="+environment.getProperty("spark.POSTGRES_CONN_URL"))
    logger.info("POSTGRES_CONN_SDE_URL ="+ environment.getProperty("spark.POSTGRES_CONN_SDE_URL"))
    logger.info("POSTGRES_DRIVER="+environment.getProperty("spark.POSTGRES_DRIVER"))
    logger.info("POSTGRES_DB_URL="+environment.getProperty("spark.POSTGRES_DB_URL"))
    logger.info("POSTGRES_DB_SDE_URL="+environment.getProperty("spark.POSTGRES_DB_SDE_URL"))
    logger.info(" POSTGRES_DB_USER="+environment.getProperty("spark.POSTGRES_DB_USER"))
    logger.info("POSTGRES_DB_PWD="+environment.getProperty("spark.POSTGRES_DB_PWD"))
    logger.info("HIVE_HDFS_PATH="+environment.getProperty("spark.HIVE_HDFS_PATH"))
    logger.info("HDFS_FILE_DIR_PATH="+environment.getProperty("spark.HDFS_FILE_DIR_PATH")) 
    logger.info("HDFS_URI="+environment.getProperty("spark.HDFS_URI"))
    logger.info("ES_HOST_PATH="+environment.getProperty("spark.ES_HOST_PATH"))
    logger.info("ES_PORT_NUM="+environment.getProperty("spark.ES_PORT_NUM"))
    logger.info("DM_USER_DUMMY="+environment.getProperty("spark.DM_USER_DUMMY"))
    logger.info("REPORT_LOG_CUTOFF_DATE="+environment.getProperty("spark.REPORT_LOG_CUTOFF_DATE"))
      /*
       * spark.HIVE_DB = dmat_logs.
spark.MaxRTP_PacketLossVal = 10000
spark.HIVE_DB = dmat_logs.
spark.TBL_QCOMM_NAME = LogRecord_QComm_Ext
spark.TBL_QCOMM2_NAME = LogRecord_QComm2_Ext
spark.TBL_B192_NAME = LogRecord_B192_Ext
spark.TBL_B193_NAME = LogRecord_B193_Ext
spark.TBL_ASN_NAME = LogRecord_ASN_Ext
spark.TBL_NAS_NAME = LogRecord_NAS_Ext
spark.TBL_IP_NAME = LogRecord_IP_Ext
spark.TBL_QCOMM5G_NAME = LogRecord_QComm_5G_Ext
spark.ES_HOST_PATH = 10.20.40.28
spark.ES_PORT_NUM = 9201
spark.DM_USER_DUMMY = 10088
       * */
      
      CommonConfigParameters(CURRENT_ENVIRONMENT=environment.getProperty("spark.CURRENT_ENVIRONMENT"),
      POSTGRES_CONN_URL=environment.getProperty("spark.POSTGRES_CONN_URL"),
      POSTGRES_CONN_SDE_URL = environment.getProperty("spark.POSTGRES_CONN_SDE_URL"),
      POSTGRES_DRIVER=environment.getProperty("spark.POSTGRES_DRIVER"),
      POSTGRES_DB_URL=environment.getProperty("spark.POSTGRES_DB_URL"),
      POSTGRES_DB_SDE_URL=environment.getProperty("spark.POSTGRES_DB_SDE_URL"),
      POSTGRES_DB_USER=environment.getProperty("spark.POSTGRES_DB_USER"),
      POSTGRES_DB_PWD=environment.getProperty("spark.POSTGRES_DB_PWD"),
      HIVE_HDFS_PATH=environment.getProperty("spark.HIVE_HDFS_PATH"),
      HDFS_FILE_DIR_PATH=environment.getProperty("spark.HDFS_FILE_DIR_PATH"), 
      HDFS_URI=environment.getProperty("spark.HDFS_URI"),
      ES_HOST_PATH=environment.getProperty("spark.ES_HOST_PATH"),
      ES_PORT_NUM=environment.getProperty("spark.ES_PORT_NUM").toInt,
      DM_USER_DUMMY=environment.getProperty("spark.DM_USER_DUMMY").toInt,
      HIVE_DB=environment.getProperty("spark.HIVE_DB"),
      TBL_QCOMM_NAME=environment.getProperty("spark.TBL_QCOMM_NAME"),
      TBL_QCOMM2_NAME=environment.getProperty("spark.TBL_QCOMM2_NAME"),
      TBL_B192_NAME=environment.getProperty("spark.TBL_B192_NAME"),
      TBL_B193_NAME=environment.getProperty("spark.TBL_B193_NAME"),
      TBL_ASN_NAME=environment.getProperty("spark.TBL_ASN_NAME"),
      TBL_NAS_NAME=environment.getProperty("spark.TBL_NAS_NAME"),
      TBL_IP_NAME=environment.getProperty("spark.TBL_IP_NAME"),
      TBL_QCOMM5G_NAME=environment.getProperty("spark.TBL_QCOMM5G_NAME"))
  }

  @Bean
  def dataSource: DataSource = {
    println("Hello!!!!!!!!!")
    val dataSource: DriverManagerDataSource = new DriverManagerDataSource

    dataSource.setDriverClassName(environment.getProperty("spark.POSTGRES_DRIVER"))
    dataSource.setUrl(environment.getProperty("spark.POSTGRES_DB_URL"))
    dataSource.setUsername(environment.getProperty("spark.POSTGRES_DB_USER"))
    dataSource.setPassword(environment.getProperty("spark.POSTGRES_DB_PWD"))

    dataSource
  }

  @Bean(Array("asyncExecutor"))
  def getAsyncExecutor: ThreadPoolTaskExecutor = {
    val executor :ThreadPoolTaskExecutor  = new ThreadPoolTaskExecutor
    executor.setCorePoolSize(20)
    executor.setMaxPoolSize(1000)
    executor.setWaitForTasksToCompleteOnShutdown(true)
    executor.setThreadNamePrefix("Async-")
    executor
  }

  @Bean def sparkSession: SparkSession = SparkSession.builder().enableHiveSupport()
    .appName("ReportView")
//    .master("spark://10.20.40.180:7077")
//    .master("yarn")
//    .config("spark.yarn.queue", "LV")
    .config("spark.submit.deployMode", "client")
    .getOrCreate()



  override def configureDefaultServletHandling(configurer: DefaultServletHandlerConfigurer): Unit = {
    configurer.enable()
  }

  override def addCorsMappings(registry: CorsRegistry): Unit = {
    registry.addMapping("/**")
  }

  override def addResourceHandlers(registry: ResourceHandlerRegistry): Unit = {
    registry.addResourceHandler("/resources/**").addResourceLocations("/resources/")
  }

  override def configureMessageConverters(converters: java.util.List[HttpMessageConverter[_]]): Unit =
    converters.add(jackson2HttpMessageConverter())

  @Bean
  def jackson2HttpMessageConverter(): MappingJackson2HttpMessageConverter =
    new MappingJackson2HttpMessageConverter(objectMapper())

  @Bean
  def objectMapper(): ObjectMapper =
    new ObjectMapper() {
      setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      registerModule(DefaultScalaModule)
    }




}

object AppConfig {
  @Bean def propertySourcesPlaceholderConfigurer: PropertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer
}




