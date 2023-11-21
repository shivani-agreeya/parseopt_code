package com.verizon.oneparser.config

case class CommonConfigParameters(
                                   CURRENT_ENVIRONMENT: String = "DEV",
                                   POSTGRES_CONN_URL: String = "jdbc:postgresql://postgres.nuc:5432/test?currentSchema=dmat_qa&user=postgres&password=postgres-d",
                                   POSTGRES_CONN_SDE_URL: String = "jdbc:postgresql://postgres.nuc:5432/test?currentSchema=sde&user=postgres&password=postgres-d",
                                   POSTGRES_DRIVER: String = "org.postgresql.Driver",
                                   POSTGRES_DB_URL: String = "jdbc:postgresql://postgres.nuc:5432/test?currentSchema=dmat_qa",
                                   POSTGRES_DB_SDE_URL: String = "",
                                   POSTGRES_DB_USER: String = "postgres",
                                   POSTGRES_DB_PWD: String = "postgres-d",
                                   HIVE_HDFS_PATH: String = "hdfs://namenode.local:9000/apps/hive/warehouse/dmat_logs.db/",
                                   HDFS_FILE_DIR_PATH: String = "hdfs://namenode.local:9000/oneparser/data/dmat/staging/",
                                   HDFS_URI: String = "hdfs://namenode.local:9000",
                                   WRITE_HDFS_URI: String = "hdfs://namenode.local:9000",
                                   ES_HOST_PATH: String = "localhost",
                                   ES_PORT_NUM: Int = 9200,
                                   DM_USER_DUMMY: Int = 10088,
                                   HIVE_DB: String = "dmat_logs.",
                                   TBL_QCOMM_NAME: String = "LogRecord_QComm_Ext_D_08132019",
                                   TBL_QCOMM2_NAME: String = "LogRecord_QComm2_Ext_D_08132019",
                                   TBL_B192_NAME: String = "LogRecord_B192_Ext_D_08132019",
                                   TBL_B193_NAME: String = "LogRecord_B193_Ext_D_08132019",
                                   TBL_ASN_NAME: String = "LogRecord_ASN_Ext_D_08132019",
                                   TBL_NAS_NAME: String = "LogRecord_NAS_Ext_D_08132019",
                                   TBL_IP_NAME: String = "LogRecord_IP_Ext_D_08132019",
                                   TBL_QCOMM5G_NAME: String = "LogRecord_QComm_5G_Ext_D_08132019",
                                   BKP_HIVE_START_DATE: String = "",
                                   BKP_HIVE_END_DATE: String = "",
                                   BKP_TBL_QCOMM_NAME: String = "",
                                   BKP_TBL_QCOMM2_NAME: String = "",
                                   BKP_TBL_B192_NAME: String = "",
                                   BKP_TBL_B193_NAME: String = "",
                                   BKP_TBL_ASN_NAME: String = "",
                                   BKP_TBL_NAS_NAME: String = "",
                                   BKP_TBL_IP_NAME: String = "",
                                   BKP_TBL_QCOMM5G_NAME: String = "",
                                   DM_USER_HEADLESS: Int = 1089,
                                   BKP_ENVIRONMENT: String = "",
                                   LOG_FILE_DEL: Boolean = false,
                                   SEQ_FILE_DEL: String = "FALSE",
                                   DETAILED_HIVE_JOB_ANALYSIS: String = "FALSE",
                                   REPORTS_HIVE_TBL_NAME: String = "_D_05052020",
                                   PROCESSING_SERVER: String = "DEV",
                                   PROCESS_RM_ONLY: String = "FALSE",
                                   ES_CLUSTER_NAME: String = "docker-cluster",
                                   ES_NODE_ID: String = "localhost",
                                   ES_PORT_ID: String = "9300",
                                   ES_CELLSITE_INDEX: String = "cellsite",
                                   ES_CELLSITE_WEEKLY_INDEX_DATE: String = "2018-07-01",
                                   FINAL_HIVE_TBL_NAME: String = "FinalLogRecord_DMAT_C7000",
                                   FILE_SIZE_LIMIT: Int = 0,
                                   HIVE_MIN_PARTITION: Int = 50,
                                   PROCESS_FILE_TYPE: String = "ALL",
                                   LOG_GENERAL_EXCEPTION: String = "FALSE",
                                   REPROCESS_FILES: String = "FALSE",
                                   FTP_BASE_PATH: String = "",
                                   FTP_PROCESSED_PATH: String = "",
                                   FTP_RM_PROCESSED_PATH: String = "",
                                   RM_BASE_PATH: String = "",
                                   SPARK_MASTER: String = "localhost[2]",
                                   APP_NAME: String = "SingleParserProcess",
                                   DRIVER_BIND_ADDRESS: String = "127.0.0.1",
                                   DEBUG_MAX_TO_STRING_FIELDS: String = "300",
                                   LOG_LEVEL: String = "WARN",
                                   ZOOKEEPER_HOSTS: String = "zoo.nuc:2181",
                                   BOOTSTRAP_SERVERS: String = "broker0.nuc:9092,broker1.nuc:9092,broker2.nuc:9092",
                                   MAX_OFFSETS_PER_TRIGGER: Int = 25,
                                   STARTING_OFFSETS: String = "earliest",
                                   KAFKA_GROUP_ID: String = "DMATgroupId",
                                   INGESTION_KAFKA_TOPIC: String = "dlf",
                                   NOTIFICATION_KAFKA_TOPIC: String = "nservice",
                                   PROCESS_PARAM_VALUES_KAFKA_TOPIC: String = "processparamvalues",
                                   HIVE_KAFKA_TOPIC: String = "hiveprocessor",
                                   ES_KAFKA_TOPIC: String = "elastic",
                                   SCHEMA_REGISTRY_URL: String = "http://schema-registry.nuc:8081",
                                   PARAM_VALUE_SCHEMA_REGISTRY_ID: String = "latest",
                                   CHECKPOINT_LOCATION: String = "checkpoints",
                                   WRITE_SEQ_TO_HDFS_CHECKPOINT_SEGMENT: String = "write_seq_to_hdfs",
                                   READ_SEQ_TO_HIVE_CHECKPOINT_SEGMENT: String = "read_seq_to_hive",
                                   PROCESS_DMAT_RECORDS_CHECKPOINT_SEGMENT: String = "process_dmat_records",
                                   EMAIL_HOST_NAME:String="",
                                   EMAIL_SMTP_PORT:Integer = 25,
                                   EMAIL_FROM:String = "",
                                   EMAIL_SENDER_NAME:String = "",
                                   EMAIL_USER_NAME:String="",
                                   EMAIL_PWD:String = "",
                                   DBGW_FO_IMPL:String="false",
                                   IP_PARSER_JAR_SUPPORT:Boolean=false,
                                   TRIGGER_EMAILS:Boolean=true,
                                   KPI_RTP_PACKET:Boolean=true, //if false, switches off resource/time-intensive join for rtpPacketLoss
                                   KPI_RTP_PACKET_BROADCAST:Boolean=false, //changes broadcast property for DF (dmatIpDfOneSecAggDF) in a join performing KPI execution in rtpPacketLoss DF
                                   KPI_RTP_PACKET_CACHE:Boolean=false, //caches DF (dmatIpDfOneSecAggDF) involved in resource/time-intensive join to prevent its recalculation in case of broadcast timeout,
                                   WRITE_SEQ_PARTITIONS: Int = 1, //number of partitions for writing SequenceFile
                                   KAFKA_MIN_PARTITIONS: Int = 1,
                                   WRITE_DMAT_RECORDS_TO_ES: Boolean = true,
                                   GEO_SHAPE_FILE_PATH: String = "/oneparser/data/dmat/geoShapes/",
                                   ODLV_TRIGGER: Boolean = false,
                                   LOG_VIEWER_URL: String = "",
                                   ONP_GNBID_URL:String="",
                                   ONP_PCIDISTANCE_URL:String="",
                                   ES_USER:String="",
                                   ES_PWD:String="",
                                   ES_NESTEDFIELD_CUTOFFDATE:String = null,
                                   PATH_TO_CORE_SITE:String = "",
                                   PATH_TO_HDFS_SITE:String = "",
                                   PRIORITY_TIME_INTERVAL:Integer=6
                                 )
