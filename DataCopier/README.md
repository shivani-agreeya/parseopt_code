#DataCopier

Project copies log files from SFTP to HDFS, creates records on postgres logs table and sends messages to kafka

##Required services
Service   |Description             |Target(s)
----------|------------------------|---------------------------------------------------------------------
postgres  | RDBMS for `logs` table | RDBMS log file metadata storage
namenode  | Hadoop namenode        | HDFS log file storage
datanode  | Hadoop Datanode        | HDFS log file storage
kafka     | Kafka broker(s)        | Spark job notification
zookeeper | ZK server(s)            | Concurrent metastore for `exactly-once delivery` processing strategy

##Supported log file types:
Filetype|Description
--------|------------------------
dlf     | DLF log file format
zip     | ZIP archived DLF file
drm_dlf | DRM_DLF log file format
dml_dlf | DML_DLF log file format
sig_dlf | SIG_DLF log file format

##Folder structure:
```
ftpdata
└── month0
    ├── ServerUpload
    ├── StaggingData
    │   └── RootMetric
    └── oneparser
        ├── failed
        ├── processed
        │   ├── 04152020
        │   │   └── 0.dlf
        │   └── 04162020
        │       └── 1.dlf
        ├── processed_dml
        └── staging
            ├── 04152020
            └── 04162020
```
####Folder structure description:
Folder                                   | Description          
-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------
/ftpdata/month0/ServerUpload/            | Data copier searches DLF files in ServerUpload dir based on SQL query `select * from logs wgere status = 'server upload done' and file_type='dlf' order by id asc`
/ftpdata/month0/StaggingData/            | Uploaded DLF log files
/ftpdata/month0/StaggingData/RootMetric/ | Uploaded DRM_DLF, DML_DLF, SIG_DLF log files
/ftpdata/month0/oneparser/staging/       | Output path
/ftpdata/month0/oneparser/processed/     | Processed DLF log files
/ftpdata/month0/oneparser/processed_dml/ | Processed DRM_DLF, DML_DLF, SIG_DLF log files
/ftpdata/month0/oneparser/failed/        | Not processed log files (processing filed for some reason filed)

##Kafka topics structure:
Each file type has a corresponding topic. The topic is named similarly to the file type.

##Kafka message structure:
Msg Filed|Description
---------|--------------------------------------------------------------
key      | Long
value    | Confluent avro encoded com.verizon.oneparser.avroschemas.Logs

##Build:
- `mvn clean package` #Build jar file. Note: please build and install [AvroSchemas](https://vzwdt.com/git/ParserOpt/ParserOpt/tree/master/AvroSchemas) before
- `mvn jib:dockerBuild` #Build docker image using docker running on a local machine
- `mvn jib:build`       #Build docker image and upload it to repository (does not require a local docker runtime)

Please provide proper profile if required.

[JIB Homepage](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin)

docker-compose.yml example
```
version: "3"

services:
  datacopier:
    image: localhost/data-copier
    container_name: datacopier
    hostname: datacopier
    volumes:
      - /Volumes/storage/docker/volumes/datacopier/tmp:/tmp
      - /Volumes/storage/docker/volumes/datacopier/log:/logs
```


