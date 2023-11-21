# Dev
If you will be launching elasticsearch container, you need to increase your vm.max_map_count
`sudo sysctl -w vm.max_map_count=262144`
The whole docker compose takes a lot of memory, with all jobs running, can easily eat up 40GB of memory. 
So be sure to have enough swap space. 
```
docker-compose -f dev/docker/docker-compose.yml up -d
docker exec -it hive-server bash
beeline -u jdbc:hive2://localhost:10000/
create database dmat_logs;
```

* import `docker/init.sql` to postgres if some tables will be missing
`psql -h localhost -U postgres < dev/docker/init.sql`

```
mkdir -p /tmp/ftpdata/month0/StaggingData/
mkdir -p /tmp/ftpdata/month0/StaggingData/RootMetric/
```
* When launching services, use the dev profile

Add avro schemas to `http://localhost:8001/#/`  
{dlf, drm_dlf, dml_dlf, sig_dlf}-value as AvroSchemas/src/main/resources/logs.avsc

####Job numbers
1. WriteSequenceToHDFS
2. ReadSequenceToHiveNew
3. ProcessDMATRecords
