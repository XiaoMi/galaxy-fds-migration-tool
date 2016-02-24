## Galaxy FDS Migration Tool

A MapReduce tool to migrate objects or files parallely between different object storage systems like AWS S3, XIAOMI Galaxy FDS and so on.

### Config
The source and target for migiration is set by the configs: migration.source.class and migration.target.class. Current suppoted source classes are
* com.xiaomi.infra.galaxy.fds.source.S3Source for AWS S3
* com.xiaomi.infra.galaxy.fds.source.FDSSource for XIAOMI Galaxy FDS
* com.xiaomi.infra.galaxy.fds.source.HTTPSource for downloading objects through http protocal

Othe sources can be supported by a class implementing Source interface.

#### AWS S3
To support the AWS S3 as a migration source, following configs must be set.

```xml
  <property>
    <name>migration.source.class</name>
    <value>com.xiaomi.infra.galaxy.fds.source.S3Source</value>
  </property>
  <property>
    <name>migration.source.s3.endpoint.name</name>
    <value>${endpoint-name}</value>
  </property>
  <property>
    <name>migration.source.s3.bucket.name</name>
    <value>${bucket-name}</value>
  </property>
  <property>
    <name>migration.source.s3.access.key</name>
    <value>${access-key}</value>
  </property>
  <property>
    <name>migration.source.s3.access.secret</name>
    <value>${access-secret}</value>
  </property>
```

If the AWS S3 is the migration target, you need change the prefix "migration.source" to "migration.target".

#### XIAOMI Galaxy FDS(dev.xiaomi.com)
To support the XIAOMI Galaxy FDS as a migration source, following configs must be set.

```xml
  <property>
    <name>migration.source.class</name>
    <value>com.xiaomi.infra.galaxy.fds.source.FDSSource</value>
  </property>
  <property>
    <name>migration.source.fds.region.name</name>
    <value>${region-name}</value>
  </property>
  <property>
    <name>migration.source.fds.bucket.name</name>
    <value>${bucket-name}</value>
  </property>
  <property>
    <name>migration.source.fds.access.key</name>
    <value>${access-key}</value>
  </property>
  <property>
    <name>migration.source.fds.access.secret</name>
    <value>${access-secret}</value>
  </property>
```

If the FDS is the migration target, you need change the prefix "migration.source" to "migration.target".

#### HTTPSource
HTTP source only can be the migration source.

```xml
  <property>
    <name>migration.source.class</name>
    <value>com.xiaomi.infra.galaxy.fds.source.HTTPSource</value>
  </property>
  <property>
    <name>migration.source.http.url.pattern</name>
    <value>${url-pattern}</value>
  </property>
  <property>
    <name>migration.source.http.downloader.list</name>
    <value>ip1,ip2</value>
  </property>
```

### Run
#### Local Test
Set the configs for the migration source and target, then run following cmds:

```shell
bash target/galaxy-fds-migration-tool-1.0-SNAPSHOT/bin/migration.sh -conf conf/job-local.xml list prefix fileList
bash target/galaxy-fds-migration-tool-1.0-SNAPSHOT/bin/migration.sh -conf conf/job-local.xml copy demo.input output/
```
#### Yarn Mode
Set the configs for the migration source and target, then run following cmds:

```shell
hadoop jar target/galaxy-fds-migration-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.xiaomi.infra.galaxy.fds.migration.Migration -conf conf/job-yarn.xml list hdfs:///user/input/fileList
hadoop jar target/galaxy-fds-migration-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.xiaomi.infra.galaxy.fds.migration.Migration -Dmapreduce.job.user.classpath.first=true -Dmapreduce.application.classpath="./" -conf conf/job-yarn.xml copy hdfs:///user/input hdfs:///user/output
```

## TODO
* Support other object/File storage systems like HDFS, Azure Blob Storage and Aliyun OSS.

