#!/bin/sh

EC_POLICY='RS-6-3-1024k'
first_partition="dt=20210807"
db_name="hive_vipudp"
tbl_name="ads_user_behavior_track_ds"
path_cluster="bipcluster04"
partition_path="hdfs://bipcluster04/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds"

spark_distcp(){
  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
  export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

  # 提交作业中，需要的参数
  # local ec_table_name="hive_vipudp.ads_user_behavior_track_ds_setec_by_xuefei"
  # local ec_table_location="hdfs://bipcluster04/backup/ecdata_tbl" + "/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei"
  # local file_size_to_partition="23998505020"
  # local srcTbl="hive_vipudp.ads_user_behavior_track_ds"
  # fixme 修改
  local ec_table_name=$1_setec_by_xuefei_567
  local file_size_to_partition='"'$2'"'
  local srcTbl=$1
  local spark_yarn_queue=$3


  local ec_table_location=""
  if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" && $first_partition == "NULL" ]];then
    local last_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
    local pre_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F"/${last_path_inner}" '{print $1}'`
    ec_table_location="hdfs://${path_cluster}/backup/ec_other_to_check${pre_path_inner}"
  elif [[ $first_partition == "NULL" && ${db_name} != "NULL"  && ${tbl_name} != "NULL" ]];then
    ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}"
  else
    ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}/${tbl_name}"
  fi

  # todo for test
  ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}/${tbl_name}_test"

  local first_partition_field=`echo $first_partition | awk -F '=' '{print $1}'`
  local partition_last_value=`echo $first_partition | awk -F '=' '{print $2}'`
  # 此处声明为全局变量，便于后续获取
  ec_dt_location=${ec_table_location}/${first_partition}

  # 先在脚本中set ec policy
  # hdfs ec -setPolicy  -path hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  # hdfs dfs -test -e hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  hdfs dfs -test -e ${ec_table_location}
  if [[ $? -ne 0 ]];then
    hdfs dfs -mkdir -p ${ec_table_location}
  fi
  hdfs ec -setPolicy -path ${ec_table_location} -policy ${EC_POLICY}

  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue ${spark_yarn_queue} \
  --driver-memory 4G \
  --executor-memory 8G \
  --executor-cores 4 \
  --conf 'spark.sql.files.maxPartitionBytes=20971520' << EOF

  var parallelism = ${file_size_to_partition}.toLong / 1024 / 1024 / 100
  // todo 如果太大，设置上限
  parallelism = if (parallelism <= 0) 1 else parallelism

  println(s"srcTbl: ${srcTbl}, ec_table_name: ${ec_table_name}, partition: ${first_partition_field}='${partition_last_value}', pall: $pall")

  // drop 其他的schema
  val df = spark.sql(s"select * from ${srcTbl} where ${first_partition_field}='${partition_last_value}'").drop("dt")

  df.count()
  df.createOrReplaceTempView("src")

  // 将该表set location到backup，同时将该表的hdfs路径设置ec policy；如果已经存在，则check一下分区有没有数据

  try {
    spark.sql(s"show tables in ${db_name} like '${ec_table_name}' ").toDF().collect().apply(0)
  } catch {
    case ex: ArrayIndexOutOfBoundsException =>
      // 表不存在，则创建，并设置hdfs_path的ec policy
      // fixme 后续改成hdfsClient，去设置ec policy
      spark.sql(s"create table ${ec_table_name} like ${srcTbl}")

      spark.sql(s"alter table ${ec_table_name} set location  '${ec_table_location}' ")
  }

  spark.sql(s"insert overwrite ${ec_table_name} partition (${first_partition_field}='${partition_last_value}') select * from src").repartition(parallelism.toInt)


EOF

  # echo ${ret}
}


spark_distcp_new(){
  echo "[start $(date +%Y%m%d-%H:%M:%S)] start to distcp with spark!"
  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
  export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

  # 提交作业中，需要的参数
  # local ec_table_name="hive_vipudp.ads_user_behavior_track_ds_setec_by_xuefei"
  # local ec_table_location="hdfs://bipcluster04/backup/ecdata_tbl" + "/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei"
  # local file_size_to_partition="23998505020"
  # local srcTbl="hive_vipudp.ads_user_behavior_track_ds"

  # fixme
  local ec_table_name=$1_setec_with_spark_test_1
  local file_size_to_partition='"'$2'"'
  local srcTbl=$1
  local spark_yarn_queue=$3


  local ec_table_location=""
  if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" && $first_partition == "NULL" ]];then
    local last_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
    local pre_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F"/${last_path_inner}" '{print $1}'`
    local ec_table_location="hdfs://${path_cluster}/backup/ec_other_to_check${pre_path_inner}"
    # 非表数据不能用spark-distcp
    # todo 退化为hadoop-distcp
    hadoop distcp -Dhadoop.ssl.enabled=false -Dmapred.job.queue.name=${spark_yarn_queue} -Dyarn.app.mapreduce.am.staging-dir=${STAGING_DIR} -update -pbugp -i ${MAPSET} -skipcrccheck ${partition_path} ${partition_path}_setec

  elif [[ $first_partition == "NULL" && ${db_name} != "NULL"  && ${tbl_name} != "NULL" ]];then
    local ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}"
    ec_dt_location=${ec_table_location}/${ec_table_name}
  else
    local ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}/${ec_table_name}"
    # 此处声明为全局变量，便于后续获取
    ec_dt_location=${ec_table_location}/${first_partition}
  fi

  local first_partition_field=`echo $first_partition | awk -F '=' '{print $1}'`
  local partition_last_value=`echo $first_partition | awk -F '=' '{print $2}'`
  # 此处声明为全局变量，便于后续获取
  ec_dt_location=${ec_table_location}/${first_partition}

  # 先在脚本中set ec policy
  # hdfs ec -setPolicy  -path hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  # hdfs dfs -test -e hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  hdfs dfs -test -e ${ec_table_location}
  if [[ $? -ne 0 ]];then
    hdfs dfs -mkdir -p ${ec_table_location}
  fi
  hdfs ec -setPolicy -path ${ec_table_location} -policy ${EC_POLICY}

  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue ${spark_yarn_queue} \
  --driver-memory 4G \
  --executor-memory 8G \
  --executor-cores 4 \
  --conf 'spark.sql.files.maxPartitionBytes=20971520' << EOF

  import com.google.common.io.Files
  import java.nio.charset.StandardCharsets
  import java.io.File

  var parallelism = ${file_size_to_partition}.toLong / 1024 / 1024 / 100
  // todo 如果太大，设置上限
  parallelism = if (parallelism <= 0) 1 else parallelism

  println(s"srcTbl: ${srcTbl}, ec_table_name: ${ec_table_name}, partition: ${first_partition_field}='${partition_last_value}', parallelism: $parallelism")

  // drop 其他的schema
  val df = spark.sql(s"select * from ${srcTbl} where ${first_partition_field}='${partition_last_value}'").drop(s"${first_partition_field}")

  df.createOrReplaceTempView("src")

  // 将该表set location到backup，同时将该表的hdfs路径设置ec policy；如果已经存在，则check一下分区有没有数据

  try {
    spark.sql(s"show tables in ${db_name} like '${ec_table_name}' ").toDF().collect().apply(0)
  } catch {
    case ex: ArrayIndexOutOfBoundsException =>
      // 表不存在，则创建，并设置hdfs_path的ec policy
      // fixme 后续改成hdfsClient，去设置ec policy
      spark.sql(s"create table ${ec_table_name} like ${srcTbl}")

      spark.sql(s"alter table ${ec_table_name} set location  '${ec_table_location}' ")
  }

  spark.sql(s"insert overwrite ${ec_table_name} partition (${first_partition_field}='${partition_last_value}') select * from src").repartition(parallelism.toInt)

  val countVal = spark.sql(s"select count(1) from ${ec_table_name} where ${first_partition_field}='${partition_last_value}'").show
  Files.write(countVal + "", new File(s"${FILEDIR}", s"spark-distcp-count.log"),
        StandardCharsets.UTF_8)
EOF
  local res=$?
  echo "[end $(date +%Y%m%d-%H:%M:%S)] distcp with spark finished!"
  local countVal=`cat ${FILEDIR}/spark-distcp-count.log`
  if [[ $res -eq 0 && $countVal -gt 0  ]];then
    return 0
  else
    return 1
  fi
}

spark_distcp_new hive_vipudp.ads_user_behavior_track_ds  22619373448  root.basic_platform.critical







