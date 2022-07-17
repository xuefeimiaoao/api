#!/bin/sh

getNum()
{
    s1=`echo $1 | cut -b 1`
    s2=`echo $1 | cut -b 2`
    s3=`echo $1 | cut -b 3`
    if [[ "$s1" == "r" ]];then
        n1=4
    else
        n1=0
    fi
    if [[ "$s2" == "w" ]];then
        n2=2
    else
        n2=0
    fi
    if [[ "$s3" == "x" ]];then
        n3=1
    else
        n3=0
    fi
    sum=`expr $n1 + $n2 + $n3`
    echo $sum
}

getSumNum()
{
    str1=`echo $1 | cut -b 2-4`
    str2=`echo $1 | cut -b 5-7`
    str3=`echo $1 | cut -b 8-10`
    num1=`getNum $str1`
    num2=`getNum $str2`
    num3=`getNum $str3`
    echo ${num1}${num2}${num3}
}

## make sure oldpath and newpath auth/user/group unity
ch_path_pug()
{
    oldpath=$1
    newpath=$2

    auth_user_group=`hadoop fs -ls -d $oldpath | awk '{print $1 "\t" $3 "\t" $4}'`
    path_auth=`echo $auth_user_group | awk '{print $1}'`
    path_user=`echo $auth_user_group | awk '{print $2}'`
    path_group=`echo $auth_user_group | awk '{print $3}'`

    ## unity authority
    path_auth_num=`getSumNum $path_auth`
    echo "[Info]: hadoop fs -chmod ${path_auth_num} $newpath"
    hadoop fs -chmod ${path_auth_num} $newpath

    ## unity user and group
    echo "[Info]: hadoop fs -chown ${path_user}:${path_group} $newpath"
    hadoop fs -chown ${path_user}:${path_group} $newpath
}

## iterator for unity auth/user/group
unify_path_pug()
{
    head_oldpath=$1
    head_newpath=$2
    all_partitions_name=$3
    echo "[Info]: Start pug --- ${head_oldpath}/${all_partitions_name} ${head_newpath}/${all_partitions_name}"

    eval $(echo $all_partitions_name | awk '{split($0, filearray, "/");for(i in filearray)print "arr["i"]="filearray[i]}')
    for i in ${arr[*]}
    do
        if [[ -z ${i} ]];then
            echo "[Wrong]: ${i} is null"
            exit
        fi
        head_oldpath="${head_oldpath}/${i}"
        head_newpath="${head_newpath}/${i}"
        echo "[Info]: pug---------- ${head_oldpath} ${head_newpath}"
        ch_path_pug $head_oldpath $head_newpath
    done
}

## 优先检查ec策略，如果数据库中已经删除过，后面又重新入库，但实际上已经ec的数据，跳过。
check_single_file(){
    par_path=$1
    hdfs dfs -test -d $par_path
    if [[ $? -ne 0 ]];then
      # 文件
      initial_ec_policy=`hdfs ec -getPolicy -path $par_path`
      echo "优先检查partition_path[${par_path}]的ec策略，为:$initial_ec_policy"
      # fixme 后续如果需要修改ecPolicy，这边也需要更改
      if [[ "$initial_ec_policy" == "RS-6-3-1024k" ]];then
        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
          update ${TARGET_TABLE} set ec_status = 4 where id = ${current_record_id};"
        echo "当前记录已经做过Ec，跳过！"
        return 0
      else
        # 只需要检查一个文件
        return 1
      fi

    else
      # 目录
      file_array=(`hdfs dfs -ls $par_path | grep hdfs | awk '{print $8}'`)
      for fd in ${file_array[@]}
        do
          check_single_file $fd
          res=$?
          if [[ "$res" == "1" ]];then
            return 1
          elif [[ "$res" == "0" ]]; then
            return 0
          fi
      done
    fi
}

spark_distcp(){
  echo "INFO [start $(date +%Y%m%d-%H:%M:%S)] start to distcp with spark!"
  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
  export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

  # 提交作业中，需要的参数
  # local ec_table_name="hive_vipudp.ads_user_behavior_track_ds_setec_by_xuefei"
  # local ec_table_location="hdfs://bipcluster04/backup/ecdata_tbl" + "/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei"
  # local file_size_to_partition="23998505020"
  # local srcTbl="hive_vipudp.ads_user_behavior_track_ds"
  local ec_table_name=$1_setec_with_spark
  local file_size_to_partition='"'$2'"'
  local file_size_to_partition_num=$2
  local srcTbl=$1
  local spark_yarn_queue=$3
  local pure_ec_table_name=${tbl_name}
  local initial_file_num=`hdfs dfs -count ${partition_path} | awk -F ' ' '{print $2}'`
  echo "INFO initial_file_num:$initial_file_num"

  local ec_table_location=""
  if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" && $first_partition == "NULL" ]];then
    local last_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
    local pre_path_inner=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F"/${last_path_inner}" '{print $1}'`
    local ec_table_location="hdfs://${path_cluster}/backup/ec_other_to_check${pre_path_inner}"
    # 非表数据不能用spark-distcp
    echo "INFO [mr-distcp|start $(date +%Y%m%d-%H:%M:%S)] start mr-distcp"
    ec_dt_location=${partition_path}_setec
    hdfs dfs -test -e ${ec_dt_location}
    if [[ $? -eq 0 ]];then
      # failed
      echo "ERROR [mr-distcp] setec path has existed --- ${ec_dt_location}"
      mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 5 where id = ${current_record_id};"
      spark_distcp_res=11
      return 0
    fi
    hdfs dfs -mkdir -p ${ec_dt_location}
    hdfs ec -setPolicy -path ${ec_dt_location} -policy ${EC_POLICY}
    hadoop distcp -Dhadoop.ssl.enabled=false -Dmapred.job.queue.name=${spark_yarn_queue} -Dyarn.app.mapreduce.am.staging-dir=${STAGING_DIR} -update -pbugp -i ${MAPSET} -skipcrccheck ${partition_path} ${ec_dt_location}
    local res_of_distcp=$?
    echo "INFO [mr-distcp|end $(date +%Y%m%d-%H:%M:%S)] finish distcp with mapreduce!"
    return $res_of_distcp
  elif [[ $first_partition == "NULL" && ${db_name} != "NULL"  && ${tbl_name} != "NULL" ]];then
    local ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}"
    ec_dt_location=${ec_table_location}/${pure_ec_table_name}
    # 判断是否需要小文件合并
    local count_partition_path=`hdfs dfs -count ${partition_path} | awk '{printf("%d",$3/$2)}'`
    echo "INFO average-size:$count_partition_path"
    if [[ ${count_partition_path} -gt ${FILE_MERGE_THRESHOLD} ]];then
      echo "INFO [mr-distcp|start $(date +%Y%m%d-%H:%M:%S)] start mr-distcp"
      ec_dt_location=${partition_path}_setec
      hdfs dfs -test -e ${ec_dt_location}
      if [[ $? -eq 0 ]];then
          # failed
          echo "ERROR [mr-distcp]: setec path has existed --- ${ec_dt_location}"
          mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                    update ${TARGET_TABLE} set ec_status = 5 where id = ${current_record_id};"
          spark_distcp_res=11
          return 0
      fi
      hdfs dfs -mkdir -p ${ec_dt_location}
      hdfs ec -setPolicy -path ${ec_dt_location} -policy ${EC_POLICY}
      hadoop distcp -Dhadoop.ssl.enabled=false -Dmapred.job.queue.name=${spark_yarn_queue} -Dyarn.app.mapreduce.am.staging-dir=${STAGING_DIR} -update -pbugp -i ${MAPSET} -skipcrccheck ${partition_path} ${ec_dt_location}
      local res_of_distcp=$?
      echo "INFO [mr-distcp|end $(date +%Y%m%d-%H:%M:%S)] finish distcp with mapreduce!"
      return $res_of_distcp
    fi
  else
    local ec_table_location="hdfs://${path_cluster}/backup/ec_tbl_to_check/${db_name}/${pure_ec_table_name}"
    # 此处声明为全局变量，便于后续获取
    ec_dt_location=${ec_table_location}/${first_partition}
    # 判断是否需要小文件合并
    local count_partition_path=`hdfs dfs -count ${partition_path} | awk '{printf("%d",$3/$2)}'`
    echo "INFO average-size:$count_partition_path"
    if [[ "$SPARK_DYNAMIC_PART_ENABLE" -eq "0" || ${count_partition_path} -gt ${FILE_MERGE_THRESHOLD} ]];then
      local show_partitions=count_res=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${spark_yarn_queue}; show partitions ${db_name}.${tbl_name}" | head -n 10`
      if [[ "$show_partitions" == */* || ${count_partition_path} -gt ${FILE_MERGE_THRESHOLD} ]];then
        echo "INFO [mr-distcp|start $(date +%Y%m%d-%H:%M:%S)] start mr-distcp"
        # 存在多级分区，暂时降级为distcp
        ec_dt_location=${partition_path}_setec
        hdfs dfs -test -e ${ec_dt_location}
        if [[ $? -eq 0 ]];then
            # failed
            echo "ERROR [mr-distcp]: setec path has existed --- ${ec_dt_location}"
            mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                      update ${TARGET_TABLE} set ec_status = 5 where id = ${current_record_id};"
            spark_distcp_res=11
            return 0
        fi
        hdfs dfs -mkdir -p ${ec_dt_location}
        hdfs ec -setPolicy -path ${ec_dt_location} -policy ${EC_POLICY}
        hadoop distcp -Dhadoop.ssl.enabled=false -Dmapred.job.queue.name=${spark_yarn_queue} -Dyarn.app.mapreduce.am.staging-dir=${STAGING_DIR} -update -pbugp -i ${MAPSET} -skipcrccheck ${partition_path} ${ec_dt_location}
        local res_of_distcp=$?
        echo "INFO [mr-distcp|end $(date +%Y%m%d-%H:%M:%S)] finish distcp with mapreduce!"
        return $res_of_distcp
      fi
    fi
  fi

  local first_partition_field=`echo $first_partition | awk -F '=' '{print $1}'`
  local partition_last_value=`echo $first_partition | awk -F '=' '{print $2}'`

  # 先在脚本中set ec policy
  # hdfs ec -setPolicy  -path hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  # hdfs dfs -test -e hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  hdfs dfs -test -e ${ec_table_location}
  if [[ $? -ne 0 ]];then
    hdfs dfs -mkdir -p ${ec_table_location}
  fi
  hdfs ec -setPolicy -path ${ec_table_location} -policy ${EC_POLICY}

  # 计算最大文件的大小
  local num_of_partitions=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${QUEUE};show partitions ${srcTbl}" | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  ((num_of_partitions=num_of_partitions-1))
  local count_cmd="hdfs dfs -count ${partition_path}"
  if [[ "${count_cmd:0-1}" -eq "/" ]];then
    # 删掉末尾的/;${vvv:0:${#vvv}-1}
    local count_cmd=${count_cmd:0:${#count_cmd}-1}
  fi
  local count_cmd="${count_cmd}/*"

  if ((num_of_partitions>0));then
    for i in `seq 1 ${num_of_partitions}`
      do
        local count_cmd="${count_cmd}/*"
    done
  fi
  echo "INFO [spark_distcp] count_cmd:${count_cmd}"
  local max_partition_bytes=`${count_cmd} | awk '{print $3}' | sort -nr | head -n1`
  echo "INFO [spark_distcp] max_partition_bytes:${max_partition_bytes}"
  # 如果最大文件大小小于100M，则按照100M切分
  if (( max_partition_bytes < ${FILE_MERGE_THRESHOLD} ));then
    ((max_partition_bytes=${FILE_MERGE_THRESHOLD}))
  fi

  # 计算合理(最小)的文件数
  ((parallelism_computed=file_size_to_partition_num/max_partition_bytes))
  if (( initial_file_num < parallelism_computed  ));then
          (( parallelism_computed=initial_file_num  ))
  fi
  if (( parallelism_computed < 1  ));then
          (( parallelism_computed=1  ))
  fi
  echo "INFO [spark-distcp] spark.default.parallelism:${parallelism_computed}"

  # 计算申请的最大资源
  spark_dynamicAllocation_maxExecutors=400
  default_acquire_cores=4
  default_acquire_mem=8
  ((max_parallism=spark_dynamicAllocation_maxExecutors*4))
  if [[ ${parallelism_computed} -gt ${max_parallism} ]];then
    # 重新计算executor资源
    ((acquire_cores=parallelism_computed/20/spark_dynamicAllocation_maxExecutors))
    # spark.memory.fraction=0.6
    ((compute_mem=(max_partition_bytes*2/3*5+300)/1024/1024/1024))
    ((acquire_mem=acquire_cores*2))
    if ((acquire_mem<compute_mem));then
      ((acquire_mem=compute_mem))
    fi
  fi
  if (( acquire_cores<default_acquire_cores ));then
    ((acquire_cores=default_acquire_cores))
  fi
  if (( acquire_mem<default_acquire_mem ));then
    ((acquire_mem=default_acquire_mem))
  fi
  if (( acquire_mem>30 ));then
    ((acquire_mem=30))
    ((acquire_cores=15))
  fi
  if (( acquire_cores>15));then
    ((acquire_cores=15))
    ((acquire_mem=30))
  fi
  echo "INFO [spark-distcp] acquire_cores: ${acquire_cores}"
  echo "INFO [spark-distcp] acquire_mem: ${acquire_mem}"

  ${SPARK_HOME_IN_SCRIPT}/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue ${spark_yarn_queue} \
  --driver-memory 4G \
  --executor-memory ${acquire_mem}G \
  --executor-cores ${acquire_cores} \
  --conf "spark.default.parallelism=${parallelism_computed}" \
  --conf "spark.sql.files.maxPartitionBytes=${max_partition_bytes}" \
  --conf 'spark.hadoop.hive.exec.dynamic.partition=true' \
  --conf 'spark.hadoop.hive.exec.dynamic.partition.mode=nostrick' \
  --conf 'spark.hadoop.hive.exec.max.dynamic.partitions=2000' \  << EOF

  import java.nio.charset.StandardCharsets
  import java.io.File

  import scala.collection.immutable.Range
  import scala.collection.mutable.ArrayBuffer
  import scala.util.{Failure, Success, Try}

  import org.apache.orc.FileFormatException

  /** concat partition spec in insert-sql */
  def getPartitionsInInsertSql(input: Array[String], firstPartitionStr: String): String = {
    if(input != null && input.size > 0) firstPartitionStr + "," + input.mkString(",") else firstPartitionStr
  }
  /** can not work when partition schema is not in show partitions */
  def getPartitionSql(input: String, without: String): Array[String] = {
    val parts = input.split("/")
    val buffer = new ArrayBuffer[String]()
    for (i <- Range(0, parts.size)) {
      buffer.append(parts(i).split("=")(0))
    }
    buffer.toStream.filter(!without.equals(_)).toArray
  }
  /** get all partitionName */
  def getDynamicPartitions(input: String, withoutPartition: String = null): Array[String] = {
    val pattern = "PARTITIONED BY[\\\s]*\\\([^(]*\\\)".r
    val partitionedByStr = pattern findFirstIn input
    if (!partitionedByStr.contains("\`")) {
      val getParArrStr = "\\\([^(]*\\\)".r
      val partitionArrStr = getParArrStr findFirstIn partitionedByStr.get
      var stream = partitionArrStr.get.stripPrefix("(").stripSuffix(")").split(",").toStream.map(_.trim)
      if (withoutPartition != null) stream = stream.filter(!withoutPartition.equals(_))
      return stream.toArray
    }
    val getDynamicPar = "\`[^\`]*\`".r
    val partitions = getDynamicPar findAllMatchIn partitionedByStr.get
    var stream = partitions.toStream.map(_.toString().stripSuffix("\`").stripPrefix("\`"))
    if (withoutPartition != null) stream = stream.filter(!withoutPartition.equals(_))
    stream.toArray
  }

  // val parallelismComputed = ${file_size_to_partition}.toLong / 1024 / 1024 / 100
  // var parallelism = Try { Math.max(Math.min(${initial_file_num}.toLong, parallelismComputed) ,1) } match {  case Failure(exception) => { println(exception.getMessage); System.exit(8); 0L; } case Success(c) => c  }
  val parallelism = ${parallelism_computed}.toLong
  println(s"srcTbl: ${srcTbl}, ec_table_name: ${ec_table_name}, partition: ${first_partition_field}='${partition_last_value}', parallelism: " + parallelism)

  val df = spark.sql(s"select * from ${srcTbl} where ${first_partition_field}='${partition_last_value}'").drop(s"${first_partition_field}")
  df.createOrReplaceTempView("src")
  val countSrc = Try { spark.sql(s"select count(1) from src").collect().apply(0).get(0).toString.toLong } match {
    case Failure(exception) => {
        if (exception.getCause.isInstanceOf[FileFormatException]) {
          println(exception.getCause.getMessage)
          System.exit(11)
        } else {
          // NoSuchTableException等 应该会在这边被捕获
          exception.printStackTrace
          System.exit(12)
        }
        0L
      }
    case Success(c) => c
   }

  // 将该表set location到backup，同时将该表的hdfs路径设置ec policy；如果已经存在，则check一下分区有没有数据

  try {
    spark.sql(s"show tables in ${db_name} like '${ec_table_name}' ").toDF().collect().apply(0)
  } catch {
    case ex: ArrayIndexOutOfBoundsException =>
      // 表不存在，则创建，并设置hdfs_path的ec policy
      // fixme 后续改成hdfsClient，去设置ec policy
      try {
        spark.sql(s"create table IF NOT EXISTS ${ec_table_name} like ${srcTbl}")
      } catch {
        case ex: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
          // 原始表不存在，则不需要做ec
          System.exit(2)
      }
  }
  // todo 这边后续改成一个spark-server时，不要每次都alter table级别的location,可以先check一下location是否和预期的Location相同，相同则不alter
  Try { spark.sql(s"alter table ${ec_table_name} set LOCATION  '${ec_table_location}' ") } match { case Failure(exception) => { println(exception.getMessage); System.exit(7); } case _ => }
  val parSql = try {
    val partitionStr = spark.sql("show partitions ${srcTbl}").collect().apply(0).get(0).toString
    getPartitionsInInsertSql(getPartitionSql(partitionStr, s"${first_partition_field}"), s"${first_partition_field}='${partition_last_value}'")
    } catch {
      case ex: Exception =>
          val createTableStr = Try { spark.sql(s"show create table ${ec_table_name} ").collect.apply(0).get(0).toString } match { case Failure(exception) => { println(exception.getMessage); System.exit(6); ""; } case Success(c) => c }
          try { getPartitionsInInsertSql(getDynamicPartitions(createTableStr, s"${first_partition_field}"), s"${first_partition_field}='${partition_last_value}'") } catch { case ex: Exception => println(ex.getMessage);System.exit(3)}
    }

  val insertSql = s"insert overwrite table ${ec_table_name} partition (" + parSql.toString + ") select * from src"
  // 这边是否需要set location? 不需要且不能: 多级分区时，这边无法alter，因为新创建的表还没有partition.
  if ( parallelism != ${initial_file_num}.toLong ) {
    println(parallelism.toInt)
    spark.sql(insertSql).repartition(parallelism.toInt)
  } else {
    println(parallelism.toInt)
    spark.sql(insertSql).rdd.coalesce(parallelism.toInt, false)
  }
  val countVal = Try { spark.sql(s"select count(1) from ${ec_table_name} where ${first_partition_field}='${partition_last_value}'").collect().apply(0).get(0).toString.toLong } match { case Failure(exception) => { println(exception.getMessage); System.exit(5); 0L; } case Success(c) => c }
  // todo 这边需要更严格的check
  if (countVal >= 0) {
    // check data quality
    if (countSrc != countVal) {
      // check data quality failed!
      System.exit(9)
    }
    if (countSrc == 0) {
      // 直接查表查不到的数据，如未被元数据管理的数据，hdfs数据存在但是分区元数据不存在
      System.exit(10)
    }
    System.exit(0)
  }
  System.exit(1)
EOF
  # 声明为全局变量
  spark_distcp_res=$?
  local file_num_after_ec=`hdfs dfs -count ${ec_dt_location} | awk -F ' ' '{print $2}'`
  echo "INFO [spark_distcp] ec前文件数量：$initial_file_num"
  echo "INFO [spark_distcp] ec后文件数量：$file_num_after_ec"
  echo "INFO [spark_distcp] spark退出状态：$spark_distcp_res"
  echo "INFO [spark_distcp | End $(date +%Y%m%d-%H:%M:%S)] distcp with spark finished!"
  if [[ $spark_distcp_res -eq 0 ]];then
    return 0
  elif [[ $spark_distcp_res -eq 2 ]]; then
    # 这边的报错应该已经被退出状态12捕获了，但仍然保留该容错代码
    echo "INFO [spark_distcp] 原始表不存在，不需要做ec"
    return 0
  elif [[ $spark_distcp_res -eq 10 ]]; then
    echo "WARN [spark_distcp] 直接查表查不到的数据跳过ec(ec_status将会设置为6)，如未被元数据管理的数据，hdfs数据存在但是分区元数据不存在"
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 6 where id = ${current_record_id};"
    return 0
  elif [[ $spark_distcp_res -eq 11 || $spark_distcp_res -eq 12 ]]; then
    echo "ERROR [spark_distcp] 读取source目录即报错(ec_status将会设置为7)"
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 7 where id = ${current_record_id};"
    return 0
  elif [[ $spark_distcp_res -eq 7 ]]; then
    echo "ERROR [spark_distcp] alter table location failed(ec_status将会设置为27)"
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 27 where id = ${current_record_id};"
    return 0
  elif [[ $spark_distcp_res -eq 9 ]]; then
    echo "ERROR [spark_distcp] insert failed(ec_status将会设置为29)"
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 29 where id = ${current_record_id};"
    return 0
  else
    return 1
  fi
}



echo "*******************"
echo $*

## get params
ACTION_ID=$1
ACTION_SID=$2
TARGET_TABLE=$3
##root.basic_platform.critical
SUBMIT_QUEUE=$4
FILE_MERGE_THRESHOLD=$5
##hdfs://bipcluster/hadoop-yarn/mr/staging
# SPARK_DYNAMIC_PART_ENABLE:0走mapreduce-distcp,1走spark-distcp
SPARK_DYNAMIC_PART_ENABLE=1
STAGING_DIR="hdfs://bipcluster/hadoop-yarn/mr/staging"
SPARK_HOME_IN_SCRIPT="/home/vipshop/platform/spark-3.0.1"


if [[ $# -ne 4 ]];then
	echo "[Info $(date +%Y-%m-%d-%H:%M)]:need 4 args, ACTION_ID ACTION_SID TARGET_TABLE SUBMIT_QUEUE, exit ..."
	exit 1
else
	echo "[Info $(date +%Y-%m-%d-%H:%M)]:action_id=${ACTION_ID} action_sid=${ACTION_SID} tagert_table=${TARGET_TABLE} submit_queue=${SUBMIT_QUEUE}"
fi

if [[ -z "$5" ]];then
  FILE_MERGE_THRESHOLD=104857600
fi

##注意：修改完后需要修改mstasys xml，添加一下的参数
if [[ $ACTION_ID == '' ]] || [[ $ACTION_ID == '' ]];then
	echo "[FAIL]: Please Set   <arg>#{dw.action.id}</arg> "
	echo "[FAIL]: Please Set   <arg>#{dw.action.schedule}</arg> "
	exit 1
fi

SUFFIX=`date +%Y%m%d%H%M%S%s`
FILEDIR="/tmp/setec_temp_dir/${SUFFIX}/${ACTION_ID}_${ACTION_SID}"
mkdir -p ${FILEDIR}

##distcp 时 map个数
MAPSET="-m 500"

i=1
while ((i< 11))
do

        ##如果没有EC的就退出
        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                use demeter;select count(id) from ${TARGET_TABLE} where ec_status =0;
        " > ${FILEDIR}/select_noec.list
        if [[ $? -ne 0 ]];then
                echo "[Fail]: mysql select count(id) id fail ...."
                ##continue
		echo ""
                exit 1
        fi

        noec_count=`tail -n1 ${FILEDIR}/select_noec.list`
        echo "[Info]: noec records count: $noec_count"
        ##echo ""
        if [[ $noec_count -eq 0 ]];then
                echo "[Info]: success exit, there are not ec records."
                exit 0
        fi


        ########
        #  use demeter;
        #  begin;
        #  SET @sele_id :=(select id from ${TARGET_TABLE} where status = 0 limit 1 for update);
        #  update ${TARGET_TABLE} set status = 1 where id = @sele_id;
        #  select id,db_name,tbl_name,location,first_partition,status,path_cluster,dt from ${TARGET_TABLE} where status =1 and id = @sele_id;
        #  commit;
        ########

        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                use demeter;
		SET @sele_id :=(select id from ${TARGET_TABLE} where ec_status = 0 order by file_size desc limit 1);
                begin;
                SET @real_sele_id :=(select id from ${TARGET_TABLE} where id = @sele_id and ec_status = 0 for update);
                update ${TARGET_TABLE} set ec_status = 1 where id = @real_sele_id;
		commit;
                select id,db_name,tbl_name,location,first_partition,ec_status,path_cluster,dt,file_size from ${TARGET_TABLE} where ec_status =1 and id = @real_sele_id;" > ${FILEDIR}/select_location.list

        if [[ $? -ne 0 ]];then
                echo "[Fail]: mysql fetch location id fail .... "
                ##mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                ##       update ${TARGET_TABLE} set ec_status = 0 where id = ${current_record_id};"
		echo ""
                continue
        fi

        echo "cat ${FILEDIR}/select_location.list:"
        cat ${FILEDIR}/select_location.list
        echo ""

        ## 开始EC设置
        select_count=`wc -l ${FILEDIR}/select_location.list | awk '{print $1}'`
        if [[ $select_count -eq 2 ]];then

                current_record=`tail -n1 ${FILEDIR}/select_location.list`
                echo "[Start $(date +%Y%m%d-%H:%M:%S)]: ################# current_record: ${i} ##################"
                echo "[Info]: EC Starting [$current_record]"

                ##获取当前的EC的记录
                current_record_id=`echo $current_record | awk '{print $1}'`
                db_name=`echo $current_record | awk '{print $2}'`
                tbl_name=`echo $current_record | awk '{print $3}'`
                partition_path=`echo $current_record | awk '{print $4}'`
                first_partition=`echo $current_record | awk '{print $5}'`
                ec_status=`echo $current_record | awk '{print $6}'`
                path_cluster=`echo $current_record | awk '{print $7}'`
                record_dt=`echo $current_record | awk '{print $8}'`
                cur_file_size=`echo $current_record | awk '{print $9}'`

		mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
			update ${TARGET_TABLE} set action_id = ${ACTION_ID},action_sid=${ACTION_SID} where id = ${current_record_id};"
		if [[ $? -ne 0 ]];then
			echo "[Fail]: 更新action_id action_sid失败..."
			exit 1
		fi

		# 优先检查ec策略，如果数据库中已经删除过，后面又重新入库，但实际上已经ec的数据，跳过。
    # partition_path=hdfs://bipcluster/bip/hive_warehouse/vipvma.db/dm_vma_user_visit_day/dt=20210106
    check_single_file $partition_path
    if [[ "$?" == "0" ]];then
      continue
    fi


                ## backup目录创建
                EC_POLICY='RS-6-3-1024k'
		backup_path=""
		if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" && $first_partition == "NULL" ]];then
			last_path=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
			pre_path=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F"/${last_path}" '{print $1}'`
			backup_path="hdfs://${path_cluster}/backup/ecdata_other${pre_path}"
		elif [[ $first_partition == "NULL" && ${db_name} != "NULL"  && ${tbl_name} != "NULL" ]];then
			backup_path="hdfs://${path_cluster}/backup/ecdata_tbl/${db_name}"
		else
			backup_path="hdfs://${path_cluster}/backup/ecdata_tbl/${db_name}/${tbl_name}"
		fi

		echo "[Info]: hdfs dfs -mkdir -p $backup_path"
                hdfs dfs -mkdir -p $backup_path
                if [[ $? -ne 0 ]];then
                        echo "[Wrong]: backup path mkdir fail; ${backup_path}"
                        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 0 where id = ${current_record_id};"
                        exit 1
                fi

                ##检查此目录是否存在
                hdfs dfs -test -e ${partition_path}
                if [[ $? -ne 0 ]];then
                        echo "[Warn]: this paritions not found --- ${partition_path}"
                        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                update ${TARGET_TABLE} set ec_status = 3 where id = ${current_record_id};"
			echo ""
                        continue
                fi

                spark_distcp ${db_name}.${tbl_name} ${cur_file_size} ${SUBMIT_QUEUE}
                distcp_res=$?
                if [[ $spark_distcp_res -eq 10 || $spark_distcp_res -eq 11 || $spark_distcp_res -eq 7 || $spark_distcp_res -eq 9  ]];then
                  continue
                fi
                if [[ $distcp_res -eq 0 ]];then
                        #source_stastist=`hdfs dfs -count ${partition_path} | awk '{print $2 "\t" $3}'`
                        #source_path_files=`echo $source_stastist | awk '{print $1}'`
                        #source_path_size=`echo $source_stastist | awk '{print $2}'`

                        #target_stastist=`hdfs dfs -count ${ec_dt_location} | awk '{print $2 "\t" $3}'`
                        #target_path_files=`echo $target_stastist | awk '{print $1}'`
                        #target_path_size=`echo $target_stastist | awk '{print $2}'`

                        #if [[ $source_path_size -eq $target_path_size ]];then
                                echo "[Info]: Distcp success. Staring move setec path ... "

				## update path's pug if distcp success
				###unify_path_pug ${partition_path} ${ec_dt_location} ${first_partition}
				ch_path_pug ${partition_path} ${ec_dt_location}
				if [[ $? -ne 0 ]];then
					echo "[FAIL]: Unify path pug fail ... exit 3"
					mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
						update ${TARGET_TABLE} set ec_status = 3 where id = ${current_record_id}; "
					exit 3
				fi

                                echo "[Info]: hdfs dfs -mv ${partition_path} ${backup_path}"
                                hdfs dfs -mv ${partition_path} ${backup_path}
                                if [[ $? -ne 0 ]];then
                                        echo "[Fail]: move to backup fail ... exit 3 "
                                        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                                update ${TARGET_TABLE} set ec_status = 3 where id = ${current_record_id}; "
                                        exit 4
                                fi

                                echo "[Info]: hdfs dfs -mv ${ec_dt_location} ${partition_path}"
                                hdfs dfs -mv ${ec_dt_location} ${partition_path}
                                if [[ $? -ne 0 ]];then
                                        echo "[Fail]: move setec_path to target_path fail ... exit 4 "
                                        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                                update ${TARGET_TABLE} set ec_status = 3 where id = ${current_record_id};"
                                        exit 5
                                fi

                                ##标志EC策略设置成功
                                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                        update ${TARGET_TABLE} set ec_status = 2 where id = ${current_record_id}; "
                                if [[ $? -ne 0 ]];then
                                        echo "[Fail]: ec set success,but set ec_status fail ... exit 5 "
                                        exit 6
                                fi

				##backup目录
				current_backuppath=""
				if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" && $first_partition == "NULL" ]];then
					last_path=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
					current_backuppath="${backup_path}/${last_path}"
				elif [[ $first_partition == "NULL" && ${db_name} != "NULL"  && ${tbl_name} != "NULL" ]];then
					last_path=`echo "${partition_path}" | awk -F "${path_cluster}" '{print $2}' | awk -F "/" '{print $NF}'`
					current_backuppath="${backup_path}/${last_path}"
				else
					current_backuppath="${backup_path}/${first_partition}"
				fi

				echo "[Info]: Orign data move backuppath: ${current_backuppath}"
				mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                                        update ${TARGET_TABLE} set backup_path = \"${current_backuppath}\" where id = ${current_record_id}; "
                                if [[ $? -ne 0 ]];then
                                        echo "[Fail]: update backup path to mysql fail ... exit 6 "
                                        exit 7
                                fi

				echo "[Info]: Ec success [${current_record}]"
                                echo "[Success $(date +%Y%m%d-%H:%M:%S)]: ############### current_record: ${i} ##################"
                                echo ""
				 ((i++))
                        #fi
                else
                        echo "[Fail]: distcp fail: ${current_record} "
			mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
				update ${TARGET_TABLE} set ec_status = 0 where id = ${current_record_id}; "
			hdfs dfs -rm -r ${ec_dt_location}
                        exit 1
                fi

        else
                echo "[Fail]: not found select location id "
		echo ""
                continue
        fi

done
