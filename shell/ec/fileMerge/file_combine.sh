#!/bin/bash
SPARK_HOME_IN_SCRIPT="/home/vipshop/platform/spark-3.0.1"
YARN_QUEUE="root.basic_platform.critical"
FILE_MERGE_THRESHOLD=104857600
export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

parse_fine_grained_partitions(){
  local db_name=$1
  local tbl_name=$2
  show_partition_arr=(`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${YARN_QUEUE};show partitions ${db_name}.${tbl_name}" `)
  # echo ${show_partition_arr[0]}
  # dim_group=vendor/dt=20200819
  table_location=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${YARN_QUEUE};desc formatted ${db_name}.${tbl_name}" | grep Location`
}

file_combine(){
  echo "INFO [start $(date +%Y%m%d-%H:%M:%S)] start to combine files with spark!"
  local db_name=$1
  local tbl_name=$2
  local srcTbl=$1.$2
  local combine_mid_table_name=${srcTbl}__combine_with_spark
  local file_size_to_partition='"'$3'"'
  local file_size_to_partition_num=$3
  local partition_path=$4
  local first_partition=$5
  local dt_partition=$6
  local partition_str_in_meta=$7
  local path_cluster=$8
  local initial_file_num=`hdfs dfs -count ${partition_path} | awk -F ' ' '{print $2}'`
  echo "INFO [file_combine] initial_file_num:$initial_file_num"

  local combine_mid_table_location=""

  local combine_mid_table_location="hdfs://${path_cluster}/backup/file_combine_tbl/${db_name}/${tbl_name}"
  # 此处声明为全局变量，便于后续获取
  combine_mid_dt_location=${combine_mid_table_location}/${partition_str_in_meta}
  local first_partition_field=`echo ${first_partition} | awk -F '=' '{print $1}'`
  local partition_last_value=`echo ${first_partition} | awk -F '=' '{print $2}'`

  local dt_partition_field=`echo ${dt_partition} | awk -F '=' '{print $1}'`
  local dt_partition_last_value=`echo ${dt_partition} | awk -F '=' '{print $2}'`

  # hdfs dfs -test -e hdfs://bipcluster04/backup/ecdata_tbl/bip/hive_warehouse/hive_vipudp.db/ads_user_behavior_track_ds_setec_by_xuefei
  hdfs dfs -test -e ${combine_mid_table_location}
  if [[ $? -ne 0 ]];then
    hdfs dfs -mkdir -p ${combine_mid_table_location}
  fi

  # 计算最大文件的大小
  local num_of_partitions=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${YARN_QUEUE};show partitions ${srcTbl}" | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  ((num_of_partitions=num_of_partitions-1))
  local count_cmd="hdfs dfs -count ${partition_path}"
#  if [[ "${count_cmd:0-1}" -eq "/" ]];then
#    # 删掉末尾的/;${vvv:0:${#vvv}-1}
#    local count_cmd=${count_cmd:0:${#count_cmd}-1}
#  fi
  local count_cmd="${count_cmd}/*"

#  if ((num_of_partitions>0));then
#    for i in `seq 1 ${num_of_partitions}`
#      do
#        local count_cmd="${count_cmd}/*"
#    done
#  fi
  echo "INFO [file_combine] count_cmd:${count_cmd}"
  local max_partition_bytes=`${count_cmd} | awk '{print $3}' | sort -nr | head -n1`
  echo "INFO [file_combine] max_partition_bytes:${max_partition_bytes}"
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
  echo "INFO [file_combine] spark.default.parallelism:${parallelism_computed}"

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
  local acquire_mem_str=${acquire_mem}G
  echo "INFO [file_combine] acquire_cores: ${acquire_cores}"
  echo "INFO [file_combine] acquire_mem: ${acquire_mem_str}"

  ${SPARK_HOME_IN_SCRIPT}/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue ${YARN_QUEUE} \
  --driver-memory 4G \
  --executor-memory ${acquire_mem_str} \
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
  println(s"srcTbl: ${srcTbl}, combine_mid_table_name: ${combine_mid_table_name}, partition: ${first_partition_field}='${partition_last_value}', parallelism: " + parallelism)

  val df = spark.sql(s"select * from ${srcTbl} where ${first_partition_field}='${partition_last_value}' and ${dt_partition_field}='${dt_partition_last_value}' ").drop(s"${first_partition_field}").drop(s"${dt_partition_field}")
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
    spark.sql(s"show tables in ${db_name} like '${combine_mid_table_name}' ").toDF().collect().apply(0)
  } catch {
    case ex: ArrayIndexOutOfBoundsException =>
      // 表不存在，则创建，并设置hdfs_path的ec policy
      // fixme 后续改成hdfsClient，去设置ec policy
      try {
        spark.sql(s"create table IF NOT EXISTS ${combine_mid_table_name} like ${srcTbl}")
      } catch {
        case ex: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
          // 原始表不存在，则不需要做ec
          System.exit(2)
      }
  }
  // todo 这边后续改成一个spark-server时，不要每次都alter table级别的location,可以先check一下location是否和预期的Location相同，相同则不alter
  Try { spark.sql(s"alter table ${combine_mid_table_name} set LOCATION  '${combine_mid_table_location}' ") } match { case Failure(exception) => { println(exception.getMessage); System.exit(7); } case _ => }
  val parSql = s"${first_partition_field}='${partition_last_value}',${dt_partition_field}='${dt_partition_last_value}'"

  val insertSql = s"insert overwrite table ${combine_mid_table_name} partition (" + parSql.toString + ") select * from src"
  // 这边是否需要set location? 不需要且不能: 多级分区时，这边无法alter，因为新创建的表还没有partition.
  if ( parallelism != ${initial_file_num}.toLong ) {
    println(parallelism.toInt)
    spark.sql(insertSql).repartition(parallelism.toInt)
  } else {
    println(parallelism.toInt)
    spark.sql(insertSql).rdd.coalesce(parallelism.toInt, false)
  }
  val countVal = Try { spark.sql(s"select count(1) from ${combine_mid_table_name} where ${first_partition_field}='${partition_last_value}' and ${dt_partition_field}='${dt_partition_last_value}'").collect().apply(0).get(0).toString.toLong } match { case Failure(exception) => { println(exception.getMessage); System.exit(5); 0L; } case Success(c) => c }
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
  file_combine_res=$?
  local file_num_after_ec=`hdfs dfs -count ${combine_mid_dt_location} | awk -F ' ' '{print $2}'`
  echo "INFO [file_combine] combine前文件数量：$initial_file_num"
  echo "INFO [file_combine] combine后文件数量：$file_num_after_ec"
  echo "INFO [file_combine] spark退出状态：$file_combine_res"
  echo "INFO [file_combine|End $(date +%Y%m%d-%H:%M:%S)] file_combine with spark finished!"
  if [[ $file_combine_res -eq 0 ]];then
    return 0
  elif [[ $file_combine_res -eq 2 ]]; then
    # 这边的报错应该已经被退出状态12捕获了，但仍然保留该容错代码
    echo "INFO [file_combine] 原始表不存在，不需要做ec"
    return 0
  elif [[ $file_combine_res -eq 10 ]]; then
    echo "WARN [file_combine] 直接查表查不到的数据跳过ec(ec_status将会设置为6)，如未被元数据管理的数据，hdfs数据存在但是分区元数据不存在"
    return 0
  elif [[ $file_combine_res -eq 11 || $file_combine_res -eq 12 ]]; then
    echo "ERROR [file_combine] 读取source目录即报错(ec_status将会设置为7)"
    return 0
  elif [[ $file_combine_res -eq 7 ]]; then
    echo "ERROR [file_combine] alter table location failed(ec_status将会设置为27)"
    return 0
  elif [[ $file_combine_res -eq 9 ]]; then
    echo "ERROR [file_combine] insert failed(ec_status将会设置为29)"
    return 0
  else
    return 1
  fi
}

schedule_9_month_part(){
  ## 对于 dim_group=vendor_yard_hm/dt=20210912 这种分区，可以直接拼接出path，可以考虑load。通过元数据获取数据类型。
  local path_cluster=`echo $3 | awk -F 'hdfs://' '{print $2}' | awk -F '/' '{print $1}'`
  local db_name=$1
  local tbl_name=$2
  for i in ${show_partition_arr[@]}
    do
      local cur_partition=$i
      if [[ "${cur_partition}" == *202109* ]];then
        if [[ "${cur_partition}" == *20210913* || "${cur_partition}" == *20210914* ]];then
          # 去掉20210913和20210914
          continue
        fi

        echo "cur_partition:${cur_partition}"

        # fixme 后续改成通用的
        # 对于cube
        local dt_partition=`echo ${cur_partition} | awk -F '/' '{print $NF}'`
        local first_partition=`echo ${cur_partition} | awk -F '/' '{print $1}'`
        echo "first_partition:${first_partition}"
        # todo 中间状态存到mysql
        # todo 调度的最细粒度是dt分区
        # todo 如果dt不是第一级分区，那么fine_combine有没有问题
        local cur_file_size=`hdfs dfs -count $3/${cur_partition} | awk '{print $3}'`
        echo "cur_file_size:${cur_file_size}"
        file_combine $1 $2 ${cur_file_size} $3/${cur_partition} ${first_partition} ${dt_partition} ${cur_partition} ${path_cluster}
        if [[ $? -eq 0 ]];then
          # todo 这边保证原子性
          local to_remove_table_location="hdfs://${path_cluster}/backup/file_combine_to_remove_tbl/${db_name}/${tbl_name}"
          echo "hdfs dfs -mkdir -p ${to_remove_table_location}/${first_partition}"
          hdfs dfs -mkdir -p ${to_remove_table_location}/${first_partition}
          echo "hdfs dfs -mv $3/${cur_partition} ${to_remove_table_location}/${first_partition}"
          hdfs dfs -mv $3/${cur_partition} ${to_remove_table_location}/${first_partition}
          echo "hdfs dfs -mv ${combine_mid_dt_location} $3/${first_partition}"
          hdfs dfs -mv ${combine_mid_dt_location} $3/${first_partition}
        fi
      fi
  done
}

# parse_fine_grained_partitions ${db_name} ${tbl_name}
# schedule_9_month_part ${db_name} ${tbl_name} ${cur_file_size} ${table_location}
# file_combine ${db_name} ${tbl_name} ${cur_file_size} ${partition_path}

# todo drop mid_table;
cube_table_location="hdfs://ssdcluster/bip/hive_warehouse/vipcube.db/ads_vendor_brand_yard_act_goods"
parse_fine_grained_partitions "vipcube" "ads_vendor_brand_yard_act_goods"
schedule_9_month_part "vipcube" "ads_vendor_brand_yard_act_goods" ${cube_table_location}