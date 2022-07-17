#!/bin/sh

DEMETER_MYSQL="mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 "
ACTION_ID=$1
ACTION_SID=$2
TARGET_TABLE=$3     # cloddata_need_ec_list
QUEUE=$4            # root.basic_platform.critical

if [[ $# -ne 4 ]];then
	echo "[Info $(date +%Y-%m-%d-%H:%M)]:need 4 args, ACTION_ID ACTION_SID TARGET_TABLE QUEUE, exit ..."
	exit 1
else
	echo "[Info $(date +%Y-%m-%d-%H:%M)]:action_id=${ACTION_ID} action_sid=${ACTION_SID} tagert_table=${TARGET_TABLE} queue=${QUEUE}"
fi

SUFFIX=`date +%Y%m%d%H%M%S%s`
FILEDIR="/tmp/ec_count_check_temp_dir/${SUFFIX}/${ACTION_ID}_${ACTION_SID}"
mkdir -p ${FILEDIR}

##distcp 时 map个数
MAPSET="-m 500"

## 在该文件夹下随机找一个file，判断是否是Orc文件
## 如果有一个文件是orc，那么就返回0，否则返回1
check_single_file_is_orc(){
  local target_path=$1
  hdfs dfs -test -d $target_path
  if [[ $? -ne 0 ]];then
    # 文件
    local tail_name=`echo $target_path | awk -F '/'  '{print $NF}' | awk -F '.' '{print $NF}'`
    if [ "$tail_name" = "orc" ];then
      return 0
    else
      return 1
    fi
  else
    # 目录
    local children_array=(`hdfs dfs -ls $target_path | grep hdfs | awk '{print $8}'`)
    for child in ${children_array[@]}
      do
        local file_name=`echo $child | awk -F '/'  '{print $NF}'`
        if [ "$file_name" = "_SUCCESS" ]; then
          # 跳过_SUCCESS文件
          continue
        fi
        check_single_file_is_orc $child
        if [[ $? -ne 0 ]];then
          return 1
        else
          return 0
        fi
    done
    # 如果只有一个_SUCCESS文件，走这个分支
    return 1
  fi
}

## double check, to avoid damage of orc file
check_with_rollback(){
  local bak_path=$1
  local part_path=$2
  local cur_id=$3
  # "hdfs://bipcluster06/bip/hive_warehouse/vipdm.db/dm_log_order_analysis_hm/dt=20210729/log_date=2021-07-29/part-00211-0b4596ab-8cb1-4fb6-b376-de14d5937b39.c000.snappy.orc"

  check_single_file_is_orc $part_path
  if [[ $? -ne 0 ]];then
    # 如果不是orc，则跳过该校验
    echo "this is not an orc file,skip the check of orcfiledump!"
    return 0
  fi

  echo "[start $(date +%Y%m%d-%H:%M:%S)] start  to check the orcfiledump!"
  hive --orcfiledump $part_path >/dev/null
  echo "[end $(date +%Y%m%d-%H:%M:%S)] finish checking the orcfiledump!"
  echo
  if [[ $? -ne 0 ]];then
    # mv backup to source,update mysql and exit
    echo "execute: hdfs dfs -mv $bak_path $part_path"
    hdfs dfs -mv $part_path ${part_path}_setec
    hdfs dfs -mv $bak_path $part_path
    # 这边直接删除临时目录${part_path}_setec
    hdfs dfs -rm -r ${part_path}_setec
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
          update ${TARGET_TABLE} set verify_status = 4 where id = $cur_id;"
    exit 18
  fi
}

## 删掉之前ec创建的表的分区的元数据
handle_post_ec(){
  local ec_table_name=${db_name}.${tbl_name}_setec_with_spark
  if [[ ${first_partition} -ne "NULL" ]];then
    count_res=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${QUEUE}; alter table ${ec_table_name} drop partition (${first_partition})"`
    if [[ $? -eq 0 ]];then
      echo "[Info]successfully drop partition ${first_partition} of table $ec_table_name"
    else
      echo "[Warn]drop partition ${first_partition} of table $ec_table_name failed !"
    fi
  fi
}

i=0
while ((i< 10))
do
    ((i++))

    echo ""
    ##如果没有需要count check的就退出
    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
        use demeter;select count(id) from ${TARGET_TABLE} where ec_status =2 and verify_status =0; " > ${FILEDIR}/select_nocheck.list
    if [[ $? -ne 0 ]];then
        echo "[Fail]: mysql select count(id) id fail ...."
        echo ""
        exit 1
    fi

    nocheck_count=`tail -n1 ${FILEDIR}/select_nocheck.list`
    if [[ $nocheck_count -eq 0 ]];then
        echo "[Info]: success exit, no partition needs count check."
        exit 0
    fi

    # mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    #     SELECT t1.id
    #     FROM ${TARGET_TABLE} AS t1 JOIN (SELECT ROUND( RAND () * ((SELECT MAX(id) FROM ${TARGET_TABLE})-(SELECT MIN(id) FROM ${TARGET_TABLE}))+(SELECT MIN(id) FROM ${TARGET_TABLE})) AS id) AS t2
    #     WHERE t1.id >= t2.id AND ec_status =2 AND verify_status =0
    #     ORDER BY t1.id LIMIT 1;"

    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
        use demeter;
        SET @sele_id :=(select id from ${TARGET_TABLE} where ec_status =2 and verify_status =0 and backup_path <>'NULL' limit 1 for update);
        begin;
        SET @real_sele_id :=(select id from ${TARGET_TABLE} where id = @sele_id for update);
        update ${TARGET_TABLE} set verify_status = 1 where id = @real_sele_id;
        commit;
        select id,db_name,tbl_name,location,first_partition,verify_status,path_cluster,dt,backup_path from ${TARGET_TABLE} where ec_status =2 and verify_status =1 and id = @real_sele_id;" > ${FILEDIR}/select_location.list


#    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
#        use demeter;
#
#        select id,db_name,tbl_name,location,first_partition,verify_status,path_cluster,dt,backup_path from ${TARGET_TABLE} id = '66666';" > ${FILEDIR}/select_location.list
#

    if [[ $? -ne 0 ]];then
        echo "[Fail]: mysql fetch location id fail ...., exit 12"
        echo ""
        exit 12
    fi

    ##
    select_count=`wc -l ${FILEDIR}/select_location.list | awk '{print $1}'`
    if [[ $select_count -eq 2 ]];then

        current_record=`tail -n1 ${FILEDIR}/select_location.list`

        echo "[Start $(date +%Y%m%d-%H:%M:%S)]: ################# current_record: ${i} ##################"
        echo "[Info]: current_record: $current_record"

        ##获取当前的EC的记录
        current_record_id=`echo $current_record | awk '{print $1}'`
        db_name=`echo $current_record | awk '{print $2}'`
        tbl_name=`echo $current_record | awk '{print $3}'`
        partition_path=`echo $current_record | awk '{print $4}'`
        first_partition=`echo $current_record | awk '{print $5}'`
        first_partition_sql=`echo $first_partition | sed "s/=/='/g" | sed "s/$/'/"`
        verify_status=`echo $current_record | awk '{print $6}'`
        path_cluster=`echo $current_record | awk '{print $7}'`
        record_dt=`echo $current_record | awk '{print $8}'`
        backup_path=`echo $current_record | awk '{print $9}'`

        # 删除通过spark-distcp重写文件创建的临时表的分区元数据
        handle_post_ec

        ## 增加actionId和actionSID
        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
			    update ${TARGET_TABLE} set verify_action_id = ${ACTION_ID},verify_action_sid=${ACTION_SID} where id = ${current_record_id};"
		    if [[ $? -ne 0 ]];then
		    	echo "[Fail]: 更新action_id action_sid失败..."
		    	exit 1
		    fi

		    ## 判断source路径是否存在，如果不存在，直接删除backup
		    hdfs dfs -test -e $partition_path
		    if [[ $? -ne 0 ]];then
		      # 不存在
		      echo "[Info]source dir is not exist,remove the backup:$backup_path"
		      mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                update ${TARGET_TABLE} set verify_status = 2 where id = ${current_record_id}; "
          hdfs dfs -rm -r $backup_path
          continue
		    fi

        ## 判断是否是 非表数据。如果是，则不需要 count check
        if [[ ${db_name} == "NULL" && ${tbl_name} == "NULL" ]];then
            echo "[Info]: Do not need count check"
            ## 更新 mysql
            mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                update ${TARGET_TABLE} set verify_status = 2 where id = ${current_record_id}; "
            if [[ $? -ne 0 ]];then
                echo "[Fail]: Do not need count check, but set verify_status fail ... exit 17 "
                exit 17
            fi
            echo "[Success $(date +%Y%m%d-%H:%M:%S)]: ############### current_record: ${i} ##################"

            ## 处理下一条数据
            continue
        fi

        ## 判断是否是 非分区表
        if [[ ${first_partition} == "NULL" ]];then
            count_res=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${QUEUE}; select count(1) from ${db_name}.${tbl_name}"`
            echo "[Info]: read count : $count_res"

            ## 如果 count_res <= 0，则将backup目录中的数据 移回 原目录
            if [[ $count_res -le 0 ]];then
                echo "[Wrong]: Verify table ${db_name}.${tbl_name} fail, now moving data from backup to source"

                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                    update ${TARGET_TABLE} set verify_status = 23 where id = ${current_record_id};"

                hdfs dfs -test -e $backup_path
                if [[ $? -ne 0 ]];then
                    echo "[Fail]: Verify table ${db_name}.${tbl_name} fail, trying moving data from backup to source, but there is no data to move ... exit 15 "
                    echo ""
                    exit 15
                fi

                hdfs dfs -test -e $partition_path
                if [[ $? -ne 0 ]];then
                    echo "[Fail]: Verify table ${db_name}.${tbl_name} fail, source data not found, ... exit 15 "
                    echo ""
                    exit 15
                fi

                hdfs dfs -mv $partition_path "${partition_path}_setec"
                hdfs dfs -mv $backup_path $partition_path
                if [[ $? -ne 0 ]];then
                    echo "[Fail]: Verify table ${db_name}.${tbl_name} fail, trying moving data from backup to source, but fail during moving ... exit 16 "
                    echo ""
                    exit 16
                fi

                echo "[Fail]: Verify table ${db_name}.${tbl_name} fail, but successfully move data from backup to source ... exit 8"
                echo ""
                exit 8
            fi

            ## count_res 没有问题，就删除backup里的数据
            hdfs dfs -test -e $backup_path
            if [[ $? -ne 0 ]];then
                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                    update ${TARGET_TABLE} set verify_status = 20 where id = ${current_record_id};"
                echo "[Fail]: Count check success, but there is no data to move ... exit 10 "
                echo ""
                exit 10
            fi

            # todo 非分区表,double check,防止ec文件损坏
            check_with_rollback $backup_path $partition_path $current_record_id

            hdfs dfs -rm -r $backup_path
            if [[ $? -ne 0 ]];then
                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                    update ${TARGET_TABLE} set verify_status = 21 where id = ${current_record_id};"
                echo "[Fail]: Count check success, but moving data from backup to .Trash fail ... exit 11 "
                echo ""
                exit 11
            fi

            ## 更新 mysql
            mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                update ${TARGET_TABLE} set verify_status = 2 where id = ${current_record_id}; "

            echo "[Info]: Count check success ${current_record}"
            echo "[Success $(date +%Y%m%d-%H:%M:%S)]: ############### current_record: ${i} ##################"

            ## 处理下一条数据
            continue
        fi

        ## 除去以上 两种情况外，只剩下分区表
        ##数据验证，可能oom or split to many map
        ##hive -e "set mapreduce.job.queuename=${QUEUE};select count(1) from ${db}.${tbl} where ${partition_name}"
        inc_threthold_sql="set mapreduce.input.fileinputformat.split.minsize=256000000;set mapreduce.input.fileinputformat.split.maxsize=512000000"
        count_res=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${QUEUE};${inc_threthold_sql};select count(1) from ${db_name}.${tbl_name} where ${first_partition_sql}"`
        # todo 这边，如果是两级分区，那么$?!=0吗？？？
        count_exit_code=$?
        echo "[Info]hive-count_exit_code:${count_exit_code}"
        if [[ $count_exit_code -ne 0 ]];then
            if [[ $count_exit_code -eq 201 ]];then
              # Job Submission failed
              mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                   update ${TARGET_TABLE} set verify_status = 0 where id = ${current_record_id};"
              exit 26
            fi

            echo "[Info]: Verify data by first partition fail, trying secondary partition "

            ##生成二级分区
            second_list="${FILEDIR}/${first_partition}.list"
            hive -e "set hive.compute.query.using.stats=false; show partitions ${db_name}.${tbl_name} partition (${first_partition_sql});" | grep -P "^.*?/.*?(?=/)|^.*?/.*?(?=$)" -o > $second_list
            if [[ $? -ne 0 ]];then
                echo "[Wrong]: Get secondary partitions fail, exit 6"
                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                   update ${TARGET_TABLE} set verify_status = 6 where id = ${current_record_id};"
                exit 6
            fi

            ##对每一个二级分区进行校验
            while read second_partition
            do
                second_partition_sql=`echo $second_partition | sed "s/\//' and /g" | sed "s/=/='/g" | sed "s/$/'/"`
                second_count_res=`hive -e "set hive.compute.query.using.stats=false;set mapreduce.job.queuename=${QUEUE};select count(1) from ${db_name}.${tbl_name} where ${second_partition_sql}"`
                if [[ $? -ne 0 ]];then
                    echo "[Wrong]: Verify second partition $second_partition data fail during counting by HIVE, exit 7"
                    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                        update ${TARGET_TABLE} set verify_status = 7 where id = ${current_record_id};"
                    exit 7
                fi

                echo "[Info]: second partition $second_partition read count : $second_count_res"
                if [[ $second_count_res -le 0 ]];then
                    echo "[Wrong]: Verify second partition $second_partition data fail, now moving data from backup to source"

                    mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                        update ${TARGET_TABLE} set verify_status = 25 where id = ${current_record_id};"

                    hdfs dfs -test -e $backup_path
                    if [[ $? -ne 0 ]];then
                        echo "[Fail]: Verify second partition $second_partition data fail, trying moving data from backup to source, but there is no data to move ... exit 15 "
                        echo ""
                        exit 15
                    fi

                    hdfs dfs -mv $partition_path "${partition_path}_setec"
                    hdfs dfs -mv $backup_path $partition_path
                    if [[ $? -ne 0 ]];then
                        echo "[Fail]: Verify second partition $second_partition data fail, trying moving data from backup to source, but fail during moving ... exit 16 "
                        echo ""
                        exit 16
                    fi

                    date=`date +%Y%m%d`
                    check_failed_path=`echo ${backup_path} | sed "s/backup/backup\/check_failed\/dt=${date}/"`
                    check_failed_fatherpath=`echo ${check_failed_path} | grep -o -P "^.*(?=/)"`
                    hdfs dfs -mkdir -p $check_failed_fatherpath

                    hdfs dfs -mv  "${partition_path}_setec" $check_failed_path
                    echo "[Fail]: Verify second partition $second_partition data fail, but successfully move data from backup to source ... exit 8"
                    echo ""
                    exit 8
                fi

                # fixme 对每个二级分区,double check，防止ec数据损坏
                check_with_rollback $backup_path $partition_path $current_record_id

            done < ${second_list}

        else

            echo "[Info]: read count : $count_res"
            if [[ $count_res -le 0 ]];then
                echo "[Wrong]: Verify first partition $first_partition data fail, exit 5"
                mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                    update ${TARGET_TABLE} set verify_status = 5 where id = ${current_record_id};"
                exit 5
            fi

            # fixme 分区表,且为一级分区,double check,防止ec数据损坏
            check_with_rollback $backup_path $partition_path $current_record_id
        fi

        ##删除backup里的数据
        hdfs dfs -test -e $backup_path
        if [[ $? -ne 0 ]];then
            mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                update ${TARGET_TABLE} set verify_status = 10 where id = ${current_record_id};"
            echo "[Fail]: Count check success, but there is no data to move ... exit 10 "
            echo ""
            exit 10
        fi

        date=`date +%Y%m%d`
        remove_path=`echo ${backup_path} | sed "s/backup/backup\/ec_to_be_deleted\/dt=${date}/"`
        remove_fatherpath=`echo ${remove_path} | grep -o -P "^.*(?=/)"`
        hdfs dfs -mkdir -p $remove_fatherpath

        ## todo 删除backup之前,再次检查ec数据是否损坏

        hdfs dfs -mv $backup_path $remove_path
        if [[ $? -ne 0 ]];then
            mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
                update ${TARGET_TABLE} set verify_status = 11 where id = ${current_record_id};"
            echo "[Fail]: Count check success, but moving data from backup to .Trash fail ... exit 11 "
            echo ""
            exit 11
        fi

        ##标志 一级分区 或 二级分区 成功通过 count check
        mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
            update ${TARGET_TABLE} set verify_status = 2 where id = ${current_record_id}; "
        if [[ $? -ne 0 ]];then
            echo "[Fail]: Count check success, but set verify_status fail ... exit 9 "
            exit 9
        fi

        echo "[Info]: Count check success ${current_record}"
        echo "[Success $(date +%Y%m%d-%H:%M:%S)]: ############### current_record: ${i} ##################"

    else
        echo "[Fail]: not found select location id, exit 14"
        echo ""
        ##exit 14
        continue
    fi

done