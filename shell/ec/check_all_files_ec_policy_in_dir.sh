#!/bin/bash

check_files(){
    local par_path=$1

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
        # 未ec
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
            ((non_ec_files++))
          elif [[ "$res" == "0" ]]; then
            ((ec_files++))
          fi
      done

    fi
}

check_files_with_result(){
  ec_files=0
  non_ec_files=0
  check_files $1

  if (( ec_files + non_ec_files == 0 ));then
    echo "该目录下没有需要ec的文件"
  elif (( ec_files == 0 ));then
    echo "所有文件都没有ec"
  elif (( non_ec_files == 0 ));then
    echo "所有文件都已经ec~~"
  else
    echo "[WARN]该目录[$1]存在部分ec的情况！！！"
  fi
}

check_partitions_in_blacklist(){
  mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    select location from potential_failed_ec_list
  " > tmp_check_partitions_ec_policy.file

  cat tmp_check_partitions_ec_policy.file | grep -v location | while read cur_location
  do
    check_files_with_result $cur_location
  done
}
