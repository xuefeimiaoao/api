#!/bin/bash
ACTION_ID=$1
ACTION_SID=$2
TARGET_TABLE="potential_failed_ec_list"
cluster_name=$3
SUFFIX=`date +%Y%m%d%H%M%S%s`
ec_blk_tmp_dir="/tmp/ec_black_list_check_temp_dir/${SUFFIX}/${ACTION_ID}_${ACTION_SID}"
mkdir -p $ec_blk_tmp_dir

check_files(){
    local par_path=$1

    hdfs dfs -test -d $par_path
    if [[ $? -ne 0 ]];then
      # 文件
      initial_ec_policy=`hdfs ec -getPolicy -path $par_path`
      echo "当前文件[${par_path}]的ec策略，为:$initial_ec_policy"
      # fixme 后续如果需要修改ecPolicy，这边也需要更改
      if [[ "$initial_ec_policy" == "RS-6-3-1024k" ]];then
        echo "当前文件[$par_path]已经做过Ec！"
	((ec_files++))
      else
        # 未ec
	echo "当前文件[$par_path]未ec！"
	((non_ec_files++))
      fi

    else
      # 目录
      file_array=(`hdfs dfs -ls $par_path | grep hdfs | awk '{print $8}'`)
      for fd in ${file_array[@]}
        do
          check_files $fd
      done

    fi
}

check_files_with_result(){
  ec_files=0
  non_ec_files=0
  check_files $1

  if (( ec_files + non_ec_files == 0 ));then
    echo "[ Info ] 该目录下没有需要ec的文件"
        	mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
		update ${TARGET_TABLE} set check_part_ec = 2  where id = ${current_id};
	"
  elif (( ec_files == 0 ));then
    echo "[ Info ] 所有文件都没有ec"
        	mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
		update ${TARGET_TABLE} set check_part_ec = 3  where id = ${current_id};
	"
  elif (( non_ec_files == 0 ));then
    echo "[ Info ] 所有文件都已经ec~~"
        	mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
		update ${TARGET_TABLE} set check_part_ec = 4  where id = ${current_id};
	"
  else
    echo "[ WARN ] 该目录[$1]存在部分ec的情况！！！"
        	mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
		update ${TARGET_TABLE} set check_part_ec = 5  where id = ${current_id};
	"
  fi
}

check_partitions_in_blacklist(){
  local res=`mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
	use demeter;
	select count(1) from ${TARGET_TABLE} where check_part_ec = 0 and path_cluster='${cluster_name}';
  "`
  local tmp_count=`echo $res | awk '{print $2}'`
  if [[ $tmp_count -eq 0 ]];then
	exit 0
  fi

  mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
	use demeter;
        SET @sele_id :=(select id from ${TARGET_TABLE} where path_cluster='${cluster_name}' and check_part_ec=0  limit 1 for update);
        begin;
        SET @real_sele_id :=(select id from ${TARGET_TABLE} where id = @sele_id for update);
        update ${TARGET_TABLE} set check_part_ec = 1 where id = @real_sele_id;
        commit;
        select id,location from ${TARGET_TABLE} where check_part_ec = 1 and id = @real_sele_id;

  " > $ec_blk_tmp_dir/select.list

  local select_count=`wc -l ${ec_blk_tmp_dir}/select.list | awk -F ' ' '{print $1}'`
  if [[ $select_count -eq 2 ]];then
	local current_record=`tail -n1 ${ec_blk_tmp_dir}/select.list`
	local current_id=`echo $current_record | awk -F ' ' '{print $1}'`
	local current_loc=`echo $current_record | awk -F ' '  '{print $2}'`
	echo "######################current_record:[$current_record]#######################"
	echo "######################current_id:[$current_id]#######################"
        echo "######################current_loc:[$current_loc]#######################"
	echo
    	check_files_with_result $current_loc
    	echo "ec_files:$ec_files"
    	echo "non_ec:$non_ec_files"
   	echo -e  "\n\n\n"

  fi
}

i=0
while (( i<10 ))
do
	((i++))
	check_partitions_in_blacklist
done