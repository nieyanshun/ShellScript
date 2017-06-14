#!/bin/bash
#author nieyanshun
#date 2017/06/13
#comment: mysql data record delete util

date_time="2016-07-11"
db_table="bdp_user_album_play_day"

if [ -n "$1" ];then
    echo "start time $1"
    date_time=$1
fi

if [ -n "$2" ];then
    echo "delete table $2 "
    table=$2
fi


end_time=`date -d "2017-05-01" +%s`

db_host="127.0.0.1"
db_user="nieyanshun"
db_password="pwd"
db_database="db_name"


 log(){
    echo  $1 >>detail.log
}
while true

do
    t1=`date -d "$date_time" +%s`
    if [ $t1 -lt $end_time ] ;then
        log  "delete record... date_time=$date_time ,table=$db_table"

        delete_sql="delete from $db_table where date_time = '$date_time' ;"

        mysql -h$db_host -u$db_user -p$db_password $db_database -A -N -e "delete from $db_table where date_time = '$date_time';"

        date_time=`date -d "$date_time 1 days" +"%Y-%m-%d"`

        log "delete $date_time record finished... sql: $delete_sql"
	
    else
	log "Delete record job finished..."
        break
    fi
  done

