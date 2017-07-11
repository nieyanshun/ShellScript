#!/bin/bash
#################################################################################
# Target Table : 
# Source Table : 
# Interface Name: 
# Refresh Frequency: per day 每日处理
# Version Info : 1.1
# 修改人   修改时间       修改原因 
# nieyanshun   2017-01-11    流程修复
#
#################################################################################
source /etc/profile
source $HOME/.bashrc
cd $(dirname $(readlink -f $0))

dateValue=`date -d 'yesterday' +%Y%m%d`

dateInFormat=`date -d 'yesterday' +%Y-%m-%d`


T_BDP_ALBUM=test.test_album
T_BDP_VIDEO=test.test_video
#每日播放记录表
T_USER_PLAY_DAY=test.user_play_day
#用户订单信息
T_USER_ORDER=test.user_order


# 每日专辑视频信息表
T_ALBUM_INFO=nys_album_video_info_batch
#每种类型流量统计表
T_FLOW_INFO=nys_album_flow_info_batch
#流量统计明细表
T_MID_FLOW_ITEM=nys_album_flow_item_batch
#流量统计汇总表
T_MID_FLOW_WITH_MEMSHIP=nys_album_flow_with_memship_batch
#会员信息表
T_MID_MEMSHIP=nys_album_memship_perday_batch
#sqoop导出表
T_ALBUM_FLOW_SUM=nys_album_flow_sum_batch


function log(){
    echo "$1 . time :`date "+%Y-%m-%d %H:%M:%S"`"
}

function createTables(){
hive <<EOF
    USE temp;
    DROP table IF EXISTS temp.$T_ALBUM_INFO;
    CREATE TABLE IF NOT EXISTS temp.$T_ALBUM_INFO(
     pid string,
     p_name string,
     p_category int,
     p_delete int ,
     p_status int ,
     vid string,
     v_on_time string ,
     v_video_type int ,
     v_is_pay int , 
     v_pay_platform string comment '',
     country string)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

    DROP table IF EXISTS temp.$T_FLOW_INFO;
    CREATE TABLE IF NOT EXISTS temp.$T_FLOW_INFO(
     pid string,
     vid string,
     vv int,
     cv int,
     vvuv int ,
     cvuv int ,
     time_long int ,
     type string comment '类型',
     mem_type string,
     uid string,
     terminal string,
     sub_terminal string)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
    
    DROP table IF EXISTS temp.$T_MID_FLOW_ITEM;
    CREATE TABLE IF NOT EXISTS temp.$T_MID_FLOW_ITEM(
    pid string,
    vid string,
    video_type string,
    pay_platform string ,
    is_pay int , 
    uid string,
    product string,
    p2 string,
    vv int,
    cv int,
    yx_cv int,
    pt int,
    vvuv int,
    cvuv int,
    yx_cvuv int,
    yx_pt int)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

    DROP table IF EXISTS temp.$T_MID_MEMSHIP;
    CREATE TABLE IF NOT EXISTS temp.$T_MID_MEMSHIP(
    userid string,
    mem_type string)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

    DROP table IF EXISTS temp.$T_MID_FLOW_WITH_MEMSHIP;
    CREATE TABLE IF NOT EXISTS temp.$T_MID_FLOW_WITH_MEMSHIP(
    pid string,
    vid string,
    is_zp int,
    vv int ,
    cv int , 
    yx_cv int,
    pt int,
    vvuv int,
    cvuv int,
    yx_cvuv int,
    yx_pt int,
    uid string,
    product string,
    p2 string,
    is_pay int,
    mem_type string)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
    
    DROP table IF EXISTS temp.$T_ALBUM_FLOW_SUM;
    CREATE TABLE IF NOT EXISTS temp.$T_ALBUM_FLOW_SUM(
    pid string,
    vv int ,
    cv int , 
    vvuv int,
    cvuv int,
    ttime int,
    type string,    
    mem_type string,
    terminal string,
    sub_terminal string,
    date_time string)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' 
    LINES TERMINATED BY '\n'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
    quit;
EOF
}


function initAlbumInfo(){
hive<<EOF
        INSERT INTO temp.$T_ALBUM_INFO  PARTITION(dt=$dateValue)
        select
                 a.pid,case when(a.cn_name='' or a.cn_name='-') then a.en_name else a.cn_name end,a.category as p_category
                 ,a.is_delete as p_delete,a.status as p_status
                 ,b.vid,b.on_time as v_on_time,b.video_type as b_video_type
                 ,b.is_pay as v_is_pay, b.pay_platform as v_pay_platform
                 ,a.cc
        from $T_BDP_ALBUM as a 
                join $T_BDP_VIDEO as b
                on (a.pid=b.pid 
                and a.dt=$dateValue and b.dt=$dateValue
                and b.cc=a.cc    
                and a.is_pay='1');
                quit;
EOF

}
function initFlowItems(){
    hive <<EOF
    insert into temp.$T_MID_FLOW_ITEM PARTITION(dt=$dateValue)
      SELECT 
          t1.pid,t1.vid
          ,max(t1.v_video_type) as v_video_type
          ,max(t1.v_pay_platform) as v_pay_platform
          ,max(t1.v_is_pay) as v_is_pay,t2.uid,t2.product,t2.p2 
          ,sum(case when (init+play+time) >0 then 1 else 0 end) vv
          ,sum(case when (play+time) > 0 then 1 else 0 end) cv
          ,sum(case when (play+time) > 0 and pt>360 then 1 else 0 end) yx_cv  
          ,sum( COALESCE((case when (play+time)>0 then pt else 0 end),0)) pt
          ,count(distinct case when (init+play+time)>0 then uid end) vvuv
          ,count(distinct case when (play+time)>0 then uid end) cvuv
          ,count(distinct case when (play+time)>0 and pt>360 then uid end) yx_cvuv 
          ,sum( COALESCE((case when (play+time)>0 and pt>360 then pt else 0 end),0)) yx_pt
      FROM temp.$T_ALBUM_INFO t1 
      join $T_USER_PLAY_DAY t2 
      on (t1.pid=t2.pid  and t1.vid=t2.vid and t1.dt=t2.dt)
      and t1.dt=$dateValue
      where
          ((t2.product = '' and t2.p1='') or
          (t2.product = '' and t2.p1=''  ) or
          (t2.product = '' and t2.p1=''  ) or
          (t2.product = '' and t2.p1=''  )
          )
          and t2.uid!='-'
          group by t1.pid,t1.vid,t2.product,t2.p2,t2.uid;
          
          quit;
EOF
}


function initMemShip(){
    hive <<EOF
    insert into temp.$T_MID_MEMSHIP PARTITION(dt=$dateValue)
    
    select temp.userid,
     max(temp.mem_type) 
    from (
    select b.userid

    ,case when orderfrom in ('1') then 'normal'

    when orderfrom in ('3') then 'pro'

    when (orderfrom='14' and aid in ('-','**')) then 'pro'

    when (orderfrom='14' and aid in ('1','**')) then 'normal'

    else 'none' end mem_type           
    from 

            ( 

             select item.uid from temp.$T_MID_FLOW_ITEM item

                where item.dt=$dateValue group by item.uid
            ) a
    join  $T_USER_ORDER b

            on (b.dt=(a.uid%100) 
                and a.uid=b.userid
                and from_unixtime(unix_timestamp(b.paytime, 'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')<=$dateValue
                and from_unixtime(unix_timestamp(b.expiretime, 'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')>=$dateValue
            )
        ) as temp
    group by temp.userid;

    quit;
EOF
}

function initFlowMemShip(){
    hive <<EOF
     set hive.auto.convert.join=false;
    
      INSERT INTO temp.$T_MID_FLOW_WITH_MEMSHIP  PARTITION(dt=$dateValue)
      
      SELECT
      a.pid,a.vid
      ,(case when a.video_type='**' then 1 else 0 end ) is_zp
      ,a.vv as vv
      ,a.cv as cv
      ,a.yx_cv as yx_cv
      ,a.pt as pt
      ,a.vvuv as vvuv
      ,a.cvuv as cvuv
      ,a.yx_cvuv as yx_cvuv
      ,a.yx_pt as yx_pt
      ,a.uid
      ,a.product
      ,a.p2
      ,(case when a.is_pay!='1' then 0 
      when (array_contains(split(a.pay_platform,','),'1') and a.product='*') then 1 

      when  (array_contains(split(a.pay_platform,','),'2') and a.product='1') then 1 

      when  (array_contains(split(a.pay_platform,','),'3') and a.product='*') then 1 

      when  (array_contains(split(a.pay_platform,','),'4') and a.product='2') then 1 

      when  (array_contains(split(a.pay_platform,','),'5') and a.product='3') then 1 
       else 0 end) is_pay
      ,case when b.mem_type is null then 'none' else b.mem_type end

      FROM 
       (
          select 
              *
          from  
              temp.$T_MID_FLOW_ITEM  where dt=$dateValue
       )
        a
      left join temp.$T_MID_MEMSHIP b
      on (b.dt=$dateValue and a.uid=b.userid);
    
    quit;
EOF
}


log "Starting job ..."

dateConfig=`mysql -h*** -u*** -p** database** -A -N -e "select album_dt,end_dt,id from bdp_album_job_config where type='1' and state='0' order by id asc limit 1;"`

if [ -n "$dateConfig" ]; then
    log "data config has value $dateConfig"
    dateCfgArr=($dateConfig)
    dateValueStart=${dateCfgArr[0]}
    dateValueEnd=${dateCfgArr[1]}
    t_id=${dateCfgArr[2]}
    
    log "update date config ,t_id: $t_id,dateValueStart:$dateValueStart,dateValueEnd:$dateValueEnd"
    mysql -h* -u* -p* database -A -N -e "update bdp_album_job_config set state='1' where id='$t_id';"
    
fi

startDate=`date -d "${dateValueStart}" +%s`
endDate=`date -d "${dateValueEnd}" +%s`
##计算两个时间戳的差值除于每天86400s即为天数差
stampDiff=`expr $endDate - $startDate`
diff=`expr $stampDiff / 86400`

log "day diff is $diff"

for((i=0;i<=diff;i ++))
do
    dateValue=`date -d "$dateValueStart +$i day " +%Y%m%d`
    dateInFormat=`date -d "$dateValue" +%Y-%m-%d`
    log "target date is $dateValue dateInFormat is $dateInFormat"

    createTables

    log "after createTables method" 

    initAlbumInfo
    
    albumCnt=`hive -S -e "select count(*) from temp.$T_ALBUM_INFO where dt=$dateValue;"`
    
    log "albumCnt:$albumCnt dateValue:$dateValue"
    
if [ $albumCnt -gt 0 ] ; then

    log "after initAlbumInfo method"

    initFlowItems

    log "after initFlowItems method"

    initMemShip

    log "after initMemShip method"

    initFlowMemShip

    log "after initFlowMemShip method"

#计算不同类型流量信息

    hive <<EOF
      
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,vv,cv,vvuv,cvuv,pt,'z',mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue;
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,cv,'0' as vvuv,cvuv,pt,'zp' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and is_zp='1';
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,cv,'0' as vvuv,cvuv,pt,'pay_zp' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and is_zp='1' and is_pay = '1';
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,cv,'0' as vvuv,cvuv,pt,'mem_z' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and mem_type in ('pro','normal');
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,cv,'0' as vvuv,cvuv,pt,'mem_zp' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and mem_type in ('pro','normal') and is_zp='1';
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,yx_cv,'0' as vvuv,yx_cvuv,yx_pt,'mem_valid_zp' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and mem_type in ('pro','normal') and is_zp='1';
    
    insert into temp.$T_FLOW_INFO PARTITION(dt=$dateValue)
    select pid,vid,'0' as vv,cv,'0' as vvuv,cvuv,pt,'mem_pay_zp' as type,mem_type,uid,product,p2
    from temp.$T_MID_FLOW_WITH_MEMSHIP
    where dt=$dateValue and mem_type in ('pro','normal') and is_zp='1' and is_pay='1';
    
    insert into temp.$T_ALBUM_FLOW_SUM PARTITION(dt=$dateValue)
    select t1.pid,sum(vv) as vv,sum(cv) as cv,count(distinct case when vvuv> 0 then uid end) vvuv
    ,count(distinct case when cvuv> 0 then uid end) cvuv,sum(time_long) as pt,t1.type,t1.mem_type 
    ,t1.terminal,t1.sub_terminal,"$dateInFormat"
    from temp.$T_FLOW_INFO t1 
    where dt=$dateValue group by t1.pid,t1.mem_type,t1.terminal,t1.sub_terminal,t1.type;
    
quit;
EOF

#导出专辑数据

log "start to select album info from hive which will be import to mysql"

declare -A pidUserCountCache=()

pidUserCountInfo=`hive -S -e " select count(distinct uid) count,t1.pid from temp.$T_FLOW_INFO t1  where dt=$dateValue and mem_type!='none' group by t1.pid;"`
log "after get pidUserCountInfo from hive"
if [ -n "$pidUserCountInfo" ]; then 
 while read line
     do
         countArr=($line)
        pidUserCountCache["${countArr[1]}"]="${countArr[0]}"
     done <<EOF
    $pidUserCountInfo
EOF
fi
log "pidUserCountInfo cache size : ${#pidUserCountCache[*]}"

rm -f temp_nys_album_infos_batch.sql 

createTime=`date +%s`
#数据导入,这里字段有空格，转数组时会有问题，sql里添加特殊字符分割
hive -S -e "select concat(t1.pid,'!@!', COALESCE(max(p_name),'-'),'!@!',COALESCE(max(p_category),0),'!@!',COALESCE(max(p_status),0),'!@!',COALESCE(max(p_delete),0)) from temp.$T_ALBUM_INFO t1 where dt=$dateValue group by t1.pid;"|
while read albumInfo 
do
        pid=`echo $albumInfo|awk -F '!@!' '{print $1}'`
        p_name=`echo $albumInfo|awk -F '!@!' '{print $2}'`
        p_name=$(echo $p_name|sed "s/'/\\\'/g")
        p_category=`echo $albumInfo|awk -F '!@!' '{print $3}'`
        p_status=`echo $albumInfo|awk -F '!@!' '{print $4}'`
        p_delete=`echo $albumInfo|awk -F '!@!' '{print $5}'`
        on_time=""
        userCount=${pidUserCountCache["$pid"]}
#若有中文，则需要加'N'，防止乱码
echo -e  "insert into bdp_mms_con_album_info(pid,name_cn,category,status,deleted,is_pay,on_time,create_time,active_user,date_time)\
 values('$pid',N'$p_name','$p_category' ,'$p_status','$p_delete','1','$on_time','$createTime','$userCount','$dateInFormat');">>temp_nys_album_infos_batch.sql
done
log "start to import album info to mysql"

unset pidUserCountCache

db=levp_bdp_c0
#导入专辑数据数据到mysql                                    
mysql -h** -u** -p** $db -e "source ./temp_nys_album_infos_batch.sql"
 #删除产生的sql文件
rm -f temp_nys_album_infos_batch.sql    
log "after import album info to mysql"

#使用sqoop导出流量数据
dbURL="jdbc:mysql://120.0.0.1:3306/$db?useUnicode=true&characterEncoding=utf-8";

dataPath="/user/hive/warehouse/temp.db/$T_ALBUM_FLOW_SUM/dt=$dateValue"


sqoop-export --connect "${dbURL}" --username webapp --password Gu4cu9eedoisoo9b --table bdp_user_album_play_day --columns "pid,vv,cv,vvuv,cvuv,ttime,type,mem_type,terminal,sub_terminal,date_time" --input-null-string '\\N' \
--input-null-non-string '\\N' --export-dir $dataPath --update-mode allowinsert --input-fields-terminated-by '\t' --input-lines-terminated-by '\n' 2>&1

log "after import flow data to mysql"
#更新on_time，以正片为准
rm -f updateAlbumTime_batch.sql
hive -S -e "select t1.pid,min(v_on_time) as on_time from temp.$T_ALBUM_INFO t1 where dt=$dateValue and v_video_type='123123' and v_on_time!='-' and  from_unixtime(unix_timestamp(v_on_time, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')>'2004-01-01' group by t1.pid;"| 
while read line
do 
    arr=($line)
    echo -e "update bdp_mms_con_album_info set on_time='${arr[1]}' where pid=${arr[0]} and date_time='$dateInFormat';">>updateAlbumTime_batch.sql
done
mysql -h** -u** -p** $db -e "source ./updateAlbumTime_batch.sql"
rm -f updateAlbumTime_batch.sql

log "after update on_time"

else log "no album info ,date:$dateValue"
fi
done