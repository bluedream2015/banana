#!/usr/bin

date1=${1}
date2=${2}
date_type=${3}
countDay=${4}

if [[ $# == 0 ]]
then
echo "请至少输入一个参数:[startdate] [enddate] [date_type] [countDay]"
exit
fi

#调度系统跑数，只传一个参数
if [[ $# == 1 ]]
then
date1=`date -d "-1 day $date1" +%Y%m%d`
date2=$date1
date_type="day"
countDay=2
fi

#手动跑数或跑周月数据
if [[ $# > 1 ]]
then
 if [[ $date_type == "" ]]
 then
 date_type="day"
 fi
 if [[ $countDay == "" ]]
 then
 countDay=2
 fi
fi

for((i=0;i<$countDay;i++));
do

startdate=`date -d "+$i day $date1" +%Y%m%d`
enddate=`date -d "+$i day $date2" +%Y%m%d`

echo "startdate:"$startdate
echo "enddate:"$enddate
echo "date_type:"$date_type
echo "countDay:"$countDay


hive \
 -hiveconf startdate="$startdate" \
 -hiveconf enddate="$enddate" \
 -hiveconf date_type="$date_type" \
 -f /home/huanglei/script/huanglei/panda_stat/pgc/reward_gift/reward_gift.sql

/usr/local/php-5.6/bin/php /home/zhangyunhua/www/panda_work/Routine_state.php panda_stat.panda_stat_pgc_reward_gift $enddate
done
