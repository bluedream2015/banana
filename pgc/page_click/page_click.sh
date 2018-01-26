#!/usr/bin

startdate=${1}
enddate=${2}
date_type=${3}
countDay=${4}

if [[ ${startdate} == "" ]]
then
startdate=`date -d "-1 day" +%Y%m%d`
fi

if [[ ${enddate} == "" ]]
then
enddate=`date -d "-1 day" +%Y%m%d`
fi

if [[ $date_type == "" ]]
then
date_type="day"
fi

if [[ $countDay == "" ]]
then
countDay=1
fi

echo ${startdate}
echo ${enddate}
echo $date_type
echo $countDay

echo "*****************"
for((i=0;i<$countDay;i++));
do

echo ${startdate}
echo ${enddate}

hive \
 -hiveconf startdate="$startdate" \
 -hiveconf enddate="$enddate" \
 -hiveconf date_type="$date_type" \
 -f /home/huanglei/script/huanglei/panda_stat/pgc/page_click/page_click.sql

/usr/local/php-5.6/bin/php /home/zhangyunhua/www/panda_work/Routine_state.php panda_stat.panda_stat_pgc_page_click $enddate

done
