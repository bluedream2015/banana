add jar /home/huanglei/script/huanglei/panda_stat/common/hiveudf-0.0.1-SNAPSHOT.jar;
create temporary FUNCTION urlformat as 'com.panda.hiveudf.UrlFormat';

alter table panda_stat.panda_stat_pgc_page_click drop partition(par_date='${hiveconf:enddate}',date_type='${hiveconf:date_type}');

set hive.auto.convert.join=true;
with q1 as (
select '${hiveconf:enddate}' as par_date,
       t2.guid,
       panda_realtime.urldecode(t2.psrc,'utf-8',1) as psrc,
       panda_realtime.urldecode(t2.channel,'utf-8',1) as channel,
       urlformat(concat(parse_url(panda_realtime.urldecode(t2.url,'utf-8',1),'HOST'),parse_url(panda_realtime.urldecode(t2.url,'utf-8',1),'PATH')))  as url,
       t2.pae,
       t2.paew,
       t2.refer,
       t3.guid as new_guid
from (
  select par_date,guid,ip 
  from panda_result.user_daily_view 
  where par_date>='${hiveconf:startdate}'  and par_date<='${hiveconf:enddate}'
  group by par_date,guid,ip 
) t1
inner join (
    select par_date,
           panda_realtime.urldecode(pref,'utf-8',1) as refer,
           concat(panda_realtime.urldecode(pchannel_a,'utf-8',1),'-',panda_realtime.urldecode(pchannel_b,'utf-8',1),'-',panda_realtime.urldecode(pchannel_c,'utf-8',1)) as channel, 
           pcid as guid,
           ip,
           concat(panda_realtime.urldecode(psrc_a,'utf-8',1),'-',panda_realtime.urldecode(psrc_b,'utf-8',1),'-',panda_realtime.urldecode(psrc_c,'utf-8',1)) as psrc,
           panda_realtime.urldecode(purl,'utf-8',1) as url,
           lower(pae) as pae,
           paew
    from panda_realtime.panda_pc_import 
    where par_date>='${hiveconf:startdate}'  and par_date<='${hiveconf:enddate}'
) t2 on t1.par_date=t2.par_date and t1.guid=t2.guid and t1.ip=t2.ip
left outer join (
select par_date,guid
from panda_stat.panda_stat_new_guid
where par_date>='${hiveconf:startdate}' and par_date<='${hiveconf:enddate}'
) t3 on t1.par_date=t3.par_date and t1.guid=t3.guid
),
--url
--模糊url
q20 as (
  select '${hiveconf:enddate}' as par_date,urlformat(lower(url)) as url from panda_dict.pgc_info where url!='' and url not like '--' and own=0 group by url
),
--精确url
q21 as (
  select '${hiveconf:enddate}' as par_date,urlformat(lower(url)) as url from panda_dict.pgc_info where url!='' and url not like '--' and own=1 group by url
),
--psrc
q3 as (
  select '${hiveconf:enddate}' as par_date,lower(psrc) as psrc from panda_dict.pgc_info where psrc!='' and psrc not like '--' group by psrc
),
--channel
q4 as (
  select '${hiveconf:enddate}' as par_date,lower(channel) as channel from panda_dict.pgc_info where channel!='' and channel not like '--' group by channel
),
--pae
q5 as (
  select '${hiveconf:enddate}' as par_date,lower(pae) as pae from panda_dict.pgc_info where pae!='' and pae not like '--' group by pae
),
--paew
q6 as (
  select '${hiveconf:enddate}' as par_date,lower(paew) as paew from panda_dict.pgc_info where paew!='' and paew not like '--' group by paew
),
--refer
q7 as (
  select '${hiveconf:enddate}' as par_date,concat('%',lower(refer),'%') as refer from panda_dict.pgc_info where refer!='' and refer not like '--' group by refer
),
--click
q8 as (
  select '${hiveconf:enddate}' as par_date,lower(description) as click from panda_dict.pgc_info where description!='' and description not like '--' group by description
)

insert overwrite table panda_stat.panda_stat_pgc_page_click partition(par_date='${hiveconf:enddate}',date_type='${hiveconf:date_type}')
--url
select 'total' as roomid,
        q1.url,
       'total' as psrc,
       'total' as channel,
       'total' as pae,
       'total' as paew,
       'total' as click,
       'total' as refer,
        count(1) as click_times,
        count(distinct(guid)) as click_users,
        1 as own,
        count(distinct(new_guid)) as new_click_users
from q1 join q21 on q1.url=q21.url 
group by q1.url 
union all
select 'total' as roomid,
        q20.url,
       'total' as psrc,
       'total' as channel,
       'total' as pae,
       'total' as paew,
       'total' as click,
       'total' as refer,
        count(1) as click_times,
        count(distinct(guid)) as click_users,
        0 as own,
        count(distinct(new_guid)) as new_click_users
from q1 join q20 on q1.par_date=q20.par_date 
where q1.url like concat('%',q20.url,'%') 
--and not (q20.url='pgc.panda.tv' and q1.url like '%pgc.panda.tv/h5%')
group by q20.url 
union all
--psrc
select 'total' as roomid,
       'total' as url,
       q1.psrc,
       'total' as channel,
       'total' as pae,
       'total' as paew,
       'total' as click,
       'total' as refer,
       count(1),
       count(distinct(guid)),
       0 as own,
       count(distinct(new_guid)) as new_click_users
from q1 join q3 on q1.par_date=q3.par_date
where q1.psrc=q3.psrc
group by q1.psrc
union all
--channel
select 'total' as roomid,
       'total' as url,
       'total' as psrc,
       q1.channel,
       'total' as pae,
       'total' as paew,
       'total' as click,
       'total' as refer,
       count(1) as click_times,
       count(distinct(guid)) as click_users,
       0 as own,
       count(distinct(new_guid)) as new_click_users
from q1 join q4 on q1.par_date=q4.par_date
where q1.channel=q4.channel
group by q1.channel
union all
--pae
select 'total' as roomid,
       'total' as url,
       'total' as psrc,
       'total' as channel,
       q1.pae,
       q6.paew,
       'total' as click,
       'total' as refer,
       count(1) as click_times,
       count(distinct(guid)) as click_users,
       0 as own,
       count(distinct(new_guid)) as new_click_users
from q1 join q5 on q1.par_date=q5.par_date
        join q6 on q1.par_date=q6.par_date
where q1.pae=q5.pae and q1.paew=q6.paew
group by q1.pae,q6.paew
union all
select 'total' as roomid,
       'total' as url,
       'total' as psrc,
       'total' as channel,
       'total' as pae,
       'total' as paew,
       'total' as click,
       q1.refer,
       count(1) as click_times,
       count(distinct(guid)) as click_users,
       0 as own,
       count(distinct(new_guid)) as new_click_users
from q1 join q7 on q1.par_date=q7.par_date
where q1.refer like q7.refer
group by q1.refer 
