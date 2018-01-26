--房间uv,pv
with q1 as (
select par_date,
       name as roomid,
       uv,
       pv,
       reg_uv,
       new_visitors,
       new_users
from panda_stat.panda_stat_user_uvpv
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
p1 as (
select par_date,
       name as roomid,
       plat,
       uv,
       pv,
       reg_uv,
       new_visitors,
       new_users
from  panda_stat.panda_stat_user_plat_uvpv
where par_date='${hiveconf:enddate}' and group_type='room_plat2' and date_type='${hiveconf:date_type}'
),
--观看时长,平均观看时长
q3 as (
select par_date,
       name as roomid,
       visitor_duration as watch_duration,
       avg_visitor_duration as avg_watch_duration,
       user_duration as user_watch_duration,
       avg_user_duration as avg_user_watch_duration
from panda_stat.panda_stat_watch_duration_group
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
p3 as (
select par_date,
       name as roomid,
       plat,
       visitor_duration as watch_duration,
       avg_visitor_duration as avg_watch_duration,
       user_duration as user_watch_duration,
       avg_user_duration as avg_user_watch_duration
from panda_stat.panda_stat_watch_duration_plat_group
where par_date='${hiveconf:enddate}' and group_type='room_plat2' and date_type='${hiveconf:date_type}'
),
--弹幕
q4 as (
select par_date,
       name as roomid,
       send_barrages,
       send_barrage_users,
       avg_send_barrages,
       max_minute_barrages
from panda_stat.panda_stat_barrage_group
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
p4 as (
select par_date,
       name as roomid,
       plat,
       send_barrages,
       send_barrage_users,
       avg_send_barrages,
       max_minute_barrages
from panda_stat.panda_stat_barrage_plat_group
where par_date='${hiveconf:enddate}' and group_type='room_plat2' and date_type='${hiveconf:date_type}'
),
--acu
q5 as (
select
       par_date,
       name as roomid,
       acu
from panda_stat.panda_stat_acu
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
--pcu
q6 as (
select par_date,
       name as roomid,
       back_pcu_time,
       back_pcu
from panda_stat.panda_stat_pcu
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
--pcu_rank
q7 as (
select back_pcu_time,roomid,pcu,back_pcu_rank
from (
select t1.minute as back_pcu_time,t1.roomid,t1.pcu,rank() over(partition by t1.minute order by t1.pcu desc) as back_pcu_rank
from (
select 
  par_date,
  room_id as roomid,
  substr(time,1,16) minute,
  max(all_user_count) pcu
from  panda_realtime.pcgameq_shadow_real_person
where par_date>='${hiveconf:startdate}' and par_date<='${hiveconf:enddate}'
group by par_date,room_id,substr(time,1,16)
) t1 join (
select t1.par_date,t1.roomid,t1.back_pcu_time
from (
select par_date,
       name as roomid,
       back_pcu_time
from panda_stat.panda_stat_pcu
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
) t1 join (
 select room_id as roomid
 from panda_dict.pgc_info
 where room_id!='' and room_id not like '--'
 group by room_id
) t2 on t1.roomid=t2.roomid 
) t2 on t1.par_date=t2.par_date and t1.minute=t2.back_pcu_time
) t 
group by back_pcu_time,roomid,pcu,back_pcu_rank
),
q8 as (
select roomid
from (
select roomid from q1
union
select roomid from q3
union
select roomid from q4
union
select roomid from q5
union
select roomid from q6
) t
group by roomid
),
p8 as (
select roomid,plat
from (
 select roomid,plat from p1
 union
 select roomid,plat from p3
 union
 select roomid,plat from p4
) t
group by roomid,plat
),
q9 as (
select '${hiveconf:enddate}' as par_date,
        minute as pcu_time,
        pcu as total_pcu
from (
select minute,sum(pcu) as pcu
from (
  select
    room_id,
    substr(time,1,16) minute,
    max(all_user_count) pcu
  from  panda_realtime.pcgameq_shadow_real_person
  where par_date>='${hiveconf:startdate}' and par_date<='${hiveconf:enddate}'
  group by par_date,room_id,substr(time,1,16)
) t
group by minute
) t
),
q10 as (
 select room_id as roomid
 from panda_dict.pgc_info 
 where room_id!='' and room_id not like '--'
 group by room_id
),
q11 as (
  select t1.roomid,t2.f_cname,t2.f_ename,t2.ename,t2.cname
  from (
       select room_id as roomid,classification as ename
       from panda_realtime.panda_room_info
       where par_date='${hiveconf:enddate}'
  ) t1 join panda_realtime.panda_full_classify t2 on t1.ename=t2.ename
  where t2.par_date='${hiveconf:enddate}'
)
insert overwrite table panda_stat.panda_stat_pgc_room_detail partition(par_date='${hiveconf:enddate}',date_type='${hiveconf:date_type}')
select q8.roomid,
       q11.cname,
       q11.ename,
       'total' as plat,
       q1.pv,
       q1.uv,
       q1.reg_uv,
       q1.new_visitors,
       q1.new_users,
       q3.watch_duration,
       round(q3.avg_watch_duration,6),
       q3.user_watch_duration,
       round(q3.avg_user_watch_duration,6),
       q4.send_barrages,
       q4.send_barrage_users,
       round(q4.avg_send_barrages,3),
       q4.max_minute_barrages,
       q5.acu,
       q6.back_pcu,
       q6.back_pcu_time,
       q7.back_pcu_rank,
       q9.total_pcu
from q8 left outer join q1 on q8.roomid=q1.roomid
        left outer join q3 on q8.roomid=q3.roomid
        left outer join q4 on q8.roomid=q4.roomid
        left outer join q5 on q8.roomid=q5.roomid
        left outer join q6 on q8.roomid=q6.roomid
        left outer join q7 on q6.roomid=q7.roomid and q6.back_pcu_time=q7.back_pcu_time
        left outer join q9 on q6.back_pcu_time=q9.pcu_time
        join q10 on q8.roomid=q10.roomid
        left outer join q11 on q8.roomid=q11.roomid
union all
select
     p8.roomid, 
     q11.cname,
     q11.ename,
     p8.plat,
     p1.pv,
     p1.uv,
     p1.reg_uv,
     p1.new_visitors,
     p1.new_users,
     p3.watch_duration,
     round(p3.avg_watch_duration,6) as avg_watch_duration,
     p3.user_watch_duration,
     round(p3.avg_user_watch_duration,6) as avg_user_watch_duration,
     p4.send_barrages,
     p4.send_barrage_users,
     round(p4.avg_send_barrages,3) as avg_send_barrages,
     p4.max_minute_barrages,
     0 as acu,
     0 as back_pcu,
     0 as back_pcu_time,
     0 as back_pcu_rank,
     0 as total_pcu
from p8 left outer join p1 on p8.roomid=p1.roomid and p8.plat=p1.plat        
        left outer join p3 on p8.roomid=p3.roomid and p8.plat=p3.plat
        left outer join p4 on p8.roomid=p4.roomid and p8.plat=p4.plat
        join q10 on p8.roomid=q10.roomid
        left outer join q11 on p8.roomid=q11.roomid
