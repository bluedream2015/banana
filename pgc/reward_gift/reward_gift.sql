with q1 as (
select name as roomid,
       give_gifts, --礼物打赏数量
       give_gift_users, --打赏礼物用户数
       give_gift_value, --打赏礼物价值
       round(give_gift_value/give_gift_users,2) as avg_give_gift_value --平均打赏礼物价值
from panda_stat.panda_stat_user_consume_group
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
q2 as (
select name as roomid, 
       give_bamboos, --打赏竹子数量
       give_bamboo_users --打赏竹子用户数
from panda_stat.panda_stat_bamboo_group
where par_date='${hiveconf:enddate}' and group_type='room' and date_type='${hiveconf:date_type}'
),
q3 as (
select roomid,
       name as gift_name,  --打赏礼物名称
       sum(quantity) as gift_num, --打赏礼物数量
       max(price) as gift_price  --打赏礼物单价
from panda_stat.panda_stat_pay_expend
where par_date>='${hiveconf:startdate}' and par_date<='${hiveconf:enddate}'
group by roomid,name
),
q4 as (
select roomid
from (
  select roomid from q1
  union all
  select roomid from q2
) t
group by roomid
),
q5 as (
select t1.roomid,
       t2.f_cname,
       t2.f_ename,
       t2.ename,
       t2.cname
from (
    select room_id as roomid,classification as ename
    from panda_realtime.panda_room_info
    where par_date='${hiveconf:enddate}'
) t1 join panda_realtime.panda_full_classify t2 on t1.ename=t2.ename
where t2.par_date='${hiveconf:enddate}'
),
q6 as (
  select room_id as roomid
  from panda_dict.pgc_info 
  where room_id!='' and room_id not like '--'
  group by room_id
)

insert overwrite table panda_stat.panda_stat_pgc_reward_gift partition(par_date='${hiveconf:enddate}',date_type='${hiveconf:date_type}')
select
  q4.roomid,
  q5.cname,
  q5.ename,
  q1.give_gifts,
  q1.give_gift_users,
  q1.give_gift_value,
  q1.avg_give_gift_value,
  q2.give_bamboos,
  q2.give_bamboo_users,
  q3.gift_name,
  q3.gift_num,
  q3.gift_price
from q4 left outer join q5 on q4.roomid=q5.roomid
        left outer join q1 on q1.roomid=q4.roomid
        left outer join q2 on q2.roomid=q4.roomid
        left outer join q3 on q3.roomid=q4.roomid
        join q6 on q4.roomid=q6.roomid
