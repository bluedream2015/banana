create table panda_stat.panda_stat_pgc_reward_gift(
roomid string,
cname string,
ename string,
give_gifts bigint,
give_gift_users bigint,
give_gift_value double,
avg_give_gift_value double,
give_bamboos bigint,
give_bamboo_users bigint,
gift_name string,
gift_num bigint,
gift_price double
)partitioned by(
par_date string,date_type string
)
