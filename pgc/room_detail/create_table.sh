create table panda_stat.panda_stat_pgc_room_detail(
roomid string,
cname string,
ename string,
plat string,
pv bigint,
uv bigint,
reg_uv bigint,
new_visitors bigint,
new_users bigint,
watch_duration float,
avg_watch_duration float,
user_watch_duration float,
avg_user_watch_duration float,
send_barrages bigint,
send_barrage_users bigint,
avg_send_barrages float,
max_minute_barrages bigint,
acu bigint,
back_pcu bigint,
back_pcu_time string,
pcu_rank bigint, --pcu峰值排名
total_pcu bigint --时点平台pcu最高值
)partitioned by(
par_date string,date_type string
)
