create table panda_stat.panda_stat_pgc_page_click(
roomid string,
url string,
psrc string,
channel string,
pae string,
paew string,
click string,
refer string,
click_times bigint,
own int,
new_click_users bigint
)partitioned by(
par_date string,date_type string
)
