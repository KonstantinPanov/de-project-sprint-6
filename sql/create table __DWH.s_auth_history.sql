drop table if exists STV230547__DWH.s_auth_history;

create table STV230547__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_auth_history_l_user_group_activity REFERENCES STV230547__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);