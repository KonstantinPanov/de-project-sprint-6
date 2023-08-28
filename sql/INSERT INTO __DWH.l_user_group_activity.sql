INSERT INTO STV230547__DWH.l_user_group_activity(hk_l_user_group_activity, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id, hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV230547__STAGING.group_log as gl
left join STV230547__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV230547__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_user_group_activity from STV230547__DWH.l_user_group_activity);