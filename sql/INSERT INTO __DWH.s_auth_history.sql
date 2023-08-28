INSERT INTO STV230547__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

select
hash(luga.hk_group_id, luga.hk_user_id),
gl.user_id_from,
gl.event,
gl.datetime as event_dt, 
now() as load_dt,
's3' as load_src

from STV230547__STAGING.group_log as gl
left join STV230547__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV230547__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV230547__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id

; 