with user_group_log as (
	select luga.hk_group_id, count(DISTINCT luga.hk_user_id) as cnt_added_users
	from STV230547__DWH.l_user_group_activity luga
	inner join STV230547__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
	where sah.event ='add' and luga.hk_user_id IN (select hu.hk_user_id from STV230547__DWH.h_users hu order by hu.registration_dt desc limit 10)
	GROUP BY luga.hk_group_id
)

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10

; 