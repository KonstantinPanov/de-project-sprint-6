with user_group_log as (
	select luga.hk_group_id, count(DISTINCT luga.hk_user_id) as cnt_added_users
	from STV230547__DWH.l_user_group_activity luga
	inner join STV230547__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
	where sah.event ='add' and luga.hk_user_id IN (select hu.hk_user_id from STV230547__DWH.h_users hu order by hu.registration_dt desc limit 10)
	GROUP BY luga.hk_group_id
)

,user_group_messages as (
	select lgd.hk_group_id, count(DISTINCT lum.hk_user_id) as cnt_users_in_group_with_messages
	from  STV230547__DWH.l_groups_dialogs lgd
	inner join STV230547__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id 
	GROUP BY lgd.hk_group_id
)

select ugm.cnt_users_in_group_with_messages, ugl.cnt_added_users, (ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users) as group_conversion from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc
