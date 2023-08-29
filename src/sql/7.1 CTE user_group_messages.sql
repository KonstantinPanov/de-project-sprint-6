with user_group_messages as (
	select lgd.hk_group_id, count(DISTINCT lum.hk_user_id) as cnt_users_in_group_with_messages
	from  STV230547__DWH.l_groups_dialogs lgd
	inner join STV230547__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id 
	GROUP BY lgd.hk_group_id
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10

; 




with user_group_messages as (
select lgd.hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
from STV230547__DWH.h_dialogs hd
inner join STV230547__DWH.l_user_message lum on lum.hk_message_id = hd.hk_message_id 
inner join STV230547__DWH.l_groups_dialogs lgd on lgd.hk_message_id = hd.hk_message_id 
group by lgd.hk_group_id
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10

; 

Результат вроде тот же