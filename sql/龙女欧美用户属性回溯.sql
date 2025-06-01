with active_users as
(select distinct "#user_id" as taid, "#account_id" as role_id, "#distinct_id" as adid
from ta.v_event_39
where "$part_date" >= '2022-01-07'
and "$part_event" in ('CreateUser', 'LogOn', 'LogOut', 'Payment', 'install', 'session', 'install_update')
and "#account_id" is not null
),

adjust_events_base as
(select "#user_id" as taid, "#account_id" as role_id, "#distinct_id" as adid,
"$part_event" as event_name, 
(case when "$part_event" = 'install_update' then '01_install_update' else "$part_event" end) as event_name_r,
"#server_time" as server_time,  "#zone_offset" as server_timezone,
"$part_date" as event_date, "#event_time" as event_time, timezone as event_timezone,
activity_kind,
tracker_name,
network_name,
campaign_name,
adgroup_name,
creative_name,
fb_ins_ref,
fb_ins_ref_campaign_group_name as fb_campaign_name,
fb_ins_ref_campaign_name as fb_adgroup_name,
fb_ins_ref_adgroup_name as fb_creative_name,
fb_ins_ref_ad_objective_name as fb_ad_objname
from ta.v_event_39
where "$part_date" >= '2022-01-07'
and "$part_event" in ('install', 'session', 'install_update')
),

adjust_events as
(select taid, role_id, adid, event_time, event_date, event_name, event_name_r,
network_name,
coalesce(campaign_name, fb_campaign_name) as campaign_name,
coalesce(adgroup_name, fb_adgroup_name) as adgroup_name,
coalesce(creative_name, fb_creative_name) as creative_name,
tracker_name,
fb_ins_ref
from adjust_events_base
),

role_first as
(select distinct role_id,
first_value(network_name) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as network_name,
first_value(campaign_name) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as campaign_name,
first_value(adgroup_name) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as adgroup_name,
first_value(creative_name) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as creative_name,
first_value(tracker_name) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as tracker_name,
first_value(fb_ins_ref) ignore nulls over (partition by role_id order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as fb_ins_ref
from adjust_events
),

adid_first as
(select distinct adid,
first_value(network_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as network_name,
first_value(campaign_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as campaign_name,
first_value(adgroup_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as adgroup_name,
first_value(creative_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as creative_name,
first_value(tracker_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as tracker_name,
first_value(fb_ins_ref) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as fb_ins_ref
from adjust_events
),


role_info as
(select a.*, 
coalesce(b.network_name, c.network_name) as network_name, 
coalesce(b.campaign_name, c.campaign_name) as campaign_name, 
coalesce(b.adgroup_name, c.adgroup_name) as adgroup_name, 
coalesce(b.creative_name, c.creative_name) as creative_name, 
coalesce(b.tracker_name, c.tracker_name) as tracker_name, 
coalesce(b.fb_ins_ref, c.fb_ins_ref) as fb_ins_ref
from active_users a
left join role_first b
on a.role_id = b.role_id
left join adid_first c
on a.adid = c.adid
),

new_user_info as
(select "#account_id" as role_id,
register_date as install_ts,
network_name as network_name_utable,
campaign_name as campaign_name_utable,
adgroup_name as adgroup_name_utable,
creative_name as creative_name_utable
from ta.v_user_39
),

event_join_user as
(select a.*,
b.role_id as role_id_utable,
b.install_ts,
b.network_name_utable,
b.campaign_name_utable,
b.adgroup_name_utable,
b.creative_name_utable
from role_info a
left join new_user_info b
on a.role_id = b.role_id
)

select role_id as "#account_id", localtimestamp as "#time", 
(case when fb_ins_ref is not null or network_name = 'Unattributed' then 'Facebook' else network_name end) as network_name,
campaign_name, adgroup_name, creative_name, 
tracker_name, fb_ins_ref
from event_join_user
where (network_name is not null and network_name_utable is null) 
or 
(campaign_name is not null and campaign_name_utable is null)
