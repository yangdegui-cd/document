with role_first as
(select distinct "#account_id" as role_id, 
first_value(adid) ignore nulls over (partition by "#account_id" order by "#event_time" 
rows between unbounded preceding and unbounded following) as adid
from ta.v_event_91
where "$part_date" >= '2023-06-01'
and "$part_event" in ('rolelogin', 'register', 'logout', 'Payment')
and "#account_id" is not null
),

adjust_events_base as
(select 
"#distinct_id" as adid,
"$part_event" as event_name, 
(case when "$part_event" = 'install_update' then '01_install_update' else "$part_event" end) as event_name_r,
"#event_time" as event_time,
"$part_date" as event_date,
activity_kind,
tracker_name,
network_name,
campaign_name,
campaign_id,
adgroup_name,
adgroup_id,
creative_name,
creative_id,
fb_ins_ref,
fb_ins_ref_campaign_group_name as fb_campaign_name,
fb_ins_ref_campaign_name as fb_adgroup_name,
fb_ins_ref_adgroup_name as fb_creative_name,
fb_ins_ref_ad_objective_name as fb_ad_objname,
fb_ins_ref_campaign_group_id as fb_campaign_id,
fb_ins_ref_campaign_id as fb_adgroup_id,
fb_ins_ref_adgroup_name as fb_creative_id,
country,
city,
gps_adid
from ta.v_event_91
where "$part_date" >= '2023-06-01'
and "$part_event" in ('install', 'session', 'install_update')
),

adjust_events as
(select adid, event_time, event_date, event_name, event_name_r,
network_name,
coalesce(campaign_name, fb_campaign_name) as campaign_name,
coalesce(campaign_id, fb_campaign_id) as campaign_id,
coalesce(adgroup_name, fb_adgroup_name) as adgroup_name,
coalesce(adgroup_id, fb_adgroup_id) as adgroup_id,
coalesce(creative_name, fb_creative_name) as creative_name,
coalesce(creative_id, fb_creative_id) as creative_id,
country,
city,
gps_adid,
tracker_name,
fb_ins_ref
from adjust_events_base
),

adid_first as
(select distinct adid,
first_value(network_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as network_name,
first_value(campaign_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as campaign_name,
first_value(campaign_id) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as campaign_id,
first_value(adgroup_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as adgroup_name,
first_value(adgroup_id) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as adgroup_id,
first_value(creative_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as creative_name,
first_value(creative_id) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as creative_id,
first_value(tracker_name) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as tracker_name,
first_value(fb_ins_ref) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as fb_ins_ref,
first_value(country) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as country,
first_value(city) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as city,
first_value(gps_adid) ignore nulls over (partition by adid order by event_name_r, event_time
rows between unbounded preceding and unbounded following) as gps_adid
from adjust_events
),


role_info as
(select 
a.role_id as "#account_id",
localtimestamp as "#time",
(case when b.fb_ins_ref is not null or b.network_name = 'Unattributed' then 'Facebook' else b.network_name end) as network_name,
b.campaign_name,
b.campaign_id,
b.adgroup_name,
b.adgroup_id,
b.creative_name,
b.creative_id,
b.tracker_name,
b.fb_ins_ref,
b.country,
b.city,
b.gps_adid
from role_first a
left join adid_first b
on a.adid = b.adid
)

select a.* from role_info a
left join ta.v_user_91 b
on a."#account_id" = b."#account_id"
where a.network_name != b.network_name