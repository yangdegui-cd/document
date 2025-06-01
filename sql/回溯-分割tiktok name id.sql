SELECT "#account_id",
       "#distinct_id",
       LOCALTIMESTAMP() AS "#time",
       network_name,
       regexp_replace(creative_name, '.*\((\d+)\)$', '$1') AS creative_id,
       regexp_replace(creative_name, '\s*\((\d+)\)$', '') AS creative_name,
       regexp_replace(campaign_name, '.*\((\d+)\)$', '$1') AS campaign_id,
       regexp_replace(campaign_name, '\s*\((\d+)\)$', '') AS campaign_name,
       regexp_replace(adgroup_name, '.*\((\d+)\)$', '$1') AS adgroup_id,
       regexp_replace(adgroup_name, '\s*\((\d+)\)$', '') AS adgroup_name
FROM ta.v_user_36
WHERE network_name = 'non-TikTok-AND'