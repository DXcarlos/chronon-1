
    WITH user_segments AS (
        SELECT 
            user_id,
            username,
            country,
            CASE 
                WHEN exists(preferences, x -> x = 'push') THEN 'push_enabled'
                WHEN exists(preferences, x -> x = 'email') THEN 'email_only'
                ELSE 'minimal'
            END AS preference_segment,
            preferences
        FROM users
        WHERE country IN ('US', 'UK', 'CA')
    ),
    
    session_events AS (
        SELECT 
            s.session_id,
            s.user_id,
            s.device_type,
            s.channels,
            collect_list(e.event_type) AS event_types,
            collect_list(e.properties) AS all_properties,
            count(*) AS event_count,
            max(e.event_timestamp) AS last_event_time
        FROM sessions s
        LEFT JOIN events e ON s.session_id = e.session_id
        GROUP BY s.session_id, s.user_id, s.device_type, s.channels
    ),
    
    engagement_metrics AS (
        SELECT 
            se.user_id,
            se.device_type,
            count(DISTINCT se.session_id) AS session_count,
            sum(se.event_count) AS total_events,
            avg(se.event_count) AS avg_events_per_session,
            collect_set(flatten(se.all_properties)) AS unique_properties,
            CASE 
                WHEN exists(flatten(collect_list(se.event_types)), x -> x = 'purchase') THEN 'converter'
                WHEN exists(flatten(collect_list(se.event_types)), x -> x = 'click') THEN 'engaged'
                ELSE 'browser'
            END AS user_type,
            array_distinct(flatten(collect_list(se.channels))) AS all_channels
        FROM session_events se
        GROUP BY se.user_id, se.device_type
    ),
    
    final_enriched AS (
        SELECT 
            us.user_id,
            us.username,
            us.country,
            us.preference_segment,
            em.device_type,
            em.session_count,
            em.total_events,
            em.avg_events_per_session,
            em.user_type,
            em.all_channels,
            CASE 
                WHEN em.user_type = 'converter' AND exists(us.preferences, x -> x = 'push') THEN 'high_value'
                WHEN em.user_type = 'engaged' AND em.avg_events_per_session > 2 THEN 'medium_value'
                ELSE 'low_value'
            END AS value_segment,
            filter(em.unique_properties, x -> x != 'home') AS filtered_properties,
            transform(em.all_channels, x -> upper(x)) AS normalized_channels
        FROM user_segments us
        JOIN engagement_metrics em ON us.user_id = em.user_id
        WHERE em.total_events > 0
    )
    
    SELECT 
        country,
        preference_segment,
        device_type,
        value_segment,
        count(*) AS user_count,
        avg(total_events) AS avg_total_events,
        array_distinct(flatten(collect_list(normalized_channels))) AS all_channel_types,
        collect_set(user_type) AS user_types_in_segment,
        CASE 
            WHEN exists(collect_list(value_segment), x -> x = 'high_value') THEN 'contains_high_value'
            ELSE 'no_high_value'
        END AS segment_quality,
        size(array_distinct(flatten(collect_list(filtered_properties)))) AS unique_property_count
    FROM final_enriched
    GROUP BY country, preference_segment, device_type, value_segment
    HAVING count(*) > 0
    ORDER BY country, preference_segment, device_type, value_segment
    