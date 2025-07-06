
with eligible as (
    select distinct
        mmxRequestUUID
    from default_iceberg.search.search_ranking_v5_intl_listing_with_labels
    where _DATE between 'start_date' and 'end_date'
    and label__search_labels_attributions_v2_tight_attributions_last_1d is not null 
),
filtered_join_listing as (
    select 
        source.*,
        source.mmxRequestUUID as request_id
    from default_iceberg.search.search_ranking_v5_intl_listing_with_labels source
    left semi join eligible on eligible.mmxRequestUUID = source.mmxRequestUUID
    where _DATE between 'start_date' and 'end_date'
),
filtered_join_context as (
    select
        *
    from search.search_ranking_v5_intl_context
    where _DATE between 'start_date' and 'end_date'
),
hardware_type as (
    select
        mmxRequestUUID,
        any_value(primaryHardwareType) as primaryHardwareType,
        _DATE
    from search.search_attributions_tight_v2
    where _DATE between 'start_date' and 'end_date'
    group by mmxRequestUUID, _DATE
),
values as (
    select 
        ls.browser_id as `browser_id`,
        ls.listing_id as `listing_id`,
        ls.mmxRequestUUID as `mmxRequestUUID`,
        ls.pipeline as `pipeline`,
        ls.placement as `placement`,
        ls.query as `query`,
        ls.search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last as `search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last`,
        ls.search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last as `search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last`,
        ls.ts as `ts`,
        ls.userCountry as `userCountry`,
        ls.user_id as `user_id`,
        ls._DATE as _DATE,
        coalesce(ls.label__search_labels_attributions_v2_tight_attributions_last_1d, array('no_event')) as label__search_labels_attributions_v2_tight_attributions_last_1d,
        ht.primaryHardwareType
    from filtered_join_listing as ls 
    join hardware_type as ht
        on (ls.request_id = ht.mmxRequestUUID 
        and ls._DATE = ht._DATE)
),
filtered_raw_logs as (
    select 
        mmxRequestUUID,
        candidateInfo,
        contextualInfo,
        requestInfo,
        listing_ids,
        placement,
        pipeline,
        _DATE
    from polaris_catalog.search.search_logging_v5_logs
    where _DATE between 'start_date' and 'end_date'
),
positions as (
    select
        mmxRequestUUID,
        position,
        listing_id,
        _DATE
    from filtered_raw_logs as frl 
    lateral view posexplode(listing_ids) as position, listing_id
),
collected as (
    select
        mmxRequestUUID,
        first(ts) as ts,
        first(_DATE) as _DATE,
        first(pipeline) as pipeline,
        case
            when max(
                exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'click')
                and exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'cart_add')
                and exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'purchase')
            ) = true then 'purchase'
            when max(
                exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'click')
                and exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'cart_add')
            ) = true then 'cart_add'
            when max(
                exists(label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x = 'click')
            ) = true then 'click'
            else 'no_event'
        end as attribution_type,
        named_struct(
            'id', collect_list(mmxRequestUUID),
            'userCountry', collect_list(userCountry),
            'placement', collect_list(placement),
            'primaryHardwareType', collect_list(primaryHardwareType),
            'search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last', collect_list(named_struct('value', search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last)),'search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last', collect_list(named_struct('value', search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last)),'label__search_labels_attributions_v2_tight_attributions_last_1d', collect_list(named_struct('value', label__search_labels_attributions_v2_tight_attributions_last_1d))
        ) as features
    from (
        select values.*
        from values
        join positions
            on values.mmxRequestUUID = positions.mmxRequestUUID
            and values.listing_id = positions.listing_id
            and values._DATE = positions._DATE
        order by values.mmxRequestUUID, positions.position
    )
    group by mmxRequestUUID
)
select
    c.ts,
    frl._DATE,
    frl.pipeline,
    attribution_type,
    transform(features.label__search_labels_attributions_v2_tight_attributions_last_1d, f -> f.value) as attributions,
    features.id as id,
    features.id as requestUUID,
    features.userCountry as userCountry,
    features.placement as placement,
    features.primaryHardwareType as primaryHardwareType,
    transform(features.search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last, x -> x.value) as `candidateInfo.ziplineInfo.search_fulfillment_edd_v1_max_calendar_days_intl_orders_keys_last`,
transform(features.search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last, x -> x.value) as `candidateInfo.ziplineInfo.search_fulfillment_edd_v1_max_calendar_days_intl_orders_values_last`,
transform(features.label__search_labels_attributions_v2_tight_attributions_last_1d, x -> x.value) as `label__search_labels_attributions_v2_tight_attributions_last_1d`,
    frl.mmxRequestUUID as `mmxRequestUUID`,
    ctx.search_beacons_user_browser_cart_adds_v2_listing_id_last50_30d as `contextualInfo[name=browser].ziplineInfo.search_beacons_user_browser_cart_adds_v2_listing_id_last50_30d`,
    frl.candidateInfo.candidateInfo_docInfo_listingInfo_activeListingBasics_countryName as `candidateInfo.docInfo.listingInfo.activeListingBasics.countryName`,
    frl.requestInfo.clientProvidedInfo_browser_canPerso as `clientProvidedInfo.browser.canPerso`,
    frl.contextualInfo.contextualInfo_name_browser_rivuletBrowserInfo_timeseries_recentlyCartaddedListingIds50FV1_listingId as `contextualInfo[name=browser].rivuletBrowserInfo.timeseries.recentlyCartaddedListingIds50FV1#listingId`,    
    array(
        'contextualInfo[name=browser].ziplineInfo.search_beacons_user_browser_cart_adds_v2_listing_id_last50_30d', 
        'contextualInfo[name=browser].ziplineInfo.search_beacons_user_browser_cart_adds_v2_shop_id_last50_30d'
    ) as to_expand,
    size(features.id) as candidate_count
from collected c
join filtered_raw_logs as frl 
    on c.mmxRequestUUID = frl.mmxRequestUUID 
    and c._DATE = frl._DATE
join filtered_join_context as ctx
    on c.mmxRequestUUID = ctx.mmxRequestUUID 
    and c._DATE = ctx._DATE

