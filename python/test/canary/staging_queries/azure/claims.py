from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

_CLAIMS_QUERY = """
with claims_spine as (
  select
    id as claim_id,
    claimnumber,
    reporteddate,
    policyid,
    timestampadd(day, 1, reporteddate::date) as as_of_ts
  from atlas_cc_claim
  where {{ start_date }} <= REPORTEDDATE::date and {{ end_date }} >= REPORTEDDATE::date
    and coalesce(retired, 0) = 0
    and record_begin_timestamp <= timestampadd(day, 1, reporteddate::date)
    and (record_end_timestamp is null or record_end_timestamp > timestampadd(day, 1, reporteddate::date))
),
claim_amounts as (
  select
    c.claim_id,
    c.claimnumber,
    c.reporteddate,
    p.ext_riskstate,
    coalesce(sum(li.claimamount), 0) as claim_amount
  from claims_spine c
  left join atlas_cc_policy p
    on cast(c.policyid as varchar) = cast(p.id as varchar)
  left join atlas_cc_transaction t
    on t.claimid = c.claim_id
   and coalesce(t.retired, 0) = 0
   and t.bookingdate is not null
   and t.bookingdate <= c.as_of_ts
   and t.record_begin_timestamp <= c.as_of_ts
   and (t.record_end_timestamp is null or t.record_end_timestamp > c.as_of_ts)
  left join atlas_cc_transactionlineitem li
    on li.transactionid = t.id
   and coalesce(li.retired, 0) = 0
   and li.record_begin_timestamp <= c.as_of_ts
   and (li.record_end_timestamp is null or li.record_end_timestamp > c.as_of_ts)
  group by c.claim_id, c.claimnumber, c.reporteddate, p.ext_riskstate
),
enriched as (
  select
    c.*,
    coalesce(tl.typecode, 'UNK') as risk_state_code,
    coalesce(cast(c.ext_riskstate as varchar), 'UNK_STATE') as risk_state_key
  from claim_amounts c
  left join atlas_cctl_typelist tl
    on tl.cctl_id = c.ext_riskstate
   and tl.cctl_table_name = 'CCTL_EXT_RISKSTATECOSTFILTER'
)
select
    c.LOSSDATE,
    datediff('DAY', c.lossdate, e.reporteddate) as DAYS_LOSS_TO_REPORTED,
    e.claimnumber as CLAIMNUMBER,
    cast(c.POLICYID as varchar) as POLICYID,
    c.FLAGGED,
    c.LITIGATIONSTATUS,
    e.reporteddate::date as reporteddate,
    e.reporteddate::date as ds,
    cast((extract(epoch_second from e.reporteddate::timestamp_ntz) * 1000) as bigint) as event_ts,
    c.LOSSCAUSE,
    c.EXT_SEVERITY,
    c.CLAIMTIER,
    c.FAULT,
    e.claim_amount,
    e.risk_state_key,
    e.risk_state_code
from enriched e
join atlas_cc_claim c
  on c.id = e.claim_id
 and coalesce(c.retired, 0) = 0
 and c.record_begin_timestamp <= timestampadd(day, 1, e.reporteddate::date)
 and (c.record_end_timestamp is null or c.record_end_timestamp > timestampadd(day, 1, e.reporteddate::date))
where {{ start_date }} <= e.reporteddate::date and {{ end_date }} >= e.reporteddate::date
qualify row_number() over (
  partition by e.claimnumber, extract(epoch_second from e.reporteddate::timestamp_ntz)
  order by c.LOSSDATE nulls last, c.POLICYID
) = 1
"""


v1 = StagingQuery(
    query=_CLAIMS_QUERY,
    output_namespace="data",
    engine_type=EngineType.SNOWFLAKE,
    dependencies=[
        TableDependency(table="atlas_cc_claim"),
        TableDependency(table="atlas_cc_policy"),
        TableDependency(table="atlas_cc_transaction"),
        TableDependency(table="atlas_cc_transactionlineitem"),
        TableDependency(table="atlas_cctl_typelist"),
    ],
    version=0,
    step_days=30,
)
