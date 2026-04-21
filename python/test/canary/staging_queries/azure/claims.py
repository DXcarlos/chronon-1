from ai.chronon.staging_query import StagingQuery, TableDependency

# Spark SQL — translated from the Snowflake source. Key substitutions:
#   x::date                               -> CAST(x AS DATE)
#   timestampadd(day, N, x::date)         -> date_add(CAST(x AS DATE), N)
#   extract(epoch_second from x::ts_ntz)  -> unix_timestamp(x)
#   cast(... as varchar)                  -> cast(... as string)
#   datediff('DAY', a, b)                 -> datediff(CAST(b AS DATE), CAST(a AS DATE))
#   QUALIFY row_number() ... = 1          -> subquery + WHERE _rn = 1
_CLAIMS_QUERY = """
with claims_spine as (
  select
    id as claim_id,
    claimnumber,
    reporteddate,
    policyid,
    date_add(cast(reporteddate as date), 1) as as_of_ts
  from default.atlas_cc_claim
  where {{ start_date }} <= cast(reporteddate as date) and {{ end_date }} >= cast(reporteddate as date)
    and coalesce(retired, 0) = 0
    and record_begin_timestamp <= date_add(cast(reporteddate as date), 1)
    and (record_end_timestamp is null or record_end_timestamp > date_add(cast(reporteddate as date), 1))
),
claim_amounts as (
  select
    c.claim_id,
    c.claimnumber,
    c.reporteddate,
    p.ext_riskstate,
    coalesce(sum(li.claimamount), 0) as claim_amount
  from claims_spine c
  left join default.atlas_cc_policy p
    on cast(c.policyid as string) = cast(p.id as string)
  left join default.atlas_cc_transaction t
    on t.claimid = c.claim_id
   and coalesce(t.retired, 0) = 0
   and t.bookingdate is not null
   and t.bookingdate <= c.as_of_ts
   and t.record_begin_timestamp <= c.as_of_ts
   and (t.record_end_timestamp is null or t.record_end_timestamp > c.as_of_ts)
  left join default.atlas_cc_transactionlineitem li
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
    coalesce(cast(c.ext_riskstate as string), 'UNK_STATE') as risk_state_key
  from claim_amounts c
  left join default.atlas_cctl_typelist tl
    on tl.cctl_id = c.ext_riskstate
   and tl.cctl_table_name = 'CCTL_EXT_RISKSTATECOSTFILTER'
),
ranked as (
  select
    c.LOSSDATE,
    datediff(cast(e.reporteddate as date), cast(c.lossdate as date)) as DAYS_LOSS_TO_REPORTED,
    e.claimnumber as CLAIMNUMBER,
    cast(c.POLICYID as string) as POLICYID,
    c.FLAGGED,
    c.LITIGATIONSTATUS,
    cast(e.reporteddate as date) as reporteddate,
    cast(e.reporteddate as date) as ds,
    cast(unix_timestamp(e.reporteddate) * 1000 as bigint) as event_ts,
    c.LOSSCAUSE,
    c.EXT_SEVERITY,
    c.CLAIMTIER,
    c.FAULT,
    e.claim_amount,
    e.risk_state_key,
    e.risk_state_code,
    row_number() over (
      partition by e.claimnumber, unix_timestamp(e.reporteddate)
      order by c.LOSSDATE asc nulls last, c.POLICYID asc
    ) as _rn
  from enriched e
  join default.atlas_cc_claim c
    on c.id = e.claim_id
   and coalesce(c.retired, 0) = 0
   and c.record_begin_timestamp <= date_add(cast(e.reporteddate as date), 1)
   and (c.record_end_timestamp is null or c.record_end_timestamp > date_add(cast(e.reporteddate as date), 1))
  where {{ start_date }} <= cast(e.reporteddate as date) and {{ end_date }} >= cast(e.reporteddate as date)
)
select
  LOSSDATE,
  DAYS_LOSS_TO_REPORTED,
  CLAIMNUMBER,
  POLICYID,
  FLAGGED,
  LITIGATIONSTATUS,
  reporteddate,
  ds,
  event_ts,
  LOSSCAUSE,
  EXT_SEVERITY,
  CLAIMTIER,
  FAULT,
  claim_amount,
  risk_state_key,
  risk_state_code
from ranked
where _rn = 1
"""


v1 = StagingQuery(
    query=_CLAIMS_QUERY,
    output_namespace="data",
    dependencies=[
        TableDependency(table="default.atlas_cc_claim"),
        TableDependency(table="default.atlas_cc_policy"),
        TableDependency(table="default.atlas_cc_transaction"),
        TableDependency(table="default.atlas_cc_transactionlineitem"),
        TableDependency(table="default.atlas_cctl_typelist"),
    ],
    version=0,
    step_days=30,
)
