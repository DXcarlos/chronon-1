
## Problem

inputs: 
  queries: table of `key, ts` rows
  events: table of `key, payload, ts` rows 
  window: w, agg (7d sum for this doc)
    
output:
  results: `key, query.ts, result => sum(payload) IF event.ts IN [query.ts - w, query.ts)`

## Core properties 

sawtooth window - assume 7d window has 5m tile size.
  sum @ `1:02 today` = sum events in `[1:00 7 days ago, 1:02 today)`

  
  1. the TAILS for two queries at `1:02p` and `1:03p` both start at `1:00 7 days ago` 
     1. they only differ in the head. 
     2. infact all queries between `[1:00, 1:05)` share the same tail - `[1:00 7 days ago, 1:00)`
     3. 99.98% overlap
    
  1. two adjacent TAILS also share 99.9% of compute 
     1. `[1:00 7 days ago, 1:00)` vs. `[1:05 7 days, 1:05]`  
  
  
## Sawtooth aggregator

we exploit these properties to aggressively reuse the partial sums
there are three layers

1. compute all tile sums (one per 5m - `1:00 - 1:05`, `1:05 - 1:10`, ...)
  1. `key, [event]` => `key, [tile_ir]` (combine reduce)
2. merge these ALL tiles together into ALL tails with maximum reuse - `1:00 7days ago - 1:00`
  2. `key, [tile_ir]` join `key, [tail_end_ts]` => `key, [tail_ir]`
3. combine `tail_irs` + `head events` for all `queries` in the 5m bucket
  1. `key, tail_end_ts, tail_ir 'join' [query] 'join' [event]`=> `key, [result per query]`

Benefits
  extreme reuse - 99.95% overlap in nearby queries, 99.9% overlap in adjacent tails ~ 1000x
  skew handling - all events and all queries of a skew key - never endup on the same machine!

Downsides
  ~7 shuffles of kryo serialized java object IRs

note: the algorithm is heavily simplified



