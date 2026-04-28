---
theme: default
colorSchema: dark
title: Designing for the next 10x in Chronon performance
info: |
  Designing for the next 10x in Chronon performance.
  Nikhil Simha Raprolu, Co-founder, Zipline.ai
class: text-center
highlighter: shiki
drawings:
  persist: false
mdc: true
transition: fade
---

# Designing for the next 10x in Chronon performance

Nikhil Simha Raprolu &middot; Co-founder, Zipline.ai

---
layout: center
class: text-center
---

<div class="text-2xl italic opacity-90 leading-relaxed max-w-2xl mx-auto">
&ldquo;Premature optimization is the root of all evil.&rdquo;
</div>

<div class="pt-4 text-sm opacity-50">
&mdash; Donald Knuth, 1974
</div>

<div v-click class="pt-16 text-xl opacity-90 max-w-2xl mx-auto leading-relaxed">
&ldquo;&hellip; yet we should not pass up our opportunities in that <span class="text-emerald-300 font-semibold">critical 3%</span>.&rdquo;
</div>

<div v-click class="pt-10 text-sm opacity-60 max-w-2xl mx-auto">
thousands of scientists &middot; billions of users &middot; trillions in transactions &middot; 10s of billions in revenue
</div>

---

# Agenda

<div class="pt-8 space-y-6 text-lg">

<div v-click>
<span class="text-purple-400 font-semibold">Online optimizations</span> <span class="opacity-50">(15 min)</span>
</div>

<div v-click>
<span class="text-blue-400 font-semibold">Offline optimizations</span> <span class="opacity-50">(7 min)</span>
</div>

<div v-click>
<span class="text-emerald-400 font-semibold">Cluster level optimizations</span> <span class="opacity-50">(5 min)</span>
</div>

<div v-click>
<span class="text-yellow-300 font-semibold">Orchestration</span> <span class="opacity-50">(3 min)</span>
</div>

</div>

---

# The optimization ladder

<div class="pt-2 flex justify-center">

<svg viewBox="0 0 1100 520" style="width:90%">
  <defs>
    <pattern id="rawWash" x="0" y="0" width="6" height="32" patternUnits="userSpaceOnUse">
      <rect x="0" y="0" width="2" height="32" fill="rgba(148,163,184,0.5)"/>
    </pattern>
    <pattern id="hopBlue" x="0" y="0" width="4" height="32" patternUnits="userSpaceOnUse">
      <rect x="0.5" y="0" width="2.5" height="32" fill="rgba(96,165,250,0.6)"/>
    </pattern>
    <pattern id="hopPink" x="0" y="0" width="4" height="32" patternUnits="userSpaceOnUse">
      <rect x="0.5" y="0" width="2.5" height="32" fill="rgba(244,114,182,0.65)"/>
    </pattern>
  </defs>
  <text x="260" y="22" style="font-size:12px" fill="#94a3b8" font-weight="500">7-day query window &middot; 1h hops</text>
  <text x="260" y="38" style="font-size:10px" fill="#64748b">oldest &rarr;</text>
  <text x="880" y="38" style="font-size:10px" fill="#64748b" text-anchor="end">&rarr; now</text>
  <line x1="260" y1="44" x2="880" y2="44" stroke="#374151" stroke-dasharray="2,3"/>
  <text x="940" y="38" style="font-size:12px" fill="#94a3b8" text-anchor="middle" font-weight="600">tiles</text>
  <text x="1050" y="38" style="font-size:12px" fill="#94a3b8" text-anchor="middle" font-weight="600">reads</text>
  <g v-click="1">
    <text x="20" y="78" style="font-size:17px" fill="#cbd5e1" font-weight="600">raw events in KV</text>
    <text x="20" y="96" style="font-size:11px" fill="#94a3b8">aggregate at read time</text>
    <rect x="260" y="65" width="620" height="32" fill="url(#rawWash)" stroke="#475569" rx="2"/>
    <text x="940" y="86" style="font-size:18px" fill="#cbd5e1" text-anchor="middle" font-weight="600">N</text>
    <text x="1050" y="86" style="font-size:14px" fill="#cbd5e1" text-anchor="middle" font-weight="600">1 scan</text>
  </g>
  <g v-click="2">
    <text x="20" y="158" style="font-size:17px" fill="#bfdbfe" font-weight="600">all hop tiles</text>
    <text x="20" y="176" style="font-size:11px" fill="#94a3b8">window / hop</text>
    <rect x="260" y="145" width="620" height="32" fill="url(#hopBlue)" stroke="#60a5fa" rx="2"/>
    <text x="940" y="166" style="font-size:18px" fill="#bfdbfe" text-anchor="middle" font-weight="600">168</text>
    <text x="1050" y="166" style="font-size:14px" fill="#bfdbfe" text-anchor="middle" font-weight="600">1 scan</text>
  </g>
  <g v-click="3">
    <text x="20" y="248" style="font-size:17px" fill="#fbcfe8" font-weight="600">status quo Chronon</text>
    <text x="20" y="266" style="font-size:11px" fill="#94a3b8">tail hops + collapsed + head hops</text>
    <rect x="259" y="227" width="439" height="42" fill="none" stroke="rgba(96,165,250,0.65)" stroke-width="1.2" stroke-dasharray="4,2" rx="3"/>
    <rect x="263" y="234" width="172" height="32" fill="url(#hopBlue)"/>
    <rect x="438" y="234" width="256" height="32" fill="rgba(96,165,250,0.35)" stroke="#60a5fa"/>
    <text x="566" y="255" style="font-size:13px" fill="#dbeafe" text-anchor="middle" font-weight="600">collapsed</text>
    <rect x="703" y="234" width="174" height="32" fill="url(#hopPink)" stroke="rgba(244,114,182,0.7)"/>
    <text x="478" y="287" style="font-size:11px" fill="#94a3b8" text-anchor="middle">batchIr &middot; 1 KV read</text>
    <text x="790" y="287" style="font-size:11px" fill="#94a3b8" text-anchor="middle">streaming hops &middot; 48 KV reads</text>
    <text x="940" y="257" style="font-size:20px" fill="#fbcfe8" text-anchor="middle" font-weight="700">97</text>
    <text x="1050" y="251" style="font-size:13px" fill="#fbcfe8" text-anchor="middle" font-weight="700">1 get + 1 scan</text>
    <text x="1050" y="267" style="font-size:13px" fill="#fbcfe8" text-anchor="middle" font-weight="700">/ N gets</text>
  </g>
  <g v-click="4">
    <text x="20" y="368" style="font-size:17px" fill="#bbf7d0" font-weight="600">MegaTile</text>
    <text x="20" y="386" style="font-size:11px" fill="#94a3b8">collapse head into daily tiles</text>
    <rect x="259" y="347" width="439" height="42" fill="none" stroke="rgba(96,165,250,0.65)" stroke-width="1.2" stroke-dasharray="4,2" rx="3"/>
    <rect x="263" y="354" width="172" height="32" fill="url(#hopBlue)"/>
    <rect x="438" y="354" width="256" height="32" fill="rgba(96,165,250,0.35)" stroke="#60a5fa"/>
    <text x="566" y="375" style="font-size:13px" fill="#dbeafe" text-anchor="middle" font-weight="600">collapsed</text>
    <rect x="703" y="354" width="85" height="32" fill="rgba(74,222,128,0.22)" stroke="#4ade80"/>
    <text x="745" y="375" style="font-size:13px" fill="#bbf7d0" text-anchor="middle" font-weight="600">yest</text>
    <rect x="792" y="354" width="85" height="32" fill="rgba(74,222,128,0.36)" stroke="#4ade80"/>
    <text x="834" y="375" style="font-size:13px" fill="#bbf7d0" text-anchor="middle" font-weight="600">today</text>
    <text x="478" y="407" style="font-size:11px" fill="#94a3b8" text-anchor="middle">batchIr &middot; 1 KV read</text>
    <text x="790" y="407" style="font-size:11px" fill="#86efac" text-anchor="middle">2 daily tiles &middot; 2 KV reads</text>
    <text x="940" y="377" style="font-size:20px" fill="#bbf7d0" text-anchor="middle" font-weight="700">51</text>
    <text x="1050" y="377" style="font-size:20px" fill="#bbf7d0" text-anchor="middle" font-weight="700">3</text>
  </g>
  <g v-click="5">
    <text x="20" y="468" style="font-size:17px" fill="#86efac" font-weight="600">GigaTile</text>
    <text x="20" y="486" style="font-size:11px" fill="#94a3b8">merged at write time</text>
    <rect x="260" y="455" width="620" height="32" fill="rgba(34,197,94,0.45)" stroke="#22c55e" stroke-width="1.5" rx="2"/>
    <text x="567" y="476" style="font-size:14px" fill="#dcfce7" text-anchor="middle" font-weight="700">finalized vector</text>
    <text x="940" y="476" style="font-size:20px" fill="#86efac" text-anchor="middle" font-weight="700">1</text>
    <text x="1050" y="476" style="font-size:20px" fill="#86efac" text-anchor="middle" font-weight="700">1</text>
  </g>
</svg>

</div>

---

# Constraints

<div class="pt-10 text-lg space-y-6">

<v-clicks>

- low latency, high RPS
- sub-second freshness
- bootstrap &mdash; long windows ready same day
- batch correction propagates same day

</v-clicks>

</div>

---

# MegaTile algorithm

<div class="pt-6 text-base space-y-3">

<v-clicks>

- two regimes by window size
  - <span class="text-blue-300">small</span> (<code>&le; 48h</code>) &mdash; keep N hop tiles (5m or hourly) in state
  - <span class="text-purple-300">large</span> (<code>&gt; 48h</code>) &mdash; keep 1 tile per day in state
- on every event, emit one row covering <span class="text-yellow-300">all windows</span> for the entity
  - e.g. with a 6h and a 4d window &rarr; row carries the <span class="text-blue-300">full 6h IR</span> (72 &times; 5m hops) and the <span class="text-purple-300">daily IR</span> for the 4d window
- fetcher merges <code>batch_ir</code> with stream entries
  - <span class="text-blue-300">small</span> &mdash; <code>batch_ir</code> + latest emitted value
  - <span class="text-purple-300">large</span> &mdash; <code>batch_ir</code> + N daily entries from <code>batchDay &rarr; queryDay</code>

</v-clicks>

</div>

---

# MegaTile in production

<div class="pt-10 text-lg space-y-6">

<v-clicks>

- <span class="text-emerald-400 font-semibold">3&times;</span> lower latency
- <span class="text-emerald-400 font-semibold">exact</span> match with old tiling approach
- <span class="text-emerald-400 font-semibold">288 / 576 tiles &rarr; 1 / 2 tiles</span> in the head

</v-clicks>

</div>

---

# GigaTile algorithm

<div class="pt-6 text-base space-y-3">

<v-clicks>

- small windows are fully in Flink state &mdash; carried from MegaTile
- &ldquo;upload&rdquo; batch IRs into Flink state
- Flink maintains large-window running sums
- on new event
  - update small-window tiles, large-window megatiles, and all running sums
  - emit if changed
- on eviction
  - drop older-than-necessary tiles (small + large)
  - refresh running sums
  - emit if changed

</v-clicks>

</div>

---

# GigaTile architecture

<div class="pt-4 flex justify-center">

<svg viewBox="0 0 800 360" style="width:92%">
  <defs>
    <marker id="arrG" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
      <path d="M0,0 L0,6 L9,3 z" fill="#9ca3af"/>
    </marker>
  </defs>

  <g v-click="1">
    <rect x="20" y="60" width="160" height="50" fill="rgba(244,114,182,0.18)" stroke="#f472b6" rx="4"/>
    <text x="100" y="83" text-anchor="middle" style="font-size:13px" fill="#fbcfe8">Kafka events</text>
    <text x="100" y="100" text-anchor="middle" style="font-size:11px" fill="#fbcfe8" opacity="0.8">real-time stream</text>
  </g>

  <g v-click="2">
    <rect x="20" y="240" width="160" height="50" fill="rgba(96,165,250,0.18)" stroke="#60a5fa" rx="4"/>
    <text x="100" y="263" text-anchor="middle" style="font-size:13px" fill="#bfdbfe">Iceberg upload table</text>
    <text x="100" y="280" text-anchor="middle" style="font-size:11px" fill="#bfdbfe" opacity="0.8">batch IR snapshots, daily</text>
  </g>

  <g v-click="3">
    <path d="M185 85 L 290 165" stroke="#9ca3af" fill="none" marker-end="url(#arrG)"/>
    <path d="M185 265 L 290 195" stroke="#9ca3af" fill="none" marker-end="url(#arrG)"/>
    <rect x="295" y="140" width="180" height="80" fill="rgba(156,163,175,0.18)" stroke="#9ca3af" rx="4"/>
    <text x="385" y="170" text-anchor="middle" style="font-size:13px" font-weight="600" fill="#e5e7eb">CoProcessFunction</text>
    <text x="385" y="190" text-anchor="middle" style="font-size:11px" fill="#d1d5db">keyBy(entity)</text>
    <text x="385" y="205" text-anchor="middle" style="font-size:11px" fill="#d1d5db">running finalized vector</text>
  </g>

  <g v-click="4">
    <path d="M480 180 L 540 180" stroke="#9ca3af" fill="none" marker-end="url(#arrG)"/>
    <rect x="545" y="155" width="180" height="50" fill="rgba(74,222,128,0.18)" stroke="#4ade80" rx="4"/>
    <text x="635" y="178" text-anchor="middle" style="font-size:13px" fill="#bbf7d0">finalized vector</text>
    <text x="635" y="195" text-anchor="middle" style="font-size:11px" fill="#86efac">to KV (_PUSH dataset)</text>
  </g>

  <g v-click="5">
    <text x="400" y="335" text-anchor="middle" style="font-size:13px" fill="#4ade80" font-weight="600">
      no Spark changes &middot; the upload table is already there
    </text>
  </g>
</svg>

</div>

---

# What GigaTile unblocks

<div class="pt-6 text-base space-y-3">

<v-clicks>

- <span class="text-emerald-300">cost</span>
  - KV size and serving fleet shrink proportionally with read fan-out
- <span class="text-emerald-300">read-side compute</span>
  - no merge on read &mdash; inference can fetch directly; clients can be pure Python
- <span class="text-emerald-300">continuous writes</span>
  - midnight batch-upload spike disappears &mdash; writes only when value changes
  - KV sees pure deltas &mdash; old bulkload reuploaded every key nightly even when the IR was unchanged
- <span class="text-emerald-300">search-index hydration</span>
  - search indexes need features to be pushed to them

</v-clicks>

</div>

---

# Flink 2.0 &middot; disaggregated state

<div class="pt-2 text-sm opacity-60">GigaTile holds more state &mdash; ForSt makes that affordable</div>

<div class="pt-6 text-base space-y-3">

<v-clicks>

- <span class="text-emerald-300 font-semibold">ForSt</span> &mdash; new state backend (FLIP-423)
  - primary state on S3 / HDFS, local disk as cache
  - replaces all-on-local-disk RocksDB
- decouples state size from TaskManager disk
  - hundreds of TBs viable &middot; checkpoints lightweight
  - rescaling no longer downloads full state
- <span class="text-blue-300 font-semibold">unblocks</span> Iceberg-loaded batchIrs in Flink
  - large batchIr state lives in Flink without disk pressure
  - elastic on K8s &middot; fast cold start from S3
- caveats &mdash; experimental in 2.0
  - full benefits need async State V2 APIs (FLIP-424/425)
  - uncached throughput ~50% of local RocksDB &middot; cache sizing matters

</v-clicks>

</div>

---
layout: center
class: text-center
---

<div class="text-sm uppercase tracking-[0.3em] opacity-50">part 2</div>

# <span class="text-blue-400">Offline</span> optimizations

<div class="pt-4 text-base opacity-60">Sawtooth &middot; UnionJoin</div>

---

# The problem

<div class="pt-6 text-base">

**Inputs**

<v-clicks>

- `queries` &mdash; `(key, ts)` rows
- `events` &mdash; `(key, payload, ts)` rows
- window `w` + aggregation `agg` &mdash; e.g., 7-day sum

</v-clicks>

</div>

<div v-click class="pt-8 text-base">

**Output**

For each query, aggregate the matching events:

</div>

<div v-after class="pt-3 font-mono text-base">
<code>result(key, query.ts) = agg(payload) where event.ts &isin; [query.ts &minus; w, query.ts)</code>
</div>

---

# Sawtooth &middot; within-bucket reuse

<div class="pt-4 flex justify-center">

<svg viewBox="0 0 1100 420" style="width:95%">
  <text x="495" y="38" style="font-size:15px" fill="#cbd5e1" text-anchor="middle">for one key &middot; 7-day window &middot; 5-min tiles</text>
  <text x="390" y="80" style="font-size:15px" fill="#86efac" text-anchor="middle" font-weight="600">tail &middot; <tspan font-family="monospace" font-weight="400">[1:00 &minus; 7d, 1:00)</tspan></text>
  <path d="M 60 110 L 60 100 L 720 100 L 720 110" stroke="#86efac" stroke-width="1.4" fill="none"/>
  <rect x="60" y="135" width="660" height="40" fill="rgba(251,191,36,0.32)" stroke="rgba(251,191,36,0.6)" stroke-width="0.9"/>
  <text x="390" y="160" style="font-size:14px" fill="#fde68a" text-anchor="middle" font-weight="600">~2,000 five-min tiles</text>
  <line x1="720" y1="125" x2="720" y2="190" stroke="#64748b" stroke-dasharray="3,2" stroke-width="1"/>
  <rect x="725" y="135" width="155" height="40" fill="rgba(96,165,250,0.10)" stroke="#60a5fa" stroke-dasharray="3,2"/>
  <text x="802" y="125" style="font-size:13px" fill="#bfdbfe" text-anchor="middle" font-family="monospace">[1:00, 1:05)</text>
  <circle cx="745" cy="155" r="3.5" fill="#f472b6"/>
  <circle cx="760" cy="155" r="3.5" fill="#f472b6"/>
  <circle cx="775" cy="155" r="3.5" fill="#f472b6"/>
  <circle cx="803" cy="155" r="3.5" fill="#f472b6"/>
  <circle cx="850" cy="155" r="3.5" fill="#f472b6"/>
  <text x="935" y="158" style="font-size:13px" fill="#fbcfe8">head events</text>
  <text x="60" y="207" style="font-size:13px" fill="#cbd5e1">1:00 &minus; 7d</text>
  <text x="720" y="207" style="font-size:13px" fill="#cbd5e1" text-anchor="end">1:00</text>
  <text x="880" y="207" style="font-size:13px" fill="#cbd5e1" text-anchor="end">1:05</text>
  <polygon points="756,232 750,219 762,219" fill="#10b981"/>
  <polygon points="787,232 781,219 793,219" fill="#10b981"/>
  <polygon points="818,232 812,219 824,219" fill="#10b981"/>
  <text x="756" y="252" style="font-size:13px" fill="#86efac" text-anchor="middle">1:01</text>
  <text x="787" y="252" style="font-size:13px" fill="#86efac" text-anchor="middle">1:02</text>
  <text x="818" y="252" style="font-size:13px" fill="#86efac" text-anchor="middle">1:03</text>
  <path d="M 753 264 L 753 272 L 821 272 L 821 264" stroke="#86efac" stroke-width="1" fill="none"/>
  <text x="787" y="295" style="font-size:14px" fill="#86efac" text-anchor="middle">3 queries &middot; same tail &middot; different heads</text>
  <text x="500" y="372" style="font-size:32px" fill="#86efac" text-anchor="middle" font-weight="700">99.98% reuse within a bucket</text>
</svg>

</div>

---

# Sawtooth &middot; adjacent-tail reuse

<div class="pt-4 flex justify-center">

<svg viewBox="0 0 1100 460" style="width:95%">
  <text x="500" y="38" style="font-size:15px" fill="#cbd5e1" text-anchor="middle">two adjacent 5-min buckets &middot; 7-day window</text>
  <text x="60" y="138" style="font-size:14px" fill="#cbd5e1" font-weight="600">tail @ 1:00</text>
  <text x="60" y="161" style="font-size:13px" fill="#86efac" font-family="monospace">[1:00 &minus; 7d, 1:00)</text>
  <text x="220" y="118" style="font-size:12px" fill="#94a3b8">1:00 &minus; 7d</text>
  <text x="1042" y="118" style="font-size:12px" fill="#94a3b8" text-anchor="end">1:00</text>
  <rect x="220" y="125" width="22" height="42" fill="rgba(244,114,182,0.55)" stroke="#f472b6" stroke-width="1.2"/>
  <rect x="242" y="125" width="800" height="42" fill="rgba(251,191,36,0.32)" stroke="rgba(251,191,36,0.6)" stroke-width="0.9"/>
  <text x="231" y="190" style="font-size:13px" fill="#fbcfe8" text-anchor="middle" font-weight="600">drop</text>
  <text x="642" y="226" style="font-size:14px" fill="#fde68a" text-anchor="middle" font-weight="600">~2,000 shared tiles</text>
  <text x="60" y="278" style="font-size:14px" fill="#cbd5e1" font-weight="600">tail @ 1:05</text>
  <text x="60" y="301" style="font-size:13px" fill="#86efac" font-family="monospace">[1:05 &minus; 7d, 1:05)</text>
  <text x="242" y="258" style="font-size:12px" fill="#94a3b8">1:05 &minus; 7d</text>
  <text x="1064" y="258" style="font-size:12px" fill="#94a3b8" text-anchor="end">1:05</text>
  <rect x="242" y="265" width="800" height="42" fill="rgba(251,191,36,0.32)" stroke="rgba(251,191,36,0.6)" stroke-width="0.9"/>
  <rect x="1042" y="265" width="22" height="42" fill="rgba(74,222,128,0.55)" stroke="#4ade80" stroke-width="1.2"/>
  <text x="1053" y="330" style="font-size:13px" fill="#bbf7d0" text-anchor="middle" font-weight="600">add</text>
  <text x="500" y="382" style="font-size:14px" fill="#94a3b8" text-anchor="middle">slide one tile right &middot; 1 in, 1 out</text>
  <text x="500" y="425" style="font-size:30px" fill="#86efac" text-anchor="middle" font-weight="700">99.9% reuse across adjacent tails</text>
</svg>

</div>

---

# Sawtooth &middot; the 3 layers

<div class="pt-4 flex justify-center">
<div class="space-y-3 w-full max-w-[1000px]">

<v-clicks>

<div class="grid grid-cols-[1fr_auto] gap-6 items-center border-l-4 border-blue-400 pl-5 pr-4 py-3">
<div>
<div class="text-base font-bold text-blue-200">layer 1 &middot; <code>hopsAggregate</code></div>
<div class="pt-1.5 text-base opacity-90">pre-aggregate events into 5m + 1h + 1d hop tiles per key</div>
<div class="pt-1.5 font-mono text-sm opacity-65">events: (key, ts, payload) &rarr; (key, [hop_ir])</div>
</div>
<div class="text-2xl font-semibold text-blue-300 whitespace-nowrap">1 shuffle</div>
</div>

<div class="grid grid-cols-[1fr_auto] gap-6 items-center border-l-4 border-yellow-400 pl-5 pr-4 py-3">
<div>
<div class="text-base font-bold text-yellow-100">layer 2 &middot; <code>computeWindows</code></div>
<div class="pt-1.5 text-base opacity-90">for each key, fold tiles into one tail IR per query's 5m head bucket</div>
<div class="pt-1.5 font-mono text-sm opacity-65">(key, [hop_ir]) &#x22c8; (key, [headStart]) &rarr; ((key, headStart), tail_ir)</div>
<div class="font-mono text-xs opacity-50 italic">headStart := round_down(query.ts, 5m)</div>
</div>
<div class="text-2xl font-semibold text-yellow-200 whitespace-nowrap">3 shuffles</div>
</div>

<div class="grid grid-cols-[1fr_auto] gap-6 items-center border-l-4 border-emerald-400 pl-5 pr-4 py-3">
<div>
<div class="text-base font-bold text-emerald-200">layer 3 &middot; <code>cumulate</code></div>
<div class="pt-1.5 text-base opacity-90">for each query, fold head events onto the bucket's tail</div>
<div class="pt-1.5 font-mono text-sm opacity-65">((k, hStart), tail_ir) &#x22c8; ((k, hStart), queries) &#x22c8; ((k, hStart), head_events)</div>
<div class="font-mono text-sm opacity-65 pl-3">&rarr; (k, query_ts, result)</div>
</div>
<div class="text-2xl font-semibold text-emerald-200 whitespace-nowrap">4 shuffles</div>
</div>

</v-clicks>

<div v-click class="pt-5 text-center">
<span class="text-base opacity-70">total: </span>
<span class="text-2xl font-semibold text-pink-300">8 shuffles</span>
<span class="text-base opacity-70"> of kryo-encoded Java IRs</span>
</div>

</div>
</div>

---

# Fully distributed sawtooth &middot; temporalEvents

<div class="pt-8 text-base space-y-3">

<v-clicks>

- run the 3 layers across the cluster
  - events + queries for the same key never land on the same machine
- <span class="text-emerald-300">handles any-skew</span>
  - a key with millions of events still parallelizes &mdash; layers fan out across executors
- <span class="text-pink-300">cost: 8 shuffles</span>
  - kryo-encoded Java IRs, back and forth between two partitionings

</v-clicks>

</div>

---

# Semi distributed sawtooth &middot; UnionJoin

<div class="pt-8 text-base space-y-3">

<v-clicks>

- most workloads aren't pathologically skewed
  - collapse the per-key sawtooth onto one machine
  - sawtooth handles moderate skew fine in a single partition
- <span class="text-emerald-300">1 shuffle</span>
  - Spark-native exchange, then sawtooth runs in-memory per partition
- <span class="text-pink-300">trade-off: key fits in memory</span>
  - a single key with millions of events can OOM the executor

</v-clicks>

</div>

---

# UnionJoin in production

<div class="pt-10 text-lg space-y-6">

<v-clicks>

- <span class="text-emerald-400 font-semibold">~10&times;</span> faster in production
- default for PITC aggregations

</v-clicks>

</div>

---
layout: center
class: text-center
---

<div class="text-sm uppercase tracking-[0.3em] opacity-50">part 3</div>

# <span class="text-emerald-400">Cluster-level</span> optimizations

<div class="pt-4 text-base opacity-60">Crucible &middot; Spark on K8s</div>

---

# Crucible

<div class="pt-4 text-base">
Bring-your-own-cloud platform for Spark batch and Flink streaming. Single Helm chart into EKS / AKS / GKE.
</div>

<div class="pt-8 text-sm opacity-70">napkin math &middot; shuffle-dominant Spark, $/vCPU&middot;hr</div>

<div class="pt-3">

| platform | $/vCPU&middot;hr | notes |
|---|---|---|
| <span class="text-pink-300">Databricks Serverless Jobs</span> | <span class="text-pink-300 font-semibold">~$0.20</span> | $0.37/DBU &middot; ~0.5 vCPU/DBU |
| <span class="text-yellow-200">EMR Serverless (Graviton)</span> | <span class="text-yellow-200 font-semibold">~$0.06</span> | published vCPU + memory rates |
| <span class="text-emerald-300">Spark on K8s &middot; Graviton spot + NVMe</span> | <span class="text-emerald-300 font-semibold">~$0.025</span> | m7gd spot &middot; NVMe shuffle, no EBS |

</div>

<div class="pt-6 text-base text-center">
<span class="text-emerald-400 font-semibold">~8&times; cheaper</span> than Databricks Serverless &middot; <span class="text-emerald-400 font-semibold">~3&times;</span> than EMR Serverless
</div>

<div class="pt-3 text-xs opacity-50 text-center">shuffle-heavy gap widens further &mdash; Databricks/EMR shuffle to remote/EBS, NVMe is free with the instance</div>

---

# Crucible &middot; scope

<div class="pt-2 text-sm opacity-60">what's in the box</div>

<div class="pt-6 text-base space-y-3">

<v-clicks>

- <span class="text-emerald-300 font-semibold">API gateway</span> &mdash; CRUD Spark / Flink jobs over HTTP/JSON
  - submit &middot; list &middot; status &middot; kill &middot; logs &middot; metrics
- <span class="text-emerald-300 font-semibold">immediate feedback</span> &mdash; Databricks-Serverless-class startup
  - warm pool reserved for drivers &middot; sub-20s submit-to-first-task
- <span class="text-emerald-300 font-semibold">one-click UIs</span> &mdash; Spark UI + Flink UI proxied per job
- <span class="text-emerald-300 font-semibold">logs + metrics</span> &mdash; Loki + Prometheus + Grafana
  - LogQL search across all jobs in a namespace
- <span class="text-emerald-300 font-semibold">performance defaults</span> &mdash; tuned for shuffle-dominant workloads
  - AQE, DRA, decommission, memory overhead, lz4/zstd split
  - locked &middot; admin &middot; user override precedence

</v-clicks>

</div>

---

# Crucible &middot; levers

<div class="pt-2 text-sm opacity-60">why each one matters</div>

<div class="pt-6 text-base space-y-3">

<v-clicks>

- <span class="text-emerald-300 font-semibold">NVMe</span> &mdash; root-disk EBS shuffle is slow + noisy
  - DaemonSet auto-mounts &middot; executors auto-routed &middot; ~3&times; faster shuffle I/O
- <span class="text-emerald-300 font-semibold">lz4 / zstd split</span> &mdash; codec per path, not one default
  - lz4 on local spill &middot; cheap CPU pairs with NVMe's IOPS headroom
  - zstd on shuffle wire &middot; ~2&times; denser, network is scarcer than NVMe bytes
- <span class="text-emerald-300 font-semibold">Graviton</span> &mdash; ARM is 60&ndash;70% cheaper than equivalent x86 spot
  - same JVM bytecode &middot; multi-arch images &middot; tested on AWS / Azure / GCP
- <span class="text-emerald-300 font-semibold">spot</span> &mdash; executors on spot, drivers pinned on-demand
  - preemption can't kill the job &middot; warm pool gives sub-20s submit-to-first-task
- <span class="text-emerald-300 font-semibold">Spark 4.1 decommission</span> &mdash; DRA without an external shuffle service
  - executors migrate shuffle blocks before exiting on the 2-min spot notice
  - no Celeborn / external shuffle service to operate

</v-clicks>

</div>

---
layout: center
class: text-center
---

<div class="text-sm uppercase tracking-[0.3em] opacity-50">part 4</div>

# <span class="text-yellow-300">Orchestration</span>

<div class="pt-4 text-base opacity-60">Zipline &middot; feature iteration</div>

---

# Zipline orchestrator

<div class="pt-2 text-sm opacity-60">why feature iteration gets fast</div>

<div class="pt-6 text-base space-y-3">

<v-clicks>

- adding a new feature must be fast &mdash; the whole point of Chronon
  - hours to first backfill, not days
- <span class="text-yellow-300 font-semibold">Chronon-native</span> &mdash; understands joins, group-bys, time ranges
  - dedups subgraphs across features &middot; reuses materialized intermediates
  - drives maximal reuse, never recomputes what is already there
- <span class="text-yellow-300 font-semibold">small Spark jobs, spot-tolerant</span>
  - short jobs fit inside spot intervals &middot; failure blast radius is small
  - many small &gt; one large &mdash; better packing, fewer stragglers

</v-clicks>

</div>

---

# Conclusion

<div class="pt-16 space-y-12 text-lg">

<v-clicks>

<div>
the whole stack has to move together &mdash; <span class="opacity-70">orchestrator + chronon + spark + flink + compute cluster</span>
</div>

<div>
performance is UX
</div>

</v-clicks>

</div>

---
layout: center
class: text-center
---

# Thanks

<div class="pt-4 text-base opacity-70">
Questions?
</div>

<div class="pt-12 text-sm opacity-50">
Nikhil Simha Raprolu &middot; Co-founder, Zipline.ai
</div>
