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

<div class="pt-8 opacity-60 text-sm">
Backfill, serving, and infra &mdash; 30 minutes.
</div>

---

# The three vectors

<div class="grid grid-cols-3 gap-6 pt-4">

<div class="border-l-4 border-purple-400 pl-4">
<div class="text-xs uppercase opacity-60">online &middot; serving</div>
<div class="text-2xl font-semibold pt-1">Mega &rarr; GigaTile</div>
<div class="text-sm pt-2 opacity-80">push merge from read to write</div>
</div>

<div class="border-l-4 border-blue-400 pl-4">
<div class="text-xs uppercase opacity-60">offline &middot; backfill</div>
<div class="text-2xl font-semibold pt-1">UnionJoin</div>
<div class="text-sm pt-2 opacity-80">collapse N shuffles into one</div>
</div>

<div class="border-l-4 border-emerald-400 pl-4">
<div class="text-xs uppercase opacity-60">offline &middot; cluster</div>
<div class="text-2xl font-semibold pt-1">Crucible</div>
<div class="text-sm pt-2 opacity-80">right hardware, right pricing</div>
</div>

</div>

<div class="pt-12 text-center text-lg opacity-80">
Each one moves work off the hot path or onto cheaper hardware.
</div>

---

# Roadmap

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

<div v-click class="pt-6 text-sm opacity-60">
Recap &middot; Q&amp;A
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
    <text x="1050" y="257" style="font-size:20px" fill="#fbcfe8" text-anchor="middle" font-weight="700">49</text>
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

<div v-click="6" class="pt-2 text-sm opacity-60 text-center">
MegaTile and GigaTile come next.
</div>

---

# Constraints

<div class="pt-10 text-xl space-y-6">

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

<div class="pt-12 grid grid-cols-3 gap-6 text-center">

<div>
<div class="text-xs uppercase opacity-60">p95 latency</div>
<div class="text-5xl font-semibold pt-2 text-emerald-400">~3&times;</div>
<div class="text-sm pt-3 opacity-80">reduction in production</div>
</div>

<div>
<div class="text-xs uppercase opacity-60">parity with tiling</div>
<div class="text-5xl font-semibold pt-2 text-emerald-400">exact</div>
<div class="text-sm pt-3 opacity-80">result match</div>
</div>

<div>
<div class="text-xs uppercase opacity-60">KV size + throughput</div>
<div class="text-5xl font-semibold pt-2 text-emerald-400">~N&times;</div>
<div class="text-sm pt-3 opacity-80">shrink scales with read fan-out</div>
</div>

</div>

<div class="pt-14 text-sm opacity-60 text-center">
Bootstrap mechanism (existing batch IR) is retained. Drop-in via <code>OnlineStrategy</code> enum.
</div>

---

# GigaTile &middot; the idea

<div class="pt-4 text-base">
Move the merge to the write path: Flink emits a finalized vector per entity. Fetcher
does one point get; no aggregation on read.
</div>

<div class="pt-8 flex justify-center">

<svg viewBox="0 0 880 220" style="width:72%">
  <defs>
    <pattern id="rawTiles" x="0" y="0" width="5" height="34" patternUnits="userSpaceOnUse">
      <rect x="0" y="0" width="3" height="34" fill="rgba(244,114,182,0.45)"/>
    </pattern>
    <pattern id="megaNested" x="0" y="0" width="6" height="34" patternUnits="userSpaceOnUse">
      <rect x="0" y="0" width="3.5" height="34" fill="rgba(251,191,36,0.40)"/>
    </pattern>
  </defs>

  <text x="100" y="40" text-anchor="middle" style="font-size:13px" fill="#fbcfe8" font-weight="600">tiling</text>
  <rect x="20" y="60" width="160" height="34" fill="url(#rawTiles)" stroke="rgba(244,114,182,0.7)" rx="2"/>
  <text x="100" y="120" text-anchor="middle" style="font-size:14px" fill="#fbcfe8" font-weight="700">500+</text>
  <text x="100" y="140" text-anchor="middle" style="font-size:11px" fill="#9ca3af">tiles in flight</text>

  <path d="M200 80 L 320 80" stroke="#9ca3af" stroke-width="1.5" fill="none" marker-end="url(#arr2)"/>

  <text x="430" y="40" text-anchor="middle" style="font-size:13px" fill="#fde68a" font-weight="600">MegaTile</text>
  <rect x="350" y="60" width="160" height="34" fill="url(#megaNested)" stroke="rgba(251,191,36,0.7)" rx="2"/>
  <text x="430" y="120" text-anchor="middle" style="font-size:14px" fill="#fde68a" font-weight="700">~250</text>
  <text x="430" y="140" text-anchor="middle" style="font-size:11px" fill="#9ca3af">nested in 1 batch + 2 daily</text>

  <path d="M530 80 L 650 80" stroke="#9ca3af" stroke-width="1.5" fill="none" marker-end="url(#arr2)"/>

  <text x="760" y="40" text-anchor="middle" style="font-size:13px" fill="#86efac" font-weight="600">GigaTile</text>
  <rect x="700" y="60" width="120" height="34" fill="rgba(34,197,94,0.32)" stroke="#22c55e" stroke-width="1.5" rx="2"/>
  <text x="760" y="80" text-anchor="middle" style="font-size:11px" fill="#dcfce7" font-weight="600">vector</text>
  <text x="760" y="120" text-anchor="middle" style="font-size:14px" fill="#86efac" font-weight="700">1</text>
  <text x="760" y="140" text-anchor="middle" style="font-size:11px" fill="#9ca3af">finalized</text>

  <text x="440" y="200" text-anchor="middle" style="font-size:13px" fill="#86efac" font-weight="600">
    1 read &middot; no decode, no merge
  </text>
</svg>

</div>

---

# GigaTile architecture

<div class="pt-2 text-xs opacity-60 text-center">click &rarr; to build</div>

<div class="pt-2 flex justify-center">

<svg viewBox="0 0 800 360" style="width:60%">
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

<div class="pt-4 grid grid-cols-2 gap-x-8 gap-y-5 text-sm">

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">cost</div>
<div class="opacity-80 pt-1">KV size and serving fleet shrink proportionally with read fan-out.</div>
</div>

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">read-side compute</div>
<div class="opacity-80 pt-1">No merge on read. Inference can fetch directly; clients can be pure Python.</div>
</div>

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">continuous writes</div>
<div class="opacity-80 pt-1">Midnight batch-upload spike disappears. Writes only when value changes.</div>
</div>

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">search-index hydration</div>
<div class="opacity-80 pt-1">Same vector, different sink &mdash; Elasticsearch / Vespa.</div>
</div>

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">broader KV backends</div>
<div class="opacity-80 pt-1">Plain key&rarr;value semantics. Adding a new backend stops being a months-long project.</div>
</div>

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400">cold serving</div>
<div class="opacity-80 pt-1">Iceberg stream emits keys for batch-only entities &mdash; full key superset, no bootstrap RPC.</div>
</div>

</div>

---

# The problem

<div class="pt-6 text-base">

**Inputs**

- `queries` &mdash; `(key, ts)` rows
- `events` &mdash; `(key, payload, ts)` rows
- window `w` + aggregation `agg` &mdash; e.g., 7-day sum

</div>

<div class="pt-8 text-base">

**Output**

For each query, aggregate the matching events:

</div>

<div class="pt-3 font-mono text-base">
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

<div class="pt-2 text-xs opacity-50 text-center">click &rarr; reveal shuffle counts</div>

<div class="pt-6 space-y-6">

<div class="grid grid-cols-[1fr_auto] gap-12 items-center border-l-4 border-blue-400 pl-6">
<div>
<div class="text-xs uppercase tracking-wider text-blue-300 font-semibold">layer 1 &middot; tile IRs</div>
<div class="text-lg pt-2">pre-aggregate events into 5-min tiles, one row per key</div>
<div class="font-mono text-sm opacity-60 pt-1">(key, event) &rarr; (key, [tile_ir])</div>
</div>
<div v-click="1" class="text-3xl font-semibold text-blue-300 whitespace-nowrap pr-4">1 shuffle</div>
</div>

<div class="grid grid-cols-[1fr_auto] gap-12 items-center border-l-4 border-yellow-400 pl-6">
<div>
<div class="text-xs uppercase tracking-wider text-yellow-300 font-semibold">layer 2 &middot; tail IRs</div>
<div class="text-lg pt-2">merge tiles into per-bucket tails &mdash; reuse across adjacent buckets</div>
<div class="font-mono text-sm opacity-60 pt-1">(key, [tile_ir]) &#x22c8; (key, [head_start]) &rarr; ((key, head_start), tail_ir)</div>
</div>
<div v-click="1" class="text-3xl font-semibold text-yellow-300 whitespace-nowrap pr-4">3 shuffles</div>
</div>

<div class="grid grid-cols-[1fr_auto] gap-12 items-center border-l-4 border-emerald-400 pl-6">
<div>
<div class="text-xs uppercase tracking-wider text-emerald-300 font-semibold">layer 3 &middot; per-query IRs</div>
<div class="text-lg pt-2">extend each tail with head events for queries in the bucket</div>
<div class="font-mono text-sm opacity-60 pt-1">((key, head_start), tail_ir) &#x22c8; queries &#x22c8; events &rarr; results</div>
</div>
<div v-click="1" class="text-3xl font-semibold text-emerald-300 whitespace-nowrap pr-4">4 shuffles</div>
</div>

</div>

<div v-click="1" class="pt-10 text-center">
<span class="text-base opacity-70">total: </span>
<span class="text-3xl font-semibold text-pink-300">8 shuffles</span>
<span class="text-base opacity-70"> of kryo-encoded Java IRs</span>
</div>

---

# Fully distributed sawtooth &middot; temporalEvents

<div class="pt-8 text-lg leading-relaxed opacity-90">
Run the 3 layers across the cluster. Events and queries for the same key never land on the same machine.
</div>

<div class="pt-14 grid grid-cols-2 gap-16">

<div class="border-l-2 border-emerald-400 pl-5">
<div class="font-semibold text-emerald-400 uppercase text-xs">benefit</div>
<div class="pt-4">
<div class="text-2xl font-semibold text-emerald-300">handles any-skew</div>
<div class="text-sm opacity-70 pt-2">a key with millions of events still parallelizes &mdash; the layers fan out across executors</div>
</div>
</div>

<div class="border-l-2 border-pink-400 pl-5">
<div class="font-semibold text-pink-400 uppercase text-xs">cost</div>
<div class="pt-4">
<div class="text-2xl font-semibold text-pink-300">8 shuffles</div>
<div class="text-sm opacity-70 pt-2">kryo-encoded Java IRs, back and forth between two partitionings</div>
</div>
</div>

</div>

---

# Semi distributed sawtooth &middot; UnionJoin

<div class="pt-8 text-lg leading-relaxed opacity-90">
Most workloads aren't pathologically skewed. Collapse the per-key sawtooth onto one machine &mdash; sawtooth handles moderate skew fine in a single partition.
</div>

<div class="pt-14 grid grid-cols-2 gap-16">

<div class="border-l-2 border-emerald-400 pl-5">
<div class="font-semibold text-emerald-400 uppercase text-xs">benefit</div>
<div class="pt-4">
<div class="text-2xl font-semibold text-emerald-300">1 shuffle</div>
<div class="text-sm opacity-70 pt-2">Spark-native exchange, then sawtooth runs in-memory per partition</div>
</div>
</div>

<div class="border-l-2 border-pink-400 pl-5">
<div class="font-semibold text-pink-400 uppercase text-xs">trade-off</div>
<div class="pt-4">
<div class="text-2xl font-semibold text-pink-300">key fits in memory</div>
<div class="text-sm opacity-70 pt-2">a single key with millions of events can OOM the executor</div>
</div>
</div>

</div>

---

# UnionJoin topology

<div class="pt-2 text-xs opacity-60 text-center">click &rarr; to advance</div>

<div class="pt-2 flex justify-center">

<svg viewBox="0 0 760 360" style="width:60%">
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
      <path d="M0,0 L0,6 L9,3 z" fill="#9ca3af"/>
    </marker>
  </defs>

  <g v-click="1">
    <rect x="20" y="40" width="140" height="60" fill="rgba(96,165,250,0.18)" stroke="#60a5fa" rx="4"/>
    <text x="90" y="75" text-anchor="middle" style="font-size:14px" fill="#bfdbfe">left (queries)</text>
    <rect x="20" y="240" width="140" height="60" fill="rgba(244,114,182,0.18)" stroke="#f472b6" rx="4"/>
    <text x="90" y="275" text-anchor="middle" style="font-size:14px" fill="#fbcfe8">right (events)</text>
  </g>

  <g v-click="2">
    <path d="M165 70 Q 220 70 235 165" stroke="#9ca3af" fill="none" marker-end="url(#arr)"/>
    <path d="M165 270 Q 220 270 235 195" stroke="#9ca3af" fill="none" marker-end="url(#arr)"/>
    <rect x="245" y="140" width="120" height="60" fill="rgba(156,163,175,0.18)" stroke="#9ca3af" rx="4"/>
    <text x="305" y="175" text-anchor="middle" style="font-size:14px" fill="#e5e7eb">union</text>
  </g>

  <g v-click="3">
    <path d="M370 170 L 420 170" stroke="#9ca3af" fill="none" marker-end="url(#arr)"/>
    <rect x="425" y="100" width="160" height="40" fill="rgba(251,191,36,0.18)" stroke="#fbbf24" rx="4"/>
    <text x="505" y="125" text-anchor="middle" style="font-size:13px" fill="#fde68a">key=A: [t1, t3, t7, ...]</text>
    <rect x="425" y="155" width="160" height="40" fill="rgba(251,191,36,0.18)" stroke="#fbbf24" rx="4"/>
    <text x="505" y="180" text-anchor="middle" style="font-size:13px" fill="#fde68a">key=B: [t2, t4, t9, ...]</text>
    <rect x="425" y="210" width="160" height="40" fill="rgba(251,191,36,0.18)" stroke="#fbbf24" rx="4"/>
    <text x="505" y="235" text-anchor="middle" style="font-size:13px" fill="#fde68a">key=C: [t1, t5, t8, ...]</text>
    <text x="505" y="85" text-anchor="middle" style="font-size:11px" fill="#fcd34d">groupBy + time-sort</text>
  </g>

  <g v-click="4">
    <path d="M590 170 L 625 170" stroke="#9ca3af" fill="none" marker-end="url(#arr)"/>
    <rect x="630" y="140" width="110" height="60" fill="rgba(74,222,128,0.18)" stroke="#4ade80" rx="4"/>
    <text x="685" y="165" text-anchor="middle" style="font-size:13px" fill="#bbf7d0">sawtooth</text>
    <text x="685" y="183" text-anchor="middle" style="font-size:11px" fill="#86efac">all windows</text>
  </g>

</svg>

</div>

---

# Cost delta

<div class="grid grid-cols-2 gap-12 pt-6">

<div>
<div class="text-sm uppercase opacity-60">before</div>
<div class="text-3xl font-semibold text-pink-300">8 shuffles</div>
<div class="text-sm pt-2 opacity-80">
kryo-encoded Java objects, three cogroups, two re-keyings
</div>
</div>

<div>
<div class="text-sm uppercase opacity-60">after</div>
<div class="text-3xl font-semibold text-emerald-400">1 shuffle</div>
<div class="text-sm pt-2 opacity-80">
Spark-native exchange, then in-partition compute
</div>
</div>

</div>

<div class="pt-14 text-center">
<div class="text-5xl font-semibold text-emerald-400">~10&times; faster</div>
<div class="text-sm pt-3 opacity-70">in production &middot; UnionJoin is now the default for PITC aggregations</div>
</div>

---

# Crucible

<div class="pt-6 text-base">
Bring-your-own-cloud platform for Spark batch and Flink streaming. Single Helm chart into EKS / AKS / GKE.
</div>

<div class="pt-10 grid grid-cols-3 gap-6">

<div class="border-l-4 border-emerald-400 pl-4">
<div class="text-3xl font-semibold">16&times;</div>
<div class="text-sm pt-2 opacity-80">cheaper than Databricks Serverless</div>
</div>

<div class="border-l-4 border-blue-400 pl-4">
<div class="text-3xl font-semibold">your VPC</div>
<div class="text-sm pt-2 opacity-80">no cross-cloud egress</div>
</div>

<div class="border-l-4 border-purple-400 pl-4">
<div class="text-3xl font-semibold">list price</div>
<div class="text-sm pt-2 opacity-80">EC2 / VM rates, ARM spot eligible</div>
</div>

</div>

---

# The Spark-on-K8s tax

<div class="pt-6 text-base">
What you get out of the box:
</div>

<div class="pt-6 space-y-3">

<v-clicks>

- Cold start &mdash; minutes from submit to first task
- Shuffle on root-disk EBS / Premium SSD &mdash; slow + noisy
- No safe spot &mdash; one preemption kills the job
- No DRA without an external shuffle service (Celeborn, etc.)
- Driver evicted before executors when memory pressure hits

</v-clicks>

</div>

<div class="pt-8 text-base opacity-80">
The cost gap to Databricks-class platforms is mostly this tax.
</div>

---

# Lever 1: NVMe DaemonSet

<div class="pt-6 text-base">
A DaemonSet auto-discovers NVMe instance-store hardware on every node, formats it,
mounts it, labels and taints the node.
</div>

<div class="pt-6 space-y-3">

<v-clicks>

- Spark executors carry a toleration and nodeSelector for NVMe nodes &mdash; routed automatically
- Driver stays on regular nodes (no NVMe needed for driver)
- 3&times; faster shuffle I/O, isolated from root-disk noisy neighbors
- Per-job config: nothing

</v-clicks>

</div>

---

# Lever 2: DRA + decommission

<div class="pt-6 text-base">
DRA + decommission, no external shuffle service.
</div>

<div class="pt-6 space-y-3">

<v-clicks>

- <code>spark.dynamicAllocation.shuffleTracking.enabled = true</code> &mdash; DRA without an external shuffle service
- <code>spark.decommission.enabled</code> + <code>storage.decommission.shuffleBlocks.enabled</code> &mdash; graceful shuffle migration on 2-min spot notice
- <code>maxPendingPods = 50</code>, <code>allocation.batch.size = 50</code> &mdash; bounded burst on scale-up

</v-clicks>

</div>

---

# Lever 3: spot + driver pinning + warm pool

<div class="pt-6 text-base">
Spot for executors. On-demand for drivers. Pre-warmed nodes for fast scheduling.
</div>

<div class="pt-6 space-y-3">

<v-clicks>

- Executors opt into spot per job (<code>"spot": true</code>)
- Driver nodeSelector pins to on-demand &mdash; preemption can't kill the job
- Driver PriorityClass = 100, warm-pool pause pods = -1 &mdash; pause pods get evicted first, drivers schedule instantly
- Warm pool keeps a small set of driver-sized nodes hot

</v-clicks>

</div>

<div class="pt-6 text-sm opacity-60">
Sub-20s submit-to-first-task with warm pool on. ~5% spot interruption rate stops mattering &mdash; drivers stay on-demand, executors recover.
</div>

---

# Lever 4: ARM

<div class="pt-6 text-base">
Graviton3 (AWS), Cobalt 100 (Azure), Axion (GCP).
</div>

<div class="pt-10 grid grid-cols-2 gap-12">

<div>
<div class="text-3xl font-semibold">60&ndash;70%</div>
<div class="text-sm pt-2 opacity-80">cheaper than equivalent x86 spot</div>
</div>

<div>
<div class="text-3xl font-semibold">same code</div>
<div class="text-sm pt-2 opacity-80">multi-arch images &middot; no JVM porting</div>
</div>

</div>

<div class="pt-10 text-sm opacity-60">
ARM toleration on executors. Driver stays on whichever arch is cheapest. Tested nightly on all three clouds.
</div>

---

# Cluster defaults &middot; `baseDefaults`

<div class="pt-4 text-base opacity-80">
<code>baseDefaults</code> in <code>pkg/k8s/sparkconfig.go</code> &mdash; the defaults every job inherits.
</div>

<div class="pt-4 text-sm opacity-60">
[ SCREENSHOT / EXCERPT &mdash; AQE bundle, DRA, decommission, memoryOverheadFactor=0.15, lz4/zstd split, allocation tuning ]
</div>

<div class="pt-6 text-base">
Locked keys (cluster-controlled) vs admin overrides vs user overrides &mdash; precedence order is explicit.
</div>

---

# Recap

<div class="pt-8 grid grid-cols-3 gap-6">

<div class="border-l-4 border-blue-400 pl-4">
<div class="text-xs uppercase opacity-60">backfill</div>
<div class="text-xl font-semibold pt-1">3 shuffles &rarr; 1</div>
<div class="text-sm pt-2 opacity-80">one union + groupBy + mapPartitions</div>
</div>

<div class="border-l-4 border-purple-400 pl-4">
<div class="text-xs uppercase opacity-60">serving</div>
<div class="text-xl font-semibold pt-1">N reads &rarr; 1</div>
<div class="text-sm pt-2 opacity-80">push merge to write</div>
</div>

<div class="border-l-4 border-emerald-400 pl-4">
<div class="text-xs uppercase opacity-60">substrate</div>
<div class="text-xl font-semibold pt-1">x86 EBS &rarr; ARM NVMe spot</div>
<div class="text-sm pt-2 opacity-80">list-price hardware, no egress</div>
</div>

</div>

---

# Where the work is

<div class="pt-8 space-y-4 text-base">

<v-clicks>

- **UnionJoin** &mdash; <code>chronon</code> main &middot; <code>spark/.../join/UnionJoin.scala</code>
- **MegaTile** &mdash; <code>chronon</code> branch <code>nikhil/megatile</code> &middot; design doc <code>docs/source/megatile.md</code>
- **GigaTile** &mdash; <code>chronon</code> branch <code>nikhil/gigatile</code> &middot; design doc <code>docs/source/gigatile.md</code>
- **Crucible** &mdash; <code>crucible</code> &middot; <code>pkg/k8s/sparkconfig.go</code>, <code>helm/crucible</code>

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
