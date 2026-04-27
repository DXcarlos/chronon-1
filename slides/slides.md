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

<div class="border-l-4 border-blue-400 pl-4">
<div class="text-xs uppercase opacity-60">backfill</div>
<div class="text-2xl font-semibold pt-1">UnionJoin</div>
<div class="text-sm pt-2 opacity-80">collapse N shuffles into one</div>
</div>

<div class="border-l-4 border-purple-400 pl-4">
<div class="text-xs uppercase opacity-60">serving</div>
<div class="text-2xl font-semibold pt-1">Mega &rarr; GigaTile</div>
<div class="text-sm pt-2 opacity-80">push merge from read to write</div>
</div>

<div class="border-l-4 border-emerald-400 pl-4">
<div class="text-xs uppercase opacity-60">substrate</div>
<div class="text-2xl font-semibold pt-1">Crucible</div>
<div class="text-sm pt-2 opacity-80">right hardware, right pricing</div>
</div>

</div>

<div class="pt-12 text-center text-lg opacity-80">
Each one moves work off the read path or onto cheaper hardware.
</div>

---

# Roadmap

<div class="pt-8 space-y-6 text-lg">

<div v-click>
<span class="text-blue-400 font-semibold">Backfill</span> &mdash; UnionJoin <span class="opacity-50">(7 min)</span>
</div>

<div v-click>
<span class="text-purple-400 font-semibold">Serving</span> &mdash; MegaTile &rarr; GigaTile <span class="opacity-50">(15 min)</span>
</div>

<div v-click>
<span class="text-emerald-400 font-semibold">Substrate</span> &mdash; Crucible cluster <span class="opacity-50">(5 min)</span>
</div>

<div v-click class="pt-6 text-sm opacity-60">
Recap &middot; Q&amp;A
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

# Sawtooth &middot; the reuse insight

<div class="pt-4 flex justify-center">

<svg viewBox="0 0 1100 380" style="width:82%">
  <defs>
    <pattern id="tilesPat" x="0" y="0" width="11" height="30" patternUnits="userSpaceOnUse">
      <rect x="0.5" y="0" width="9" height="30" fill="rgba(251,191,36,0.32)"/>
      <line x1="10.2" y1="0" x2="10.2" y2="30" stroke="rgba(251,191,36,0.55)" stroke-width="0.4"/>
    </pattern>
  </defs>
  <text x="500" y="34" style="font-size:13px" fill="#cbd5e1" text-anchor="middle">for one key &middot; 7-day window &middot; 5-min tiles</text>
  <path d="M 60 100 L 60 92 L 720 92 L 720 100" stroke="#86efac" stroke-width="1.2" fill="none"/>
  <text x="390" y="80" style="font-size:13px" fill="#86efac" text-anchor="middle" font-weight="600">tail &mdash; sum of all tiles up to the bucket boundary</text>
  <rect x="60" y="135" width="660" height="30" fill="url(#tilesPat)"/>
  <text x="390" y="155" style="font-size:11px" fill="#fde68a" text-anchor="middle" font-weight="600">~2,000 five-min tiles</text>
  <text x="60" y="190" style="font-size:10px" fill="#64748b">7 days ago</text>
  <text x="715" y="190" style="font-size:10px" fill="#64748b" text-anchor="end">bucket boundary</text>
  <text x="900" y="190" style="font-size:10px" fill="#64748b" text-anchor="end">now</text>
  <line x1="720" y1="125" x2="720" y2="178" stroke="#64748b" stroke-dasharray="3,2" stroke-width="1"/>
  <rect x="725" y="135" width="155" height="30" fill="rgba(96,165,250,0.10)" stroke="#60a5fa" stroke-dasharray="3,2"/>
  <text x="802" y="125" style="font-size:11px" fill="#bfdbfe" text-anchor="middle">current 5-min bucket</text>
  <circle cx="743" cy="150" r="2.5" fill="#f472b6"/>
  <circle cx="755" cy="150" r="2.5" fill="#f472b6"/>
  <circle cx="772" cy="150" r="2.5" fill="#f472b6"/>
  <circle cx="788" cy="150" r="2.5" fill="#f472b6"/>
  <circle cx="803" cy="150" r="2.5" fill="#f472b6"/>
  <text x="900" y="154" style="font-size:10px" fill="#fbcfe8">head events</text>
  <polygon points="753,184 748,172 758,172" fill="#10b981"/>
  <polygon points="783,184 778,172 788,172" fill="#10b981"/>
  <polygon points="823,184 818,172 828,172" fill="#10b981"/>
  <text x="753" y="202" style="font-size:10px" fill="#86efac" text-anchor="middle">Q&#8321;</text>
  <text x="783" y="202" style="font-size:10px" fill="#86efac" text-anchor="middle">Q&#8322;</text>
  <text x="823" y="202" style="font-size:10px" fill="#86efac" text-anchor="middle">Q&#8323;</text>
  <path d="M 750 215 L 750 222 L 825 222 L 825 215" stroke="#86efac" stroke-width="0.8" fill="none"/>
  <text x="788" y="240" style="font-size:10px" fill="#86efac" text-anchor="middle">all 3 queries share the same tail</text>
  <text x="500" y="288" style="font-size:12px" fill="#94a3b8" text-anchor="middle">99.98% reuse within bucket &middot; 99.9% across adjacent buckets (1 tile in, 1 tile out)</text>
  <text x="500" y="335" style="font-size:24px" fill="#86efac" text-anchor="middle" font-weight="700">~1,000&times; compute saved by reuse</text>
</svg>

</div>

---

# Sawtooth &middot; the 3 layers

<div class="pt-4 flex justify-center">

<svg viewBox="0 0 1000 440" style="width:78%">
  <defs>
    <marker id="arrUp" markerWidth="10" markerHeight="10" refX="5" refY="9" orient="auto">
      <path d="M0,9 L5,2 L10,9 z" fill="#9ca3af"/>
    </marker>
  </defs>
  <rect x="60" y="20" width="880" height="100" fill="rgba(74,222,128,0.10)" stroke="#4ade80" rx="6"/>
  <text x="80" y="50" style="font-size:11px" fill="#86efac" font-weight="700">3 &middot; PER-QUERY IRs</text>
  <text x="80" y="78" style="font-size:14px" fill="#dcfce7">for each query in a bucket: combine tail + head events</text>
  <text x="80" y="103" style="font-size:12px" fill="#9ca3af" font-family="monospace">(key, tail_ir) join [queries] join [events] &rarr; results</text>
  <path d="M 500 145 L 500 130" stroke="#9ca3af" stroke-width="1.5" marker-end="url(#arrUp)" fill="none"/>
  <rect x="60" y="155" width="880" height="100" fill="rgba(251,191,36,0.10)" stroke="#fbbf24" rx="6"/>
  <text x="80" y="185" style="font-size:11px" fill="#fde68a" font-weight="700">2 &middot; TAIL IRs</text>
  <text x="80" y="213" style="font-size:14px" fill="#fef3c7">merge tiles into per-bucket tails &mdash; reuse across adjacent buckets</text>
  <text x="80" y="238" style="font-size:12px" fill="#9ca3af" font-family="monospace">(key, [tile_ir]) join (key, [tail_end_ts]) &rarr; (key, [tail_ir])</text>
  <path d="M 500 280 L 500 265" stroke="#9ca3af" stroke-width="1.5" marker-end="url(#arrUp)" fill="none"/>
  <rect x="60" y="290" width="880" height="100" fill="rgba(96,165,250,0.10)" stroke="#60a5fa" rx="6"/>
  <text x="80" y="320" style="font-size:11px" fill="#bfdbfe" font-weight="700">1 &middot; TILE IRs</text>
  <text x="80" y="348" style="font-size:14px" fill="#dbeafe">pre-aggregate events into 5-min tiles, one row per key</text>
  <text x="80" y="373" style="font-size:12px" fill="#9ca3af" font-family="monospace">(key, [event]) &rarr; (key, [tile_ir])</text>
</svg>

</div>

---

# Benefits &amp; cost

<div class="pt-12 grid grid-cols-2 gap-16">

<div class="border-l-2 border-emerald-400 pl-5">
<div class="font-semibold text-emerald-400 uppercase text-xs">benefits</div>
<div class="pt-4 space-y-5">
<div>
<div class="text-2xl font-semibold text-emerald-300">~1,000&times; compute reuse</div>
<div class="text-sm opacity-70 pt-1">99.98% within bucket &middot; 99.9% across adjacent buckets</div>
</div>
<div>
<div class="text-base font-semibold text-emerald-300">skew handling</div>
<div class="text-sm opacity-70 pt-1">a hot key's events + queries are split across machines, never piled on one</div>
</div>
</div>
</div>

<div class="border-l-2 border-pink-400 pl-5">
<div class="font-semibold text-pink-400 uppercase text-xs">cost</div>
<div class="pt-4">
<div class="text-2xl font-semibold text-pink-300">~8 shuffles</div>
<div class="text-sm opacity-70 pt-1">kryo-encoded Java objects, back and forth between two partitionings</div>
</div>
</div>

</div>

---

# The move

<div class="pt-6 text-2xl">
Union into one stream. groupBy + sort once. Sweep with the sawtooth.
</div>

<div class="pt-8 space-y-3 text-base">

- Union left + right into one stream of `(key, time, side)` rows
- Group by key &mdash; **one shuffle**
- Inside each partition: time-sort, sweep with the sawtooth aggregator, emit one row per query

</div>

<div class="pt-8 text-sm opacity-60">
The sawtooth was already invariant to which window we're computing. Run it once over the
unified per-key sequence and produce all windows in a single mapPartitions pass.
</div>

---

# UnionJoin algorithm walk

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

# Stage 1 &middot; Raw events in KV

<div class="pt-6 text-base">
Store events directly. Fetcher reads all events in the window and aggregates at read time.
</div>

<div class="pt-8 grid grid-cols-2 gap-12 text-sm">

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400 uppercase text-xs">pro</div>
<div class="opacity-80 pt-1">simple. always fresh — no aggregation lag.</div>
</div>

<div class="border-l-2 border-pink-400 pl-3">
<div class="font-semibold text-pink-400 uppercase text-xs">con</div>
<div class="opacity-80 pt-1">aggregation cost on every read. read latency scales with event volume.</div>
</div>

</div>

---

# Stage 2 &middot; Batch compaction

<div class="pt-4 text-base">
Spark precomputes the IR daily. <code>FinalBatchIr(collapsed, tailHops)</code>.
</div>

<div class="pt-6 flex justify-center">

<svg viewBox="0 0 760 200" style="width:50%">
  <rect x="40" y="40" width="200" height="120" fill="rgba(96,165,250,0.18)" stroke="#60a5fa" rx="4"/>
  <text x="140" y="35" text-anchor="middle" style="font-size:13px" fill="#bfdbfe" font-weight="600">collapsed</text>
  <text x="140" y="105" text-anchor="middle" style="font-size:12px" fill="#bfdbfe">aggregate over</text>
  <text x="140" y="123" text-anchor="middle" style="font-size:11px" fill="#9ca3af">[start, batchEnd &minus; tailBuffer)</text>

  <text x="280" y="35" text-anchor="middle" style="font-size:13px" fill="#fde68a" font-weight="600">tailHops &mdash; 576 nested at 5min</text>
  <g transform="translate(280, 50)">
    <g v-for="i in 36" :key="i">
      <rect :x="i * 12" y="0" width="9" height="100" fill="rgba(251,191,36,0.18)" stroke="#fbbf24" stroke-width="0.5"/>
    </g>
  </g>
</svg>

</div>

<div class="pt-4 text-sm opacity-70 text-center">
576 = 2 days &times; 24 hours &times; 12 (5-min slots). One KV value, deeply nested.
</div>

---

# Stage 3 &middot; Streaming tiling (today)

<div class="pt-4 text-base">
Flink writes per-event into hop-aligned tiles. Real-time on the head, batch covers the tail.
</div>

<div class="pt-8 grid grid-cols-2 gap-12 text-sm">

<div class="border-l-2 border-emerald-400 pl-3">
<div class="font-semibold text-emerald-400 uppercase text-xs">pro</div>
<div class="opacity-80 pt-1">real-time. write amplification bounded per event.</div>
</div>

<div class="border-l-2 border-pink-400 pl-3">
<div class="font-semibold text-pink-400 uppercase text-xs">con</div>
<div class="opacity-80 pt-1">read fan-out grows linearly with window. ~288 head tiles per day per tier.</div>
</div>

</div>

---

# The cost at scale

<div class="pt-12 grid grid-cols-3 gap-6 text-center">

<div>
<div class="text-5xl font-semibold text-pink-300">500+</div>
<div class="text-sm pt-3 opacity-80">head tiles<br/>per window per counter</div>
</div>

<div>
<div class="text-5xl font-semibold text-pink-300">&times; 1000</div>
<div class="text-sm pt-3 opacity-80">counters<br/>per query</div>
</div>

<div>
<div class="text-5xl font-semibold text-pink-300">&times; 1000</div>
<div class="text-sm pt-3 opacity-80">candidates<br/>per recsys query</div>
</div>

</div>

<div class="pt-14 text-center">
<div class="text-3xl font-semibold text-pink-400">= 500M tile fetches per query</div>
<div class="text-sm opacity-60 pt-3">read latency directly impacts topline metrics</div>
</div>

---

# KV stores without range scans

<div class="pt-4 text-base">
Most production KV backends don't range-scan efficiently. The fetcher enumerates tile
starts and issues N point gets per (entity, tier).
</div>

<div class="pt-6 flex justify-center">

<svg viewBox="0 0 760 240" style="width:50%">
  <defs>
    <marker id="arrFan" markerWidth="8" markerHeight="8" refX="7" refY="2.5" orient="auto">
      <path d="M0,0 L0,5 L7,2.5 z" fill="#9ca3af"/>
    </marker>
  </defs>

  <rect x="30" y="100" width="120" height="40" fill="rgba(244,114,182,0.18)" stroke="#f472b6" rx="4"/>
  <text x="90" y="125" text-anchor="middle" style="font-size:13px" fill="#fbcfe8">fetcher</text>

  <g stroke="#9ca3af" stroke-width="0.8" fill="none" opacity="0.7" marker-end="url(#arrFan)">
    <line x1="155" y1="118" x2="320" y2="20"/>
    <line x1="155" y1="118" x2="320" y2="50"/>
    <line x1="155" y1="118" x2="320" y2="80"/>
    <line x1="155" y1="118" x2="320" y2="110"/>
    <line x1="155" y1="118" x2="320" y2="140"/>
    <line x1="155" y1="118" x2="320" y2="170"/>
    <line x1="155" y1="118" x2="320" y2="200"/>
    <line x1="155" y1="118" x2="320" y2="230"/>
  </g>

  <g transform="translate(330, 0)">
    <rect x="0" y="10" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="25" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 00:05</text>
    <rect x="0" y="40" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="55" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 00:10</text>
    <rect x="0" y="70" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="85" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 00:15</text>
    <rect x="0" y="100" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="115" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 00:20</text>
    <text x="60" y="138" text-anchor="middle" style="font-size:11px" fill="#9ca3af">...</text>
    <rect x="0" y="160" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="175" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 23:50</text>
    <rect x="0" y="190" width="120" height="20" fill="rgba(244,114,182,0.10)" stroke="rgba(244,114,182,0.4)" rx="2"/>
    <text x="60" y="205" text-anchor="middle" style="font-size:10px" fill="#9ca3af">tile @ 23:55</text>
  </g>

  <text x="540" y="125" style="font-size:13px" fill="#fbcfe8" font-weight="600">N point gets</text>
  <text x="540" y="143" style="font-size:11px" fill="#9ca3af">per (entity, tier)</text>
</svg>

</div>

<div class="pt-4 text-sm opacity-60 text-center">
Latency is the dominant cost on the serving fleet, not throughput.
</div>

---

# MegaTile &middot; one head tile, not N

<div class="pt-4 text-base">
Collapse the streaming side. One daily entry per entity carries everything for windows &le; 48h
and an accumulator for larger windows.
</div>

<div class="pt-6 flex justify-center">

<svg viewBox="0 0 880 280" style="width:72%">
  <defs>
    <pattern id="manyHeadTiles" x="0" y="0" width="6" height="22" patternUnits="userSpaceOnUse">
      <rect x="0" y="0" width="3" height="22" fill="rgba(244,114,182,0.45)"/>
    </pattern>
  </defs>
  <text x="440" y="14" text-anchor="middle" style="font-size:11px" fill="#64748b">production hops: 5 min &middot; 1 day of head &rArr; 24 &times; 12 = 288</text>
  <text x="220" y="32" text-anchor="middle" style="font-size:14px" font-weight="600" fill="#fbcfe8">before</text>
  <rect x="40" y="80" width="100" height="22" fill="rgba(96,165,250,0.20)" stroke="#60a5fa" rx="2"/>
  <text x="90" y="96" text-anchor="middle" style="font-size:11px" fill="#bfdbfe">batchIr</text>
  <rect x="150" y="80" width="240" height="22" fill="url(#manyHeadTiles)" stroke="rgba(244,114,182,0.7)" rx="2"/>
  <text x="270" y="96" text-anchor="middle" style="font-size:11px" fill="#fbcfe8">~288 head tiles</text>
  <text x="220" y="135" text-anchor="middle" style="font-size:13px" fill="#fbcfe8">1 + ~288 reads</text>
  <text x="220" y="155" text-anchor="middle" style="font-size:11px" fill="#9ca3af">decode + merge each</text>

  <text x="660" y="32" text-anchor="middle" style="font-size:14px" font-weight="600" fill="#bbf7d0">after &mdash; MegaTile</text>
  <rect x="490" y="80" width="100" height="22" fill="rgba(96,165,250,0.20)" stroke="#60a5fa" rx="2"/>
  <text x="540" y="96" text-anchor="middle" style="font-size:11px" fill="#bfdbfe">batchIr</text>
  <rect x="600" y="80" width="80" height="22" fill="rgba(74,222,128,0.22)" stroke="#4ade80" rx="2"/>
  <text x="640" y="96" text-anchor="middle" style="font-size:11px" fill="#bbf7d0">today</text>
  <rect x="690" y="80" width="80" height="22" fill="rgba(74,222,128,0.12)" stroke="#4ade80" rx="2"/>
  <text x="730" y="96" text-anchor="middle" style="font-size:11px" fill="#bbf7d0">yesterday</text>
  <text x="660" y="135" text-anchor="middle" style="font-size:13px" fill="#bbf7d0" font-weight="600">3 reads</text>
  <text x="660" y="155" text-anchor="middle" style="font-size:11px" fill="#9ca3af">small windows: latest only</text>
  <text x="660" y="172" text-anchor="middle" style="font-size:11px" fill="#9ca3af">large windows: merge with batch</text>

  <line x1="440" y1="40" x2="440" y2="200" stroke="#374151" stroke-dasharray="2,3"/>

  <text x="440" y="240" text-anchor="middle" style="font-size:13px" fill="#86efac" font-weight="600">
    head: 1 daily read &middot; tail still inside batchIr
  </text>
</svg>

</div>

---

# How MegaTile splits windows

<div class="pt-6 grid grid-cols-2 gap-12">

<div class="border-l-4 border-blue-400 pl-4">
<div class="text-xs uppercase opacity-60">small windows</div>
<div class="text-base pt-1 font-semibold">window &le; 48h</div>
<div class="text-sm pt-3 opacity-80">
Self-contained in today's daily entry. Latest entry is the answer.
</div>
</div>

<div class="border-l-4 border-purple-400 pl-4">
<div class="text-xs uppercase opacity-60">large windows + unwindowed</div>
<div class="text-base pt-1 font-semibold">window &gt; 48h</div>
<div class="text-sm pt-3 opacity-80">
Daily accumulator (today + yesterday) merged with batch collapsed + tail hops at fetch time.
</div>
</div>

</div>

<div class="pt-10 text-sm opacity-60">
48h matches <code>tailBuffer</code>. Windows &lt; 2d aren't safe to move into batch when batch
is delayed by more than 2 days &mdash; coverage gap.
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

- Cold start &mdash; minutes from submit to first task
- Shuffle on root-disk EBS / Premium SSD &mdash; slow + noisy
- No safe spot &mdash; one preemption kills the job
- No DRA without an external shuffle service (Celeborn, etc.)
- Driver evicted before executors when memory pressure hits

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

- Spark executors carry a toleration and nodeSelector for NVMe nodes &mdash; routed automatically
- Driver stays on regular nodes (no NVMe needed for driver)
- 3&times; faster shuffle I/O, isolated from root-disk noisy neighbors
- Per-job config: nothing

</div>

---

# Lever 2: DRA + decommission

<div class="pt-6 text-base">
DRA + decommission, no external shuffle service.
</div>

<div class="pt-6 space-y-3">

- <code>spark.dynamicAllocation.shuffleTracking.enabled = true</code> &mdash; DRA without an external shuffle service
- <code>spark.decommission.enabled</code> + <code>storage.decommission.shuffleBlocks.enabled</code> &mdash; graceful shuffle migration on 2-min spot notice
- <code>maxPendingPods = 50</code>, <code>allocation.batch.size = 50</code> &mdash; bounded burst on scale-up

</div>

---

# Lever 3: spot + driver pinning + warm pool

<div class="pt-6 text-base">
Spot for executors. On-demand for drivers. Pre-warmed nodes for fast scheduling.
</div>

<div class="pt-6 space-y-3">

- Executors opt into spot per job (<code>"spot": true</code>)
- Driver nodeSelector pins to on-demand &mdash; preemption can't kill the job
- Driver PriorityClass = 100, warm-pool pause pods = -1 &mdash; pause pods get evicted first, drivers schedule instantly
- Warm pool keeps a small set of driver-sized nodes hot

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

- **UnionJoin** &mdash; <code>chronon</code> main &middot; <code>spark/.../join/UnionJoin.scala</code>
- **MegaTile** &mdash; <code>chronon</code> branch <code>nikhil/megatile</code> &middot; design doc <code>docs/source/megatile.md</code>
- **GigaTile** &mdash; <code>chronon</code> branch <code>nikhil/gigatile</code> &middot; design doc <code>docs/source/gigatile.md</code>
- **Crucible** &mdash; <code>crucible</code> &middot; <code>pkg/k8s/sparkconfig.go</code>, <code>helm/crucible</code>

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
