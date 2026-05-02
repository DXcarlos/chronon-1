# Chat Guardrails: Safe LLM Apps with Chronon

This guide shows how to build a safety guardrail for an LLM chat application using
Chronon's `Join`, `Model`, and `ModelTransforms` primitives. The output is a single
fetcher endpoint your chat app calls at three points — after each user turn, after
the model proposes a tool call, and (defense in depth) before the tool runs — to
get a structured `safety_verdict` it can use to refuse, route, or proceed.

The guide assumes familiarity with Chronon's `EventSource`, `GroupBy`, and `Join`.
If those are new, read [GroupBy.md](GroupBy.md) and [Join.md](Join.md) first.

## TL;DR

Compose the guardrail as a `ModelTransforms` whose source is a `Join`:

```
chat events  ──►  GroupBy(LAST_K of messages)
                          │
user request ──►  Join (left: chat-event timeline; right: history GroupBy)
                          │
                  ModelTransforms (sources=[JoinSource(join)], models=[judge_llm])
                          │
                          ▼
                  fetcher endpoint  ──►  your chat app
```

Your chat app sends `{user_id, session_id, current_message, pending_tool_call}`
to the fetcher at each gate. The fetcher returns
`{safety_verdict, safety_categories, safety_score, ...}`. Your app reads
`safety_verdict` and acts on it. Chronon does not block the LLM; your app does.

## Why this layering matters: the threat model

Single-turn safety screening is not enough. Three failure modes drive the design:

- **Multi-turn jailbreaks.** Crescendo (Russinovich et al., 2024,
  [arXiv:2404.01833](https://arxiv.org/abs/2404.01833)) reaches up to 98% success
  against single-turn judges by escalating from benign first turns. The judge
  needs conversation context, not just the latest message.
- **Indirect prompt injection.** Greshake et al., 2023
  ([arXiv:2302.12173](https://arxiv.org/abs/2302.12173); AISec '23 at CCS) showed
  that malicious instructions hidden in retrieved content or tool outputs steer
  agents into actions the user never asked for. Real-world benchmarks confirm
  this is an ongoing failure mode: AgentDojo (Debenedetti et al., 2024,
  [arXiv:2406.13352](https://arxiv.org/abs/2406.13352); NeurIPS 2024 D&B) and
  InjecAgent (Zhan et al., 2024,
  [arXiv:2403.02691](https://arxiv.org/abs/2403.02691); ACL Findings 2024). The
  judge needs to inspect *planned tool calls*, not just user prompts.
- **Long-context dilution.** LongSafetyBench (Huang et al., 2024,
  [arXiv:2411.06899](https://arxiv.org/abs/2411.06899)), LongSafety (Lu et al.,
  2025, [arXiv:2502.16971](https://arxiv.org/abs/2502.16971)), and Liu et al.,
  2025 ([arXiv:2510.05864](https://arxiv.org/abs/2510.05864)) all show that
  harmful content interleaved in long inputs is missed by current LLM judges.
  When history is long, summarize before judging.

The three layers below address these in order.

## Primitives at a glance

Real signatures from `python/src/ai/chronon/model.py` and
`python/src/ai/chronon/source.py`:

```python
from ai.chronon.types import (
    Model, ModelTransforms, InferenceSpec, ModelBackend,
    Source, EventSource, EntitySource, JoinSource,
    Query, selects,
)

InferenceSpec(
    model_backend=ModelBackend.VERTEXAI,           # VertexAI | SageMaker
    model_backend_params={"endpoint_id": "..."},   # passed to your ModelPlatform impl
)

Model(
    version="1.0",
    inference_spec=...,
    input_mapping={"text": "concat(history_summary, current_message)"},   # Spark SQL
    value_fields=[("verdict", DataType.STRING),
                  ("score",   DataType.DOUBLE)],
    # output_mapping (optional): Spark SQL post-processing over `<modelName>__field`.
    # Omitted in this guide — value_fields already names the columns directly.
)

ModelTransforms(
    sources=[Source(...) | JoinSource(...) | another_ModelTransforms],
    models=[model_a, model_b, ...],     # parallel within one block
    passthrough_fields=["user_id", ...],
    version=1,
)
```

Two semantics worth pinning down up-front:

1. **Models inside one `ModelTransforms` run in parallel**, not in chain. Each
   model receives the same source row; outputs from one are not visible to
   another at inference time. Parallel composition is for ensembles or for
   producing multiple independent features in one pass. (See
   `online/.../ModelTransformsFetcher.scala:105` — `Future.sequence` over
   per-model futures.)
2. **`ModelTransforms` is itself a `Source`**. The `ANY_SOURCE_TYPE` union in
   `python/src/ai/chronon/utils.py:30-31` includes `ModelTransforms`, so a
   downstream `ModelTransforms`, `GroupBy`, or `Join` can read from an upstream
   `ModelTransforms`. This is how you chain sequentially — one model's output
   feeds the next model's input.

## The chat event source

You need a single event log that captures every turn. One row per turn, partitioned
daily, with a Kafka topic for streaming.

```python
# events/chat/turns.py
from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.types import Query, selects

chat_turns = Source(
    events=EventSource(
        table="chat.turns",                    # Hive/BQ table, ds-partitioned
        topic="kafka://chat.turns",            # streaming source — required for TEMPORAL
        query=Query(
            selects=selects(
                "user_id",
                "session_id",
                "role",                        # 'user' | 'assistant' | 'tool'
                "message",                     # text content
                tool_call="tool_call_json",    # alias: read from tool_call_json column
            ),
            time_column="ts",
        ),
    )
)
```

What flows in: every user turn, every assistant turn (including pending tool
calls before they execute), and every tool result. The guardrail itself does not
need to consume tool results, but they're useful for downstream review and for
detecting indirect injection patterns over time.

## Recent-history feature

```python
# group_bys/chat/recent_messages.py
from ai.chronon.types import GroupBy, Aggregation, Operation, Accuracy
from events.chat.turns import chat_turns

# Last 20 messages per user, in a 7-day window.
# Tighten the key to (user_id, session_id) if you don't want cross-session memory.
recent_messages = GroupBy(
    sources=[chat_turns],
    keys=["user_id"],
    online=True,
    version=0,
    accuracy=Accuracy.TEMPORAL,    # streaming-fresh; requires a topic on the source
    aggregations=[
        Aggregation(
            input_column="message",
            operation=Operation.LAST_K(20),
            windows=["7d"],
        ),
    ],
)
```

`Operation.LAST_K(20)` returns the most recent 20 messages in the window. If you
keep `(user_id, session_id)` as keys, the feature represents only the current
conversation; if `user_id` only, it persists across sessions and is what catches
Crescendo-style escalation that spans a reconnect.

The aggregation produces a column whose name is auto-generated from
`{group_by_name}_{input_column}_{operation}_{window}`. Below we use a
`Derivation` on the Join to alias it to a stable `chat_history` field so the
downstream `Model` doesn't depend on the auto-name.

## Layer 1: stateless judge (current message only)

The minimum viable guardrail. Useful as a cheap pre-screen before deeper layers
or as the only check on systems that don't yet maintain chat history.

```python
# models/safety/judge_v1.py
from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.types import (
    InferenceSpec, Model, ModelBackend, ModelTransforms,
    Query, selects, DataType,
)

# Per-request input shape — the chat app passes these as `keys` at fetch time.
# Modeled here as an EventSource only so the schema is explicit.
request_source = Source(
    events=EventSource(
        table="chat.guardrail_requests",        # logging table; live requests bypass it
        query=Query(
            selects=selects(
                "user_id",
                "session_id",
                "current_message",
            ),
            time_column="ts",
        ),
    )
)

judge_v1 = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "endpoint_id": "gemini-3-1-flash-lite",   # cheap + fast classifier
            "judge_kind":  "guardrail",
        },
    ),
    input_mapping={
        # SQL over request keys at fetch time (or source columns at backfill time).
        # The model's ModelPlatform receives a dict with these keys.
        "text": "current_message",
    },
    value_fields=[
        ("safety_verdict",    DataType.STRING),
        ("safety_categories", DataType.LIST(DataType.STRING)),
    ],
)

guardrail_v1 = ModelTransforms(
    sources=[request_source],
    models=[judge_v1],
    passthrough_fields=["user_id", "session_id"],
    version=1,
)
```

What it stops: blatant single-turn harmful prompts the guardrail model was
trained to recognise.

What it misses: Crescendo-style multi-turn escalation; indirect injection that
arrives via tool outputs.

Cost: one LLM call per gated turn.

## Layer 2: + recent history

Add the `recent_messages` GroupBy to the input. The judge now sees the last K
messages plus the current one.

```python
# joins/safety/context_v2.py
from ai.chronon.types import Join, JoinPart, Derivation
from events.chat.turns import chat_turns
from group_bys.chat.recent_messages import recent_messages

context_v2 = Join(
    left=chat_turns,
    right_parts=[
        JoinPart(group_by=recent_messages, key_mapping={"user_id": "user_id"}),
    ],
    derivations=[
        # Star to keep all base columns, plus a stable alias for the LAST_K output.
        # Replace <auto_name> with the exact auto-generated column reported by
        # `compile.py --conf=joins/safety/context_v2.py` or the analyzer.
        # Convention: {group_by_name}_{input_column}_last_k_{k}_{window}
        Derivation(name="*",            expression="*"),
        Derivation(name="chat_history", expression="<auto_name>"),
    ],
    row_ids=["user_id", "session_id", "ts"],
    version=2,
    online=True,
)
```

```python
# models/safety/judge_v2.py
from gen_thrift.api.ttypes import JoinSource, Source
from joins.safety.context_v2 import context_v2
from ai.chronon.types import (
    InferenceSpec, Model, ModelBackend, ModelTransforms,
    Query, selects, DataType,
)

context_source = Source(
    joinSource=JoinSource(
        join=context_v2,
        query=Query(selects=selects("user_id", "session_id")),
    )
)

judge_v2 = Model(
    version="2",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={"endpoint_id": "gemini-3-1-flash-lite"},
    ),
    input_mapping={
        # The Join's output (resolved at fetch time) merged with request keys is
        # the input. `chat_history` is the Derivation alias on the Join above;
        # `current_message` arrives via request.keys.
        "history": "chat_history",
        "current": "current_message",
    },
    value_fields=[
        ("safety_verdict",    DataType.STRING),
        ("safety_categories", DataType.LIST(DataType.STRING)),
    ],
)

guardrail_v2 = ModelTransforms(
    sources=[context_source],
    models=[judge_v2],
    passthrough_fields=["user_id", "session_id"],
    version=2,
)
```

What it adds over Layer 1: defense against multi-turn escalation. The judge can
recognise Crescendo-style patterns where each turn looks benign in isolation.

Cost: one LLM call; longer prompt (K turns of context).

## Layer 3: + pre-execution tool-call review

Same Join, plus a `pending_tool_call` field passed in at fetch time. Call the
fetcher *after* the assistant generates a tool call and *before* you execute it.

You don't need a new Join for this; it's the same `context_v2` plus a request
field the chat app sends. The judge's `input_mapping` includes the tool call:

```python
# models/safety/judge_v3.py — same as judge_v2 but with tool-call awareness
judge_v3 = Model(
    version="3",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={"endpoint_id": "gemini-3-1-flash-lite"},
    ),
    input_mapping={
        "history":   "chat_history",
        "current":   "current_message",
        # Empty string when called post-user-turn; populated when called
        # post-tool-generation. Your ModelPlatform impl decides how to template.
        "tool_call": "coalesce(pending_tool_call, '')",
    },
    value_fields=[
        ("safety_verdict",    DataType.STRING),
        ("safety_categories", DataType.LIST(DataType.STRING)),
        ("blocked_action",    DataType.STRING),   # which tool/arg triggered, if any
    ],
)

guardrail_v3 = ModelTransforms(
    sources=[context_source],            # same JoinSource as v2
    models=[judge_v3],
    passthrough_fields=["user_id", "session_id"],
    version=3,
)
```

Call sites:

| When                              | What the chat app sends                     | What it does with verdict |
| --------------------------------- | ------------------------------------------- | ------------------------- |
| After a user turn                 | `current_message`, `pending_tool_call=null` | Refuse / continue          |
| After model generates a tool call | `current_message`, `pending_tool_call=<the JSON>` | Block / continue          |
| Pre-tool-invocation (defense in depth) | Same as previous                       | Block / continue          |

What it adds over Layer 2: tool-payload inspection. The judge sees what the
agent is about to *do*, not just what was said. This is the layer that defends
against indirect injection — content the model retrieved from a webpage,
document, or other tool output that has steered it into a malicious tool call.

Cost: one LLM call per planned tool invocation. Latency budget compounds; see
"Operational notes" below.

## Advanced: chained summarize → judge

Layer 2 sends the raw last-K messages to the judge. When K is large or messages
are long, this dilutes the signal: the judge can miss harmful content embedded
in a long context, as documented in LongSafetyBench
([arXiv:2411.06899](https://arxiv.org/abs/2411.06899)) and Liu et al., 2025
([arXiv:2510.05864](https://arxiv.org/abs/2510.05864)).

The mitigation is to summarize history first, then judge. Two approaches:

### Option A: chain via the Source union (recommended)

`ModelTransforms` is itself a `Source`, so a downstream `ModelTransforms` can
consume an upstream one. Pattern:

```python
# models/safety/summarized_v4.py
from ai.chronon.types import (
    InferenceSpec, Model, ModelBackend, ModelTransforms, DataType,
)
from gen_thrift.api.ttypes import JoinSource, Source
from joins.safety.context_v2 import context_v2

context_source = Source(
    joinSource=JoinSource(
        join=context_v2,
        query=Query(selects=selects("user_id", "session_id")),
    )
)

summarizer = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "endpoint_id": "gemini-3-1-flash-lite",
            "judge_kind":  "summarizer",
        },
    ),
    input_mapping={"history": "chat_history"},
    value_fields=[("history_summary", DataType.STRING)],
)

summarized_context = ModelTransforms(
    sources=[context_source],
    models=[summarizer],
    passthrough_fields=["user_id", "session_id", "current_message", "pending_tool_call"],
    version=1,
)

# Chain: judge consumes summarizer's output as a Source.
judge_v4 = Model(
    version="4",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={"endpoint_id": "gemini-3-1-flash-lite"},
    ),
    input_mapping={
        # The upstream ModelTransforms produced a `<summarizerName>__history_summary`
        # column; reference it here. The exact column name is the summarizer Model's
        # auto-generated name plus `__history_summary`. See the naming note below.
        "history":   "<summarizer_name>__history_summary",
        "current":   "current_message",
        "tool_call": "coalesce(pending_tool_call, '')",
    },
    value_fields=[
        ("safety_verdict",    DataType.STRING),
        ("safety_categories", DataType.LIST(DataType.STRING)),
    ],
)

guardrail_v4 = ModelTransforms(
    sources=[summarized_context],              # <-- chained MT
    models=[judge_v4],
    passthrough_fields=["user_id", "session_id"],
    version=4,
)
```

Your chat app fetches `guardrail_v4`. The fetcher resolves the chain — fetch the
join, run the summarizer, run the judge — and returns the verdict.

### Option B: precompute the summary as a feature

If summarization is expensive and the conversation evolves slowly, materialise
the summary as a feature via streaming, and have the judge read it as a
`JoinPart`. This trades freshness for cost. Use a Flink job to recompute the
summary every N turns, write it to the KV store, and add a dedicated
summary GroupBy to the join's right_parts.

This works but adds a moving piece (the streaming summarizer job) and a
freshness window (the summary lags the last few turns). Use Option A unless
you've measured that summarization latency is a problem at your traffic.

### Honest caveat

There is published evidence that *raw* long history dilutes a safety judge's
signal. There is, as of writing, **no peer-reviewed paper showing that a
summarized history outperforms a truncated raw history specifically for a
safety classifier**. The motivation for summarizing is the dilution evidence
plus general intuition; the head-to-head benchmark is folklore. If your
deployment depends on the claim, run your own ablation on a held-out attack
set (HarmBench, AgentDojo) and decide based on measured AUPRC.

## Choosing the guardrail model

A side-channel guardrail has three constraints:

1. **Latency** must fit under the user-perceptible budget (≤300ms for the
   user-turn gate; ≤800ms for the tool-call gate where the alternative is
   running the tool itself).
2. **Cost** must be small enough that you can run it on every turn without
   second-guessing the budget.
3. **Capability** must be enough to follow a structured-output prompt and emit
   a verdict in your category schema.

The recommended default for both the summarizer and the judge is **Gemini 3.1
Flash-Lite** on Vertex AI:

| Model | Input / Output (per 1M tokens) | TTFT | Output speed | Use |
| --- | --- | --- | --- | --- |
| **Gemini 3.1 Flash-Lite** | $0.25 / $1.50 | ~0.3s | ~400 tps | Default judge + summarizer |
| Gemini 2.5 Flash-Lite | $0.10 / $0.40 | ~0.3s | ~390 tps | Cheaper floor; older but stable |
| Gemini 3.1 Flash | ~mid | ~0.4s | ~300 tps | Higher quality, ~3× cost |
| Gemini 3.1 Pro | $4.00 / higher | ~1s+ | lower | Reserve for offline review only |

Source: [Gemini 3.1 Flash-Lite announcement](https://blog.google/innovation-and-ai/models-and-research/gemini-models/gemini-3-1-flash-lite/),
[Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing),
[Artificial Analysis benchmarks](https://artificialanalysis.ai/models/gemini-2-5-flash-lite).

Why 3.1 Flash-Lite for the judge: Google explicitly markets it for "real-time
translation, classification, rapid tagging, high-volume consumer-facing tasks,
and latency-critical integrations" — that is the use case. It runs 2.5×
faster TTFT than 2.5 Flash with comparable or better quality, and at 1/6th the
output cost of 2.5 Flash.

A representative cost: a Layer 3 gate (history + current + tool call) sends
~2K input tokens and receives ~50 output tokens. At Gemini 3.1 Flash-Lite
pricing that's $0.0005 + $0.000075 ≈ **$0.0006 per gate** — under a tenth of
a cent. Two gates per turn (user-turn + tool-call) ≈ $1.20 per 1000 turns.

### When to use a purpose-built guardrail instead

Gemini is a general LLM you prompt to classify. Purpose-built guardrails are
small models trained directly on safety-classification tasks. Consider these
if you've measured that a prompted Gemini judge has poor recall on your
attack distribution, or if you need the AILuminate taxonomy out of the box:

- **ShieldGemma** (Zeng et al. 2024,
  [arXiv:2407.21772](https://arxiv.org/abs/2407.21772)) — 2B/9B/27B Gemma-2
  derivatives, hostable on Vertex AI Model Garden. The 2B is in the same
  latency class as Flash-Lite and emits the MLCommons categories directly.
- **Llama Guard 3** family (Inan et al. 2023,
  [arXiv:2312.06674](https://arxiv.org/abs/2312.06674); Llama Guard 3-1B-INT4,
  Liu et al. 2024, [arXiv:2411.17713](https://arxiv.org/abs/2411.17713)) —
  reference baseline; the 1B-INT4 fits on-device pre-screening.
- **WildGuard** (Han et al. 2024,
  [arXiv:2406.18495](https://arxiv.org/abs/2406.18495); NeurIPS 2024) —
  measured to reduce jailbreak success from 79.8% → 2.4% as an interface filter.
- **Granite Guardian** (Padhi et al. 2024,
  [arXiv:2412.07724](https://arxiv.org/abs/2412.07724)) — native RAG-failure
  detection (groundedness, context-relevance) alongside safety.

You wire any of these into the same `Model(...)` config — they live behind
the `endpoint_id` your `ModelPlatform` resolves. Hosting cost is your problem.

### Category taxonomy

Use **MLCommons AILuminate v1.0** (Ghosh et al., 2025,
[arXiv:2503.05731](https://arxiv.org/abs/2503.05731)): twelve hazard categories
(violent crimes, non-violent crimes, sex-related, CSAM, indiscriminate weapons,
suicide/self-harm, IP, privacy, defamation, hate, sexual, specialized advice).
This is what Llama Guard 3, ShieldGemma, and others already emit; for a Gemini
prompted judge, instruct it to return one of these codes. Don't invent your
own taxonomy unless you have a reason MLCommons doesn't cover.

> Note: **Prompt Guard** (Meta's small DeBERTa injection classifier) and
> **Llama Guard 4** are documented in vendor materials but not in
> peer-reviewed literature as of writing. If you need an academic citation
> covering Prompt Guard's behaviour, cite InjecGuard (Li et al., 2024,
> [arXiv:2410.22770](https://arxiv.org/abs/2410.22770)), which benchmarks it
> and shows over-defense issues.

## A note on output column naming

Every `Model`'s output columns are prefixed with the model's auto-generated
name when they leave `ModelTransforms`. So a `Model` with `value_fields=[(
"safety_verdict", STRING)]` becomes a feature named
`<model_name>__safety_verdict` in the fetcher response and in offline tables.

The `<model_name>` is set by Chronon at compile time from the file path,
variable name, and version of the `Model` (see `__set_name` in
`python/src/ai/chronon/utils.py`). Concretely, a `judge_v1 = Model(version="1", ...)`
in `models/safety/judge_v1.py` will get a name like `safety_judge_v1__1` after
sanitization. Run `compile.py --conf=<your_model_transforms.py>` and inspect the
emitted JSON to see the exact name; or read it from the analyzer.

This matters for two cases:

1. **Chained ModelTransforms.** The downstream Model's `input_mapping` SQL must
   reference the upstream Model's prefixed output columns
   (`<upstream_name>__<value_field>`).
2. **Reading the response in your chat app.** The response keys are also
   prefixed. Resolve them once at startup rather than hardcoding throughout.

## Plugging in your LLM provider

Chronon dispatches inference through a `ModelPlatform` implementation, selected
by the `ModelBackend` enum and `model_backend_params` you set on the
`InferenceSpec`. The interface is at
`online/src/main/scala/ai/chronon/online/Api.scala` (look for the
`ModelPlatformProvider` and `ModelPlatform` traits).

A `ModelPlatform` receives a `PredictRequest(model, inputs: Seq[Map[String, AnyRef]])`
and returns a `PredictResponse(outputs: Try[Seq[Map[String, AnyRef]]])`. Your
implementation:

1. Reads `model.inferenceSpec.modelBackendParams` (e.g.,
   `endpoint_id`, `judge_kind`) to pick the right LLM endpoint.
2. Reads `model.metaData.name` if you want per-model dispatch logic.
3. For each input map, builds a provider-specific request body. For Gemini on
   Vertex AI, that means assembling the `history`, `current`, and `tool_call`
   fields into a structured prompt and calling
   `generateContent` with `responseMimeType: "application/json"` and a
   response schema matching your `value_fields`.
4. Issues the call, parses the response back into a `Map[String, AnyRef]` whose
   keys match the `value_fields` you declared on the `Model`.
5. Returns the bulk response.

A minimal sketch (Scala) for a Gemini-backed guardrail:

```scala
class GeminiGuardrailPlatform(client: VertexGeminiClient) extends ModelPlatform {
  override def predict(req: PredictRequest): Future[PredictResponse] = {
    val params = req.model.inferenceSpec.modelBackendParams.asScala.toMap
    val endpointId = params.getOrElse("endpoint_id", "gemini-3-1-flash-lite")
    val judgeKind  = params.getOrElse("judge_kind", "guardrail")

    val futures = req.inputs.map { in =>
      val (system, user, schema) = judgeKind match {
        case "summarizer" => SummarizerPrompt.render(in)
        case "guardrail"  => SafetyJudgePrompt.render(in)  // emits AILuminate codes
      }
      client.generateContent(
        model            = endpointId,
        systemInstruction = system,
        userMessage      = user,
        responseSchema   = schema,            // structured output
        thinkingBudget   = 0,                 // off for latency-sensitive paths
        maxOutputTokens  = 256,
      ).map(parseJsonToMap)
    }
    Future.sequence(futures).map(outs => PredictResponse(req, Success(outs)))
  }
}
```

Two Gemini-specific knobs that matter for a side channel:

- **`responseSchema`** + `responseMimeType="application/json"`. Forces the
  model to emit JSON conforming to your `value_fields` schema. Dramatically
  reduces parse-error rates compared to free-form output.
- **`thinkingBudget=0`** (Gemini 3.x family). Disables the model's internal
  chain-of-thought, which is unnecessary for a single classification and
  costs latency + output tokens. Set to a small positive value only if you've
  measured that classification quality drops without it.

The `parseJsonToMap` step must produce keys that match your `value_fields`.
If your judge returns `{"safety_verdict": "unsafe", "safety_categories": ["S2","S6"]}`
and your `value_fields` say `[("safety_verdict", STRING), ("safety_categories", LIST(STRING))]`,
the keys line up. Chronon prefixes them with the model name as described in
the naming note above.

To swap providers — for example, to host **ShieldGemma 2B** on Vertex Model
Garden and call it instead of Gemini — keep the `Model(...)` config unchanged
and route on `endpoint_id` inside your `ModelPlatform`. To wire SageMaker, swap
the backend and params. To add a new backend altogether (e.g., direct OpenAI),
extend the `ModelBackend` enum (`thrift/api.thrift`) and register a new
`ModelPlatform`.

## Calling the API from your chat app

The chat app fetches the guardrail at three call sites. The conceptual API is
the standard Chronon fetcher; what matters is the request shape:

```python
# Pseudocode against the Chronon fetcher client.
# JUDGE_PREFIX is the auto-generated model name; resolve it once at startup.
JUDGE_PREFIX = "safety_judge_v3__3"   # example; actual value from compile output

def safety_check(user_id, session_id, current_message, pending_tool_call=None):
    response = chronon.fetch(
        name="safety/guardrail.v3",
        keys={
            "user_id":            user_id,
            "session_id":         session_id,
            "current_message":    current_message,
            "pending_tool_call":  pending_tool_call or "",
        },
    )
    v = response.values   # Map[String, AnyRef]
    return {
        "verdict":     v[f"{JUDGE_PREFIX}__safety_verdict"],
        "categories":  v[f"{JUDGE_PREFIX}__safety_categories"],
        "blocked":     v.get(f"{JUDGE_PREFIX}__blocked_action"),
        "exception":   v.get(f"{JUDGE_PREFIX}_exception"),   # see Failure mode below
    }

# Gate 1: post user turn
v = safety_check(user_id, session_id, user_msg)
if v["verdict"] == "unsafe":
    return refuse(reason=v["categories"])

# Gate 2: post tool-call generation, pre tool invocation
proposed = llm.generate(user_msg, history)
if proposed.tool_call:
    v = safety_check(user_id, session_id, user_msg, proposed.tool_call)
    if v["verdict"] == "unsafe":
        return refuse(reason=v["categories"], blocked=v["blocked"])

# Proceed with execution
result = tools.execute(proposed.tool_call)
```

The same fetcher endpoint serves all three gates because the input shape is
the same (the second and third gates just populate `pending_tool_call`). If
you'd rather expose three named endpoints, define three `ModelTransforms`
versions sharing the same join.

## Operational notes

**Latency budget.** Every layer adds at least one LLM round-trip. With Gemini
3.1 Flash-Lite (TTFT ~0.3s, ~400 tps) the realistic budget per gate:

- Gate 1 (post user turn): ~300-500ms end-to-end, dominated by network +
  TTFT; user-perceptible but acceptable on every turn.
- Gate 2 (post tool-call gen): ~400-700ms end-to-end with the larger prompt
  (history + current + tool call); runs *before* tool execution, so it
  overlaps with what would have been a tool RTT anyway.
- Chained summarize → judge (Layer 4): summarizer + judge in series, so
  budget ~700ms-1.2s. Avoid for Gate 1 if your application is latency-sensitive;
  reserve for sessions with very long history or for batched offline review.

**Failure mode.** When a model call fails, the fetcher writes
`<model_name>_exception` into the response (see
`ModelTransformsFetcher.scala:113`). Your chat app must decide upfront whether
to fail-open (let the turn through, log) or fail-closed (refuse, alert).
Default to fail-closed for tool-call gates, fail-open for the user-turn gate
unless you have a strong threat model.

**Caching and dedup.** The fetcher dedupes identical (model, transformed_input)
tuples within a request batch (`ModelTransformsFetcher.scala:163`). Identical
prompts hitting the judge in the same micro-batch cost one LLM call. Above that,
add an application-level cache keyed on `hash(current_message, history_summary)`
if you see repeated requests for the same content.

**Logging.** Set `online=True` on the upstream Join and tune `sample_percent`
to log inputs and verdicts. Use those logs to (a) compute consistency between
the online and offline paths, (b) feed your own evaluation pipeline, (c) spot
classes of inputs the judge is over- or under-flagging.

**Ramping.** When changing the judge model or template, deploy the new version
side-by-side (`guardrail_v4` and `guardrail_v5`) and shadow-evaluate v5 against
v4 on production traffic before flipping. The sample logs are the input data
for that comparison.

## Evaluation

Before promoting a new judge or template to production, evaluate against
established attack sets. None of these alone is sufficient; pick the ones that
match your threat model.

| Benchmark | Paper | What it tests |
| --- | --- | --- |
| HarmBench | Mazeika et al. 2024, [arXiv:2402.04249](https://arxiv.org/abs/2402.04249); ICML 2024 | 510 behaviours, 18 attacks, standardised pipeline |
| AdvBench | Zou et al. 2023, [arXiv:2307.15043](https://arxiv.org/abs/2307.15043) | 520 harmful behaviours; gradient adversarial suffixes |
| AgentDojo | Debenedetti et al. 2024, [arXiv:2406.13352](https://arxiv.org/abs/2406.13352); NeurIPS 2024 D&B | 97 agent tasks, 629 prompt-injection security tests |
| InjecAgent | Zhan et al. 2024, [arXiv:2403.02691](https://arxiv.org/abs/2403.02691); ACL Findings 2024 | 1,054 indirect-injection cases via tool outputs |
| R-Judge | Yuan et al. 2024, [arXiv:2401.10019](https://arxiv.org/abs/2401.10019); EMNLP Findings 2024 | 569 multi-turn agent trajectories; tests trajectory-level safety judgment |
| ToolEmu | Ruan et al. 2024, [arXiv:2309.15817](https://arxiv.org/abs/2309.15817); ICLR 2024 | LM-emulated sandbox; helpfulness-vs-safety tradeoff |

Note R-Judge specifically: GPT-4o tops out at 74% on trajectory-safety judgment
in the published evaluation, and most off-the-shelf LLMs are barely above
random. This is direct evidence that a dedicated guardrail model — not a
general-purpose LLM-as-judge — is necessary if your threat model includes
trajectory-level attacks.

## References

Cited above. Quick index:

- Greshake et al. 2023, [arXiv:2302.12173](https://arxiv.org/abs/2302.12173) — indirect prompt injection
- Inan et al. 2023, [arXiv:2312.06674](https://arxiv.org/abs/2312.06674) — Llama Guard 1
- Zou et al. 2023, [arXiv:2307.15043](https://arxiv.org/abs/2307.15043) — GCG / AdvBench
- Ruan et al. 2024, [arXiv:2309.15817](https://arxiv.org/abs/2309.15817) — ToolEmu
- Yuan et al. 2024, [arXiv:2401.10019](https://arxiv.org/abs/2401.10019) — R-Judge
- Mazeika et al. 2024, [arXiv:2402.04249](https://arxiv.org/abs/2402.04249) — HarmBench
- Zhan et al. 2024, [arXiv:2403.02691](https://arxiv.org/abs/2403.02691) — InjecAgent
- Russinovich et al. 2024, [arXiv:2404.01833](https://arxiv.org/abs/2404.01833) — Crescendo
- Debenedetti et al. 2024, [arXiv:2406.13352](https://arxiv.org/abs/2406.13352) — AgentDojo
- Han et al. 2024, [arXiv:2406.18495](https://arxiv.org/abs/2406.18495) — WildGuard
- Zeng et al. 2024, [arXiv:2407.21772](https://arxiv.org/abs/2407.21772) — ShieldGemma
- Li et al. 2024, [arXiv:2410.22770](https://arxiv.org/abs/2410.22770) — InjecGuard
- Huang et al. 2024, [arXiv:2411.06899](https://arxiv.org/abs/2411.06899) — LongSafetyBench
- Liu et al. 2024, [arXiv:2411.17713](https://arxiv.org/abs/2411.17713) — Llama Guard 3-1B-INT4
- Padhi et al. 2024, [arXiv:2412.07724](https://arxiv.org/abs/2412.07724) — Granite Guardian
- Ghosh et al. 2025, [arXiv:2501.09004](https://arxiv.org/abs/2501.09004) — Aegis 2.0
- Lu et al. 2025, [arXiv:2502.16971](https://arxiv.org/abs/2502.16971) — LongSafety
- Ghosh et al. 2025, [arXiv:2503.05731](https://arxiv.org/abs/2503.05731) — AILuminate v1.0
- Liu et al. 2025, [arXiv:2510.05864](https://arxiv.org/abs/2510.05864) — sensitivity to harmful content in long input
