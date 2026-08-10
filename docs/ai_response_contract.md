AI Response Template — Aviation Intelligence Platform

Example Question:
Why was Delta more delayed than Southwest in Winter 2024?

1. OBSERVED FROM BTS DATA

Analysis Period: Winter 2024
Flights Analyzed: 4,2xx,xxx

Metric Delta Southwest
Delay Rate 18.3% 12.1%
Efficiency-Pillar Delay Share xx.x% xx.x%
Difference in Delay Rate +6.2 pp —

Largest Contributing Category: Carrier Delay
Secondary Contributor: Late Aircraft Delay

Highest Exposure:
ATL: xx.x% · JFK: xx.x% · MSP: xx.x%

2. PRIMARY DRIVER

The largest measurable contributor to the performance difference was Carrier Delay, followed by Late Aircraft Delay.

The analysis should identify:

Which delay category contributed most to the difference
Which airports/routes/time periods concentrated that difference 3. ROOT-CAUSE EVIDENCE

The observed performance gap is primarily associated with higher carrier-controlled and late-aircraft delay exposure.

Strongest concentration:
[Airport / Route / Departure Bank / Time Window]

Cascade Evidence:
xx.x% of affected flights were preceded by a late inbound aircraft.

Important: BTS data supports these operational patterns but does not independently prove the underlying organizational, mechanical, staffing, or scheduling cause.

4. OPERATIONAL OPPORTUNITY

Highest-Priority Opportunity:
[Specific route / airport / departure bank]

Target:
Reduce controllable cascade exposure rather than treating every delay equally.

Example Findings:

ATL evening departures show the highest concentration of late-aircraft propagation.

ORD → ATL shows unusually high downstream delay exposure during the analyzed period.

5. EXPECTED IMPACT — MODEL ESTIMATE

Only display this section when supported by an analytical/predictive model.

Intervention Tested: +12 min schedule buffer
Estimated Cascade Reduction: 14–18%
Estimated Flights Protected: x,xxx
Estimated Delay Minutes Avoided: xx,xxx

Expected Business Outcome

Fewer propagated delays → improved on-time performance → fewer passenger disruption events → improved operational reliability → potential improvement in customer experience.

Important: Estimated/modelled values must be clearly distinguished from observed BTS statistics.

6. EVIDENCE & TRUST

Every answer should allow the user to inspect the evidence behind it.

Actions:

View SQL · Metric Definition · Data Lineage · Airport Breakdown · Cascade Analysis

Data Lineage:

BTS Source → Bronze → Silver → Gold → Analytics API → AI Interface

Core principle for the AI

The response should always separate:

OBSERVED — What does the data prove?

INTERPRETED — What pattern does the evidence suggest?

ESTIMATED — What does a validated model predict?

ACTIONABLE — What operational area deserves attention?

VERIFIABLE — Can the user inspect the SQL, metrics and lineage?

This is actually a better artifact to save than the visual mockup. The mockup is for presentation; this version is the functional contract you can use later when you're building the API and AI response layer.
