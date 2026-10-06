# Fitting work admission before native mechanism execution

Status: source-derived arithmetic, not measured runtime or a scientific result. The active selector qualification bundle remains unchanged.

The prospective method has 6,250 physical groups at 100,000 rows, up to eight proposed destinations, 256 training anchors, and two alternating rounds. Calling `evaluate_checkpoint` independently for each proposal evaluates both before and after selectors for every anchor. In the frozen source, inference alone charges `2*(64*D+(4+8*s)*64)+4*(4+8*s)+4*D` modeled operations. This excludes candidate generation, sorting, hashing, coverage, teacher construction and gradients.

At D768, even the deliberately optimistic s1 model gives 102,960 units per inference. The complete eight-proposal traversal therefore requires at least 6,250*8*256*2*2*102,960 = 5,271,552,000,000 modeled units, over ten times the declared 512,000,000,000 cap. At s11 the inference-only bound is 5,812,224,000,000. These are bounds for the full declared proposal traversal; early refusals or fewer proposals can use less work. No wall-time estimate follows.

Before fitting, implement reuse of immutable per-anchor normalized inputs, head probabilities, candidate scores and selection order for each frozen model checkpoint. Membership proposals still recompute occupancy, capacity, maximum-body budget selection and actual covered neighbor weight exactly. Do not reuse a selection when occupancy changes. Bind cached inference to model/query identities and charge its construction, retained capacity and every proposal evaluation. A model update invalidates its inference cache. Compare cached move/checkpoint outcomes against the existing uncached evaluator on independent tiny fixtures before any real fitting.

This is an execution optimization requirement, not permission to change the frozen teacher, anchor split, optimizer, proposal order, acceptance rule or resource cap. If exact reuse cannot fit the cap, report a mechanism refusal and amend the prospective method explicitly before another experiment. The current Rust primitive has no fitter and remains pending native qualification.
