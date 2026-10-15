def require($ok;$message): if $ok then . else error($message) end;
def valid($expected):
 .schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED"
 and .complete==true and .population_percentiles_valid==true and .diagnostic_only==false
 and .identity==$expected.expected_identity and .inputs==$expected.expected_bound_inputs
 and .native_terminal.complete==true and .native_terminal.selected_count==1000
 and .native_terminal.population_count==1000 and .native_terminal.recall_denominator==10000
 and .statistics.count==1000
 and all([.statistics.p90_ns,.statistics.p95_ns,.statistics.query_wall_ns][]; type=="number" and floor==. and .>0 and .<=1000000000000)
 and .statistics.p90_ns<=.statistics.query_wall_ns
 and .statistics.p95_ns<=.statistics.query_wall_ns
 and .statistics.p90_ns>0 and .statistics.p95_ns>0 and .statistics.sequential_qps>0
 and .native_terminal.transport_last_boundary.after.transport_failures==0
 and .native_terminal.transport_last_boundary.after.stream_failures==0
 and .native_terminal.transport_last_boundary.after.dropped_error_bodies==0
 and all(.native_terminal.charges[]; .failed_gets==0);
def workload: del(.generation_prefix,.generation_root_sha256,.sq8_get_budget,.sq8_byte_budget);
def cell($label):
 {label:$label,result_sha256:.result_sha256,result_bytes:.result_bytes,
  runtime_config_sha256:.identity.config_sha256,binary_sha256:.identity.binary_sha256,
  generation_root_sha256:.native_terminal.generation_root_sha256,
  hits:.native_terminal.recall_numerator,denominator:.native_terminal.recall_denominator,
  underfilled_queries:.native_terminal.underfilled_queries,
  statistics:.statistics,observed_process_peak_bytes:.native_terminal.observed_process_peak_bytes,
  logical_charges:.native_terminal.charges,
  cumulative_sdk_transport:.native_terminal.transport_last_boundary.after,
  startup:.startup,stage_statistics:.stage_statistics};
def pair($a;$b):
 {control:$a.label,candidate:$b.label,
  p90_gain_fraction:(1-$b.statistics.p90_ns/$a.statistics.p90_ns),
  p90_regression_within_five_percent:($b.statistics.p90_ns*100 <= $a.statistics.p90_ns*105),
  qps_regression_within_five_percent:($b.statistics.query_wall_ns*95 <= $a.statistics.query_wall_ns*100),
  p95_gain_fraction:(1-$b.statistics.p95_ns/$a.statistics.p95_ns),
  sequential_qps_change_fraction:($b.statistics.sequential_qps/$a.statistics.sequential_qps-1),
  source_get_delta:($b.logical_charges.source.submitted_gets-$a.logical_charges.source.submitted_gets),
  source_byte_delta:($b.logical_charges.source.verified_bytes-$a.logical_charges.source.verified_bytes),
  sq8_get_delta:($b.logical_charges.sq8.submitted_gets-$a.logical_charges.sq8.submitted_gets),
  sq8_byte_delta:($b.logical_charges.sq8.verified_bytes-$a.logical_charges.sq8.verified_bytes)};
require(([$a1,$b1,$b2,$a2,$ea,$eb]|all(.[];length==1));"single report/template document required")
| require(($a1[0]|valid($ea[0])) and ($a2[0]|valid($ea[0])) and
          ($b1[0]|valid($eb[0])) and ($b2[0]|valid($eb[0]));"native report/header/coverage/transport validation")
| require(($ea[0].expected_bound_inputs|workload)==($eb[0].expected_bound_inputs|workload);"cross-arm workload mismatch")
| [($a1[0]|cell("A1")),($b1[0]|cell("B1")),($b2[0]|cell("B2")),($a2[0]|cell("A2"))] as $c
| require(([$c[].result_sha256]|unique|length)==4;"four distinct sealed result bodies required")
| [pair($c[0];$c[1]),pair($c[3];$c[2])] as $pairs
| ([($c[3].statistics.p95_ns/$c[0].statistics.p95_ns-1|fabs),
    ($c[2].statistics.p95_ns/$c[1].statistics.p95_ns-1|fabs)]|max) as $repeat
| {schema:"borsuk-v9-two-elf-abba-summary-v1",status:"MEASURED",complete:true,
   ordered_labels:["A1","B1","B2","A2"],cells:$c,pairs:$pairs,
   control_anchor_reproduced:($c[0].hits==9780 and $c[3].hits==9780),
   candidate_matched_recall:($c[1].hits>=9780 and $c[2].hits>=9780),
   candidate_full_k:($c[1].underfilled_queries==0 and $c[2].underfilled_queries==0),
   candidate_matched_quality:($c[1].hits>=9780 and $c[2].hits>=9780 and $c[1].underfilled_queries==0 and $c[2].underfilled_queries==0),
   candidate_prospective_floor:($c[1].hits>=9760 and $c[2].hits>=9760),
   descriptive_p95_repeat_reference:$repeat,
   modest_performance_screen:($pairs|all(.[];.p95_gain_fraction>0 and .p90_regression_within_five_percent and .qps_regression_within_five_percent)),
   p95_repeat_numeric_boundary:($pairs|any(.[];((.p95_gain_fraction-$repeat)|fabs)<1e-12)),
   p95_gains_exceed_descriptive_repeat:(if ($pairs|any(.[];((.p95_gain_fraction-$repeat)|fabs)<1e-12)) then null else ($pairs|all(.[];.p95_gain_fraction>$repeat)) end),
   numerical_boundary_requires_root_precision_check:true,
   quality_and_efficiency_are_separate:true,preserve_valid_pareto_results:true,
   chronology_qualification:"external root chain events, supervision and source receipt required",
   physical_wire_or_billed_quantities_qualified:false,server_cache_eviction_qualified:false,
   first_query_latency_available:false,vendor_win_claim:false,
   comparison_scope:"Two complete architecture configurations; not shortlist-size-only causality"}
