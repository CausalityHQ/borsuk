
def union_ns: sort_by(.[0]) | reduce .[] as $i ({end:0,total:0}; if $i[1] <= .end then . else .total += ($i[1]-([$i[0],.end]|max)) | .end=$i[1] end) | .total;
def projection: del(.diagnostic,.stages,.query_wall_ns,.query_process_cpu_ns,.transport);
[.[]|select(.phase=="query")] as $q |
{schema:"borsuk-source-stall-closed-analysis-v1",executions:($q|length),
parity:($q|sort_by(.ordinal)|group_by(.ordinal)|map({ordinal:.[0].ordinal,repetitions:length,logical_semantic_equal:(map(projection)|unique|length==1)})),
observer:($q|sort_by(.ordinal)|group_by(.ordinal)|map(
[.[]|select(.diagnostic.source_range_trace!=null)] as $on |
[.[]|select(.diagnostic.source_range_trace==null)] as $off |
{ordinal:.[0].ordinal,on_count:($on|length),off_count:($off|length),
wall_on_ns:($on|map(.query_wall_ns)|add/length),wall_off_ns:($off|map(.query_wall_ns)|add/length),
cpu_on_ns:($on|map(.query_process_cpu_ns)|add/length),cpu_off_ns:($off|map(.query_process_cpu_ns)|add/length)})),
traced:[$q[]|select(.diagnostic.source_range_trace!=null)|
.diagnostic.source_range_trace as $s |
[$s.ranges[]|[.[12],.[13]]] as $auth |
{ordinal,query_wall_ns,query_process_cpu_ns,
source_wall_ns:(.stages.source.end_ns-.stages.source.start_ns),
acquire_wall_ns:($s.all_ranges_complete_ns-$s.fetch_start_ns),
auth_union_ns:($auth|union_ns),
auth_cpu_ns:$s.cpu.authentication.thread_cpu_ns,
copy_cpu_ns:$s.cpu.collector_copy.thread_cpu_ns,
unattributed_source_cpu_ns:$s.unattributed_process_cpu_ns,
longest_auth_ns:([$s.ranges[]|.[13]-.[12]]|max),
longest_gap_ns:([$s.ranges[]|.[17]]|max),
gap_auth_overlap_union_ns:([$s.ranges[] as $r|$auth[] as $a|
[([$r[18]-$r[17],$a[0]]|max),([$r[18],$a[1]]|min)]|select(.[1]>.[0])]|union_ns),
ranges:$s.planned_ranges,source_bytes:$s.planned_bytes}],
transport:{attempts:([$q[]|.transport.after.attempts-.transport.before.attempts]|add),
consumed_bytes:([$q[]|.transport.after.consumed_payload_bytes-.transport.before.consumed_payload_bytes]|add),
failures:([$q[]|(.transport.after.transport_failures-.transport.before.transport_failures)+(.transport.after.stream_failures-.transport.before.stream_failures)]|add)}
} | .observer |= map(. + {wall_ratio:(.wall_on_ns/.wall_off_ns),cpu_ratio:(.cpu_on_ns/.cpu_off_ns)}) |
. + {all_logical_parity:(.parity|all(.logical_semantic_equal and .repetitions==4)),
weighted_observer_wall_ratio:((.observer|map(.wall_on_ns)|add)/(.observer|map(.wall_off_ns)|add)),
weighted_observer_cpu_ratio:((.observer|map(.cpu_on_ns)|add)/(.observer|map(.cpu_off_ns)|add))}
