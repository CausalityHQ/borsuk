"""Small executable range/scorer adapter check; synthetic, never a corpus query."""
import numpy as np
from scripts.run_native_two_bit_topology import physical_rows,scored,plan_fields,PAGE_BYTES,OBJECT_BYTES
from scripts.test_native_two_bit_topology import rejects

plan=dict(query_ordinal=0,ranges=[[0,PAGE_BYTES]],planned_bytes=PAGE_BYTES)
assert np.array_equal(physical_rows(plan),np.arange(256))
assert np.array_equal(physical_rows(dict(plan,ranges=[[390*PAGE_BYTES,OBJECT_BYTES]],planned_bytes=OBJECT_BYTES-390*PAGE_BYTES)),np.arange(99840,100000))
assert plan_fields(dict(plan,arm='candidate',root_sha256='ignored'))==plan
rejects(lambda: physical_rows(dict(plan,planned_bytes=PAGE_BYTES+1)))
rejects(lambda: physical_rows(dict(plan,ranges=[[0,PAGE_BYTES],[PAGE_BYTES,2*PAGE_BYTES]],planned_bytes=2*PAGE_BYTES)))
rejects(lambda: physical_rows(dict(plan,ranges=[[0.0,PAGE_BYTES]])))
dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
sq8=np.zeros(256,dtype=dtype);sq8['id']=np.arange(255,-1,-1);sq8['norm']=1
sample,returned=scored(plan,sq8,np.zeros(256,dtype=np.float32),list(range(100)),list(range(100)))
assert returned==list(range(100))
assert sample==dict(query_ordinal=0,fetched_hits=100,returned_hits=100,flat_hits=100,gets=1,bytes=PAGE_BYTES)
print('topology range/scorer adapter checks passed')
