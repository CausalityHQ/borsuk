# Gather v2 decision: REJECT

Exact candidate 45456b5168043d5405b7b792ed9360be5e657cfc; ELF 6ebd599e908ccc94802c8d4c92f647bbbbbb962250f95b48ad4bafe6b0167045. Same original instance i-000581d6e3b92ef88 terminated/waited before collection. All 44 artifacts authenticated; native canary passed; all80 score-sealed cells and independent ratio checks completed. CPU12.002s, wall12.001s; cellCPU139.993–157.997ms meets50ms minimum.

| Dimensions | Rows | Candidate/scalar median | Decision |
| --- | --- | --- | --- |
| 257 | 32 | 1.007081 | PASS |
| 257 | 17 | 1.007214 | PASS |
| 768 | 32 | 1.055504 | FAIL |
| 768 | 17 | 1.070169 | FAIL |
| 1024 | 32 | 1.040039 | FAIL |
| 1024 | 17 | 1.053212 | FAIL |
| 1025 | 32 | 1.034270 | PASS |
| 1025 | 17 | 1.039979 | PASS |

D1024 fails the prospectively fixed <=0.8 CPU ratio in both panels/order subsets. D768 exceeds the <=1.05 regression limit. No algorithm/config/host failure explains this completed verdict. Do not integrate or spend a full cold campaign on this arm. Original v1 method-invalid receipts remain immutable.

Next distinct possibility from the already completed review: vectorize independent rows, preserving each row’s serial accumulation and callback/trace order. This requires a separate bounded design/source correctness gate and the same cheap timing screen before any production or cold claim. Do not retry within-row gather or the previously rejected SHA backend.
