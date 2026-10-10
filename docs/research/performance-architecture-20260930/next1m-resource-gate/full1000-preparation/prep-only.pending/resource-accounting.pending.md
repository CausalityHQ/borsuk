# Full1000 preparation accounting — source-derived, runtime UNVERIFIED
Native source: c3e52c8bbf0fcc985c0a8a06d2abeec5d7442d38, prepare_cohere_native_cohort.rs resident_charge and prepare output_cap.
Qualified x86_64 producer ABI retained: Candidate16B, BinaryHeap24B, f64 8B.
Queries1000: vectors4096000; top-k160000; norms8000; heap headers24000.
Resident reservation2897237664B. ID reservation1281280000B unchanged: source prefix1001000 rows.
Output reservation11275409536B including65536B control; output body cap11275344000B.
Source+output+caller scratch+temporary reservation15335384993B < declared17179869184B.
Host envelope unchanged CPU4/8GiB RAM/noSwap/PID128/2400s. These are reservations, not measured RSS or timing.
Request output must equal all4096000 frozen bytes. Truth80000B must authenticate whole body and first2560B must match historical1M Q32 truth SHA36e83267ebfe5efb86db28c778d18897563a8c6c0784933e2b8d6d05cf31bb93.
Static bash parse only; no native runtime, mock, corpus read, or EC2 launch performed. Runtime checks belong on causality EC2.
Remaining launch integration: output-parent binding, prep-only bootstrap/transport/collection, 80000B truth collection cap, exact frozen asset hashes, bounded Spot cleanup.
