# Collector review reconciliation

Group 2dcb2580e0bd421a, research ec19839a1c5d4d7f (Opus limit then configured Sol fallback), engineering 9d6cf072e8f04b95 (Astra), both completed. Full result saved without truncation in collector-review-result.json.

Both required: (1) zero launcher and final receipts at every successful final closure, not only initial absence; (2) reconcile only demonstrated unloading/disappearance races, retaining timeouts, other manager failures, populated/unreadable surviving cgroups and prior failures as INVALID. v2 applies both. Unexpected saved paths are refused without stop or authenticated-absence claim. Exact stop stderr is deliberately strict and target-dependent; unknown spelling refuses, not success.

The smoke collector is literal extraction from v2, with only CLI/evidence working directory, initial zero candidate code and output exit wrapper. It is UNRUN. Original bootstrap manager-show/terminal archive and original-exit preservation remain in v2; this extracted smoke targets cleanup branches only and cannot qualify the whole compiler campaign. Actual failure preservation and manager observation require explicit separate smoke checks. No reclassification of a0003.

Still required on causality EC2: successful unit already unloaded, actual bounded nonzero unit, live descendant proved present at collector entry, wrong/missing paths and missing/nonzero receipts after actual stop, deterministic mocked stop/read disappearance seams labelled as such. No native compiler rerun.
