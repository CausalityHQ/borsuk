# Immutable raw evidence

Original native logs retain their terminating blank lines and the Clippy diagnostic's trailing spaces. Original independent review answers retain Markdown hard breaks and blank-line spaces. These bytes match their recorded hashes and are preserved unchanged.

Default Git whitespace verification applies to all changed Rust source and the remaining delivery files. Only the copied `native.log`, `engineering-review.md`, and `research-review.md` originals are excluded from the delivery whitespace check. The first whole-delivery check's exit 2 and diagnostic log are retained outside this snapshot; it found these original-output formatting differences and no source defect.
