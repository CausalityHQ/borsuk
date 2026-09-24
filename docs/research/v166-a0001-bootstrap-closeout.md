# V166 a0001 bootstrap failure

The first V166 Spot attempt from source commit
`dbf9399eb66c6ae5b9e6f8ef93a53c4e98bc4075` ran on `c7i.12xlarge`
instance `i-03848d2217d6da50e` in `eu-central-1`. Its complete terminal
at `research/v166-surrogate-ranking/dbf9399eb66c6ae5b9e6f8ef93a53c4e98bc4075/runs/a0001/terminal.json`
has SHA-256 `075a5336699b2678eea305b7b39236f88bd4b6e026008aef4e96b07e6bf00726`,
status `failed`, exit code 1, phase `prepare`. The controller read back
and rehashed both terminal-listed artifacts, then terminated the instance.
No cases, plans, summary, recall, or ranking measurement was produced.

All 11 pinned input downloads passed byte and SHA-256 checks. The closed
`run.log` then shows `load_source_router` rejected the authenticated V115
router at its manifest check. The pinned artifact is
`borsuk-v115-source-router-v1`; current shared code accepts only
`borsuk-source-router-v2`. The failure was a research-artifact loader
mismatch before any pseudoquery work. The correction is an exact V115
reader confined to the V166 research runner, with a synthetic v1 acceptance
and v2 rejection test. Production format policy remains unchanged. Restart
the entire cell from a new pushed source archive and immutable attempt;
none of a0001's partial state is reused.
