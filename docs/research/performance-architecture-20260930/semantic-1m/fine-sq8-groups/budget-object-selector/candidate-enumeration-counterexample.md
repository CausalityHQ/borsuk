# Independent mixture shortlist counterexample

This is a synthetic arithmetic falsifier, not a trained selector or ANN quality result.

Use four equally weighted components and33 labels per head. For component r, its eight private labels8r..8r+7 each have probability10/89; common label32 has probability9/89; all other labels have probability0. Both heads use the same probabilities. Each head sums to1.

The per-component top8 Cartesian enumeration emits4*64=256 distinct candidates, all private within-component pairs. It excludes common pair(32,32).

The complete mixture score of(32,32) is81/7921. Every emitted private pair scores25/7921. Mixed private pairs score0, and private/common pairs score22.5/7921. Thus(32,32) is the global winner and is excluded; its score is3.24 times the best candidate score.

Root reproduced this with Python standard-library `fractions.Fraction`, exhaustive33*33 pair evaluation, exact normalized inputs and assertions on the unique global winner, exclusion, candidate count and score ordering. No native experiment, source vectors, query panel or GT was read.

The proposed bounded enumeration is therefore an approximate selector. A correct implementation must report its loss independently against full-mixture selection on small fixtures and against object-coverage oracles. A successful classifier fit cannot establish that the shortlist contains its best objects. Final algorithm choice awaits the existing independent review groupbf121fb269bb4ea6; no alternative policy is frozen here.
