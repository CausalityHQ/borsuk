use strict; use warnings;
my $base='/data/target/borsuk-sq8-panel-attribution.pending';
my $old="$base/launch-split-prerequisites-a0002";
my $new="$base/launch-split-screen-a0001";
sub derive {
 my ($src,$dst,$pairs)=@_; open my $in,'<',$src or die "$src: $!"; local $/; my $s=<$in>; close $in;
 for my $pair (@$pairs) {my ($from,$to,$expected)=@$pair; my $n=($s=~s/\Q$from\E/$to/g); die "count $from=$n wanted $expected" unless $n==$expected;}
 open my $out,'>',$dst or die "$dst: $!"; print $out $s; close $out or die $!;
}
derive("$base/split-prerequisite-preparation.pending/prepare-setup-command.sh","$new/prepare-setup-command.sh",[
 ["out=$old","out=$new",1],
 ['prefix=research/semantic-router/20261016/sq8-split-prerequisites-a0002','prefix=research/semantic-router/20261016/sq8-split-screen-a0001',1],
 ['for name in authority runtime support;','for name in authority runtime support receipts;',1],
 ['for name in C1.runtime.json C1.expected.json NULL_SMOKE.runtime.json NULL_SMOKE.expected.json;','for name in C1.runtime.json C1.expected.json;',1]
]);
derive("$old/submit-setup-once.sh","$new/submit-setup-once.sh",[["out=$old","out=$new",1],['borsuk-sq8-split-prerequisites-setup','borsuk-sq8-split-screen-setup',1]]);
derive("$old/launch-user-data.txt","$new/launch-user-data.txt",[]);
derive("$old/await-readiness.sh","$new/await-readiness.sh",[["out=$old","out=$new",1]]);
derive("$old/watch-terminal-once.sh","$new/watch-terminal-once.sh",[["out=$old","out=$new",1],['borsuk-sq8-split-prerequisites-a0002-external','borsuk-sq8-split-screen-a0001-external',2]]);
