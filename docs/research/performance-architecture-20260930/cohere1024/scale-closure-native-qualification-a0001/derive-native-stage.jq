def rep($a;$b): split($a)|join($b);
.commands |= map(
 rep("/mnt/borsuk-header-qualification";"/mnt/borsuk-scale-closure-qualification")
 | rep("borsuk-header-population-boundary-red";"borsuk-scale-"+$stage)
 | rep("population-boundary-red";$stage)
 | rep("18f36c212f5590ed628dd5a7299ff467b074adaf";$revision)
 | rep("completed-header-population-qualification-a0001";"scale-closure-native-qualification-a0001")
 | rep("borsuk-native-header-binding-stage-v1";"borsuk-scale-closure-native-stage-v1")
 | rep("sha256sum target/debug/examples/compare_native_replay-f0e199fb7cd664dd > \"$ev/test-elf-sha.txt\"\ngrep -q ^442ac3156fcac371be65e986f4fbad0f0c2d3218f76f153b6a04e4bc0adf5604 \"$ev/test-elf-sha.txt\"\ntest $(stat -c %s target/debug/examples/compare_native_replay-f0e199fb7cd664dd) = 52367272";
 "test_elf=$(jq -Rr '\\''fromjson? | select(.reason==\"compiler-artifact\" and .target.name==\"borsuk\" and .profile.test==true and .executable!=null) | .executable'\\'' \"$root/evidence/"+$compile+"/native.log\")\n[[ \"$test_elf\" == \"$root/source/target/debug/deps/borsuk-\"* && \"$test_elf\" != *$'\\''\\n'\\''* ]]\ntest -f \"$test_elf\" && test ! -L \"$test_elf\" && test -x \"$test_elf\"\nsha256sum \"$test_elf\" > \"$ev/test-elf-sha.txt\"\nstat -c %s \"$test_elf\" > \"$ev/test-elf-bytes.txt\"\nprintf \"%s\\n\" \"$test_elf\" > \"$ev/test-elf-path.txt\"")
 | rep("./target/debug/examples/compare_native_replay-f0e199fb7cd664dd completed_header_binding_tests::bind_completed_scale_authenticates_population_sized_result_tail --exact --test-threads=1";
 "\"$test_elf\" "+$filter+" "+$exact+" --test-threads=1")
 | rep("(( status == 101 ))";"(( status == "+($expected|tostring)+" ))")
 | rep("[[ \"$native\" == 101 && \"$manager\" == 101";"[[ \"$native\" == "+($expected|tostring)+" && \"$manager\" == "+($expected|tostring))
)
