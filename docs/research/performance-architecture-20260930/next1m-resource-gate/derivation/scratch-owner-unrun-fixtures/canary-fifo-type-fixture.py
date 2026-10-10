#!/usr/bin/env python3
"""Source-bound FIFO path + type fault fixture for the canary coordinator C0 admission (UNRUN by its author).

usage: canary-fifo-type-fixture.py NEW_TAIL NEW_COORDINATOR BASE_TAIL BASE_COORDINATOR [PREV_COORDINATOR]
BASE = exact d2621ff5; PREV (optional) = the path-only repair cce5dabe, used to show the symlink gap the typed exclusion closes.
Every script it runs is rewritten onto a temporary tree and asserted to hold no unrewritten host path: the host's
/var/lib/cloud, /run/cloud-init and /etc/cloud are never read or written.

It cuts the real lines out of the committed coordinator (the `fifo=` pin, the special-file admission loop, lst(), the
case-copy tar command and the admission.json jq text) and drives them with bash on a temporary tree whose /var/lib/cloud,
/run/cloud-init and /etc/cloud prefixes are rewritten to that tree. The only intended behaviour change is the FIFO path.
Needs only bash, GNU find/tar/coreutils and python3 (mkfifo / AF_UNIX sockets are made unprivileged).
"""
import hashlib, os, re, socket, subprocess, sys, tempfile, difflib

new_tail, new_coord, base_tail, base_coord = (open(p).read() for p in sys.argv[1:5])
prev_coord = open(sys.argv[5]).read() if len(sys.argv) > 5 else None
OLD, NEW = "/run/cloud-init/hook-hotplug-cmd", "/run/cloud-init/share/hook-hotplug-cmd"
n = 0


def ok(cond, what):
    global n
    if not cond:
        sys.exit("FAIL: " + what)
    n += 1
    print("PASS:", what)


def line(src, start):
    ls = [l for l in src.split("\n") if l.lstrip().startswith(start)]
    assert len(ls) == 1, (start, len(ls))
    return ls[0]


def func(src, name):
    m = re.search(r"^%s\(\) \{.*?^\}$" % re.escape(name), src, re.S | re.M)
    assert m, name
    return m.group(0)


# ---- (A) source facts ------------------------------------------------------------------------------------------------
ok(line(base_coord, "fifo=") == "fifo=" + OLD, "base pins the root-level FIFO path")
ok(line(new_coord, "fifo=") == "fifo=" + NEW, "new pins exactly /run/cloud-init/share/hook-hotplug-cmd")
ok("hook-hotplug-cmd" not in new_coord.replace("share/hook-hotplug-cmd", ""), "no other spelling of the FIFO name is left")
ok(new_coord.count("share/hook-hotplug-cmd") == 4, "exactly four sites name the FIFO (pin, lst, tar, admission.json)")
d = [l for l in difflib.unified_diff(base_coord.split("\n"), new_coord.split("\n"), lineterm="", n=0) if l[:1] in ("+", "-") and l[:3] not in ("+++", "---")]
ok(len(d) == 8 and all("hook-hotplug-cmd" in l for l in d), "coordinator delta is 4 lines replaced, every one naming the FIFO")
ok(r"ex=(! \( -path ./share/hook-hotplug-cmd -type p \))" in new_coord, "the lst exclusion is type-exact: that path AND -type p")
loop = 'while IFS= read -r -d \'\' p; do [[ $p == "$fifo" && -p $p ]] || { sev 2 "unadmitted special file $p"; exit 0; }; done < "$res/special.lst"'
ok(loop in base_coord and loop in new_coord, "admission loop (exact path AND -p type test) is byte-identical to the base")
ok(all(a == "/" and b in ("", " ", ")", '"') for a, b in re.findall(r"(.)share/hook-hotplug-cmd(.?)", new_coord)), "the new path is a literal word (no glob or other character glued to it)")
ok("admitted_special:[\"%s\"]" % NEW in new_coord, "admission.json records the real FIFO path")
ok(new_tail.replace(re.search(r"^coord_sha=[0-9a-f]{64}$", new_tail, re.M).group(0), "") == base_tail.replace(re.search(r"^coord_sha=[0-9a-f]{64}$", base_tail, re.M).group(0), ""), "tail differs from the base only on its coord_sha line")
ok(re.search(r"^coord_sha=([0-9a-f]{64})$", new_tail, re.M).group(1) == hashlib.sha256(new_coord.encode()).hexdigest(), "tail pins the new coordinator sha256")

# ---- (B) behaviour on a real tree (prefix rewrite only) ----------------------------------------------------------------
def tree():
    t = tempfile.mkdtemp(prefix="canary-fifo-")
    for p in ("var/lib/cloud/data", "run/cloud-init/share", "etc/cloud/cloud.cfg.d"):
        os.makedirs(os.path.join(t, p))
    for p in ("run/cloud-init/share/keep.txt", "run/cloud-init/status.json", "var/lib/cloud/data/instance-id"):
        open(os.path.join(t, p), "w").write("x\n")
    return t


def rw(text, t):
    for a in ("/var/lib/cloud", "/run/cloud-init", "/etc/cloud"):
        text = text.replace(a, t + a)
    left = text.replace(t + "/var/lib/cloud", "").replace(t + "/run/cloud-init", "").replace(t + "/etc/cloud", "")
    assert not any(a in left for a in ("/var/lib/cloud", "/run/cloud-init", "/etc/cloud")), "unrewritten host path"
    return text


def special(t, kind, rel):
    p = os.path.join(t, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    if kind == "fifo":
        os.mkfifo(p)
    else:
        s = socket.socket(socket.AF_UNIX)
        s.bind(p)  # leaves a socket inode; closed handle does not remove it
        s.close()


def bash(script):
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)


def admit(coord, t):
    fifo = rw(line(coord, "fifo="), t)
    find = rw(re.search(r"run 5 (find /var/lib/cloud /run/cloud-init /etc/cloud ! -type f ! -type d ! -type l -print0) > ", coord).group(1), t)
    s = 'sev(){ echo "SEV $*"; }\nres=%s\n%s\n%s > "$res/special.lst"\n%s\necho LOOP_DONE\n' % (t, fifo, find, loop)
    r = bash(s)
    return "LOOP_DONE" in r.stdout, r.stdout


def admitted(coord, kind, rel):
    t = tree(); special(t, kind, rel); return admit(coord, t)[0]


ok(admitted(new_coord, "fifo", "run/cloud-init/share/hook-hotplug-cmd"), "NEW admits the actual FIFO at /run/cloud-init/share/hook-hotplug-cmd")
ok(not admitted(base_coord, "fifo", "run/cloud-init/share/hook-hotplug-cmd"), "BASE refuses it (the a0010 failure: unadmitted special file)")
ok(not admitted(new_coord, "fifo", "run/cloud-init/hook-hotplug-cmd"), "NEW refuses the old root-level path")
ok(not admitted(new_coord, "socket", "run/cloud-init/share/hook-hotplug-cmd"), "NEW refuses a socket at the admitted path (type requirement kept)")
for rel in ("run/cloud-init/share/hook-hotplug-cmd2", "run/cloud-init/share/x/hook-hotplug-cmd", "run/cloud-init/share/other",
            "var/lib/cloud/share/hook-hotplug-cmd", "etc/cloud/share/hook-hotplug-cmd", "run/cloud-init/hook-hotplug-cmd2"):
    ok(not admitted(new_coord, "fifo", rel), "NEW refuses a foreign FIFO at " + rel)
ok(not admitted(new_coord, "socket", "run/cloud-init/share/other"), "NEW refuses a foreign socket")
t = tree(); special(t, "fifo", "run/cloud-init/share/hook-hotplug-cmd"); special(t, "fifo", "run/cloud-init/other-fifo")
ok(not admit(new_coord, t)[0], "NEW refuses the real FIFO when any foreign special file is also present")

# listing: the admitted FIFO is excluded by find itself (only when $2 is set); everything else stays visible
def listing(coord, t, ex):
    r = bash("set -e\n%s\nlst %s %s" % (func(coord, "lst"), t + "/run/cloud-init", "x" if ex else ""))
    return r.returncode, r.stdout


t = tree(); special(t, "fifo", "run/cloud-init/share/hook-hotplug-cmd")
rc, out = listing(new_coord, t, True)
ok(rc == 0 and "hook-hotplug-cmd" not in out and "d ./share" in out and "keep.txt" in out, "NEW listing excludes the FIFO and keeps its directory and files")
rc, out = listing(base_coord, t, True)
ok(rc == 0 and "p ./share/hook-hotplug-cmd" in out, "BASE listing would have kept the FIFO (digest mismatch precursor)")
rc, out = listing(new_coord, t, False)
ok(rc == 0 and "p ./share/hook-hotplug-cmd" in out, "NEW listing without the exclusion flag still shows the FIFO")
special(t, "socket", "run/cloud-init/share/other")
rc, out = listing(new_coord, t, True)
ok(rc == 0 and "s ./share/other" in out, "NEW listing keeps a foreign special file visible")

# case copy: the exclude must drop the real FIFO and nothing else
def tarcopy(coord, t):
    m = re.search(r"run 8 bash -c '([^']*--exclude=[^']*)' _", coord)
    assert m
    dst = tempfile.mkdtemp(prefix="canary-fifo-copy-")  # fresh per call: a tree is copied by more than one coordinator
    r = bash("bash -c '%s' _ %s %s" % (m.group(1), t + "/run/cloud-init", dst))
    return r.returncode, dst


t = tree(); special(t, "fifo", "run/cloud-init/share/hook-hotplug-cmd")
rc, dst = tarcopy(new_coord, t)
ok(rc == 0 and not os.path.lexists(dst + "/share/hook-hotplug-cmd") and os.path.isfile(dst + "/share/keep.txt") and os.path.isfile(dst + "/status.json"), "NEW copy drops only the FIFO")
rc, dst = tarcopy(base_coord, t)
ok(rc == 0 and os.path.lexists(dst + "/share/hook-hotplug-cmd"), "BASE copy would have carried the FIFO into the case tree")


# ---- (C) type matrix at the admitted path: only a FIFO survives admission AND the live/copy digest -------------------------
import stat


def mk(t, kind):
    p = t + "/run/cloud-init/share/hook-hotplug-cmd"
    if kind == "fifo":
        os.mkfifo(p)
    elif kind == "regular":
        open(p, "w").write("x\n")
    elif kind == "socket":
        special(t, "socket", "run/cloud-init/share/hook-hotplug-cmd")
    elif kind == "symlink":
        os.symlink("keep.txt", p)
    elif kind == "dangling-symlink":
        os.symlink("no-such-target", p)
    elif kind == "directory":
        os.mkdir(p)
    elif kind == "chardev":
        os.mknod(p, 0o600 | stat.S_IFCHR, os.makedev(1, 3))  # needs root; skipped otherwise
    else:
        raise AssertionError(kind)


def dg(coord, d, ex):
    r = bash("set -o pipefail\n%s\nlst %s %s | sha256sum" % (func(coord, "lst"), d, "x" if ex else ""))
    w = r.stdout.split()
    return w[0] if r.returncode == 0 and w else None


def stages(coord, kind):
    """(admission loop passes, live digest == copied digest)"""
    t = tree()
    mk(t, kind)
    loop_ok = admit(coord, t)[0]
    live = dg(coord, t + "/run/cloud-init", True)
    rc, dst = tarcopy(coord, t)
    return loop_ok, bool(live) and rc == 0 and live == dg(coord, dst, False)


ok(stages(new_coord, "fifo") == (True, True), "NEW: a FIFO at the admitted path passes both the admission loop and the digest")
for kind in ("regular", "symlink", "dangling-symlink", "directory"):
    ok(stages(new_coord, kind) == (True, False), "NEW: a %s at the admitted path is not special, so the digest (listing vs tar copy) must refuse it" % kind)
for kind in ("socket", "chardev"):
    try:
        res = stages(new_coord, kind)
    except PermissionError:
        print("SKIP: %s needs root to create" % kind)
        continue
    ok(res[0] is False, "NEW: a %s at the admitted path is refused by the -p admission loop" % kind)
if prev_coord:
    ok(stages(prev_coord, "symlink") == (True, True), "PREV (path-only exclusion) silently dropped a symlink at the FIFO path - the gap the typed exclusion closes")
    ok(stages(prev_coord, "fifo") == (True, True) and stages(prev_coord, "regular") == (True, False), "PREV still admits only the FIFO among FIFO/regular")

print("ALL %d PASS" % n)
