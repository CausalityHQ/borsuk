#!/usr/bin/env python3
"""Source-only experiment glue; runtime/data execution is causality EC2 only.

CLI: AUTHORITY AUTHORITY_SHA NEW_DEST. A stdout inventory receipt is not native,
closure, component-provenance, ANN or quality acceptance. Failures retain evidence.
Archive mechanics adapted from verify-closed.py, SHA256
437eb6d80d9d4954337b7edeca6a09be060bd09a950afba08536af92de0ab80a.
"""
import contextlib
import gzip
import hashlib
import json
import os
import pathlib
import re
import stat
import sys
import tarfile

CHUNK = 65536
MAX_ARCHIVE = MAX_RAW = 268435456
MAX_MEMBER, MAX_HEADERS, MAX_MANIFEST = 67108864, 4096, 2097152
NAME_RE = re.compile(r"[A-Za-z0-9._@+=,-]+(?:/[A-Za-z0-9._@+=,-]+)*")
PINS = {
    "historical": (596854,
                   "c02c364a97817ed3d5fc5c86dc2f02f145c5a3f94a3ddca1d713e49e5000862c",
                   "09b7ce99e5293c0e1254287416d536451b77c4386f70a677fb8e9f034563319d",
                   "prepared-parent_query_baseline-result.jsonl"),
    "local": (596125,
              "84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062",
              "5795dcd4de77ce712839be11fa41fd849cbdea53bcd553a55fe5e8c09adb88aa",
              "baseline-result.jsonl"),
}
COMMON = frozenset((
    "evidence-chain/configs/baseline.json", "prepared-parent_cohort_complete.json",
    "prepared-parent_cohort_truth.u64", "prepared-parent_derived_derivation.json",
    "prepared-parent_generation_manifest.json", "prepared-parent_publication-receipt.json",
    "evidence-chain/terminal.json", "chain-outer/outer-closure.json",
    "chain-outer/manager.show", "chain-outer/drain.proof.json", "chain-actual.exit",
))


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def keys(value, names):
    require(type(value) is dict and set(value) == set(names), "unexpected JSON fields")


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON field " + key)
        result[key] = value
    return result


def no_constant(value):
    raise ValueError("nonfinite JSON " + value)


def hex_sha(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def safe_rel(path):
    return (type(path) is str and len(path) <= 255 and NAME_RE.fullmatch(path) is not None
            and all(part not in (".", "..") for part in path.split("/")))


def canonical(path):
    require(type(path) is str and os.path.isabs(path)
            and str(pathlib.Path(path)) == path
            and str(pathlib.Path(path).resolve(strict=True)) == path,
            "noncanonical or symlink path: " + str(path))


def stamp(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def open_regular(stack, path, maximum, size=None):
    canonical(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    stack.callback(os.close, fd)
    before = os.fstat(fd)
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
            and 0 < before.st_size <= maximum, "input regular/size/link: " + path)
    require(size is None or before.st_size == size, "input pinned length: " + path)
    stream = stack.enter_context(os.fdopen(fd, "rb", closefd=False))
    entry = (path, stream, before)
    check_file(entry)
    return entry


def check_file(entry):
    path, stream, before = entry
    canonical(path)
    require(stamp(before) == stamp(os.fstat(stream.fileno()))
            == stamp(os.stat(path, follow_symlinks=False)), "file drift: " + path)


class Hashing:
    """Bounded streaming reads, for both compressed and expanded bytes."""
    def __init__(self, stream, cap):
        self.stream, self.cap, self.n, self.h = stream, cap, 0, hashlib.sha256()

    def read(self, size=CHUNK):
        require(type(size) is int and size >= 0, "unbounded archive read")
        block = self.stream.read(min(size, CHUNK, self.cap - self.n + 1))
        self.n += len(block)
        require(self.n <= self.cap, "archive byte cap")
        self.h.update(block)
        return block


def authenticate(entry, expected_sha, capture=False):
    proxy = Hashing(entry[1], entry[2].st_size)
    kept = bytearray()
    while True:
        block = proxy.read()
        if not block:
            break
        if capture:
            kept.extend(block)
    require(proxy.n == entry[2].st_size and proxy.h.hexdigest() == expected_sha,
            "input SHA/length/EOF: " + entry[0])
    check_file(entry)
    return bytes(kept)


def descriptor(value, maximum):
    keys(value, ("path", "bytes", "sha256"))
    require(type(value["bytes"]) is int and 0 < value["bytes"] <= maximum
            and hex_sha(value["sha256"]), "artifact size/SHA")


def parse_manifest(body):
    require(body.endswith(b"\n"), "manifest newline")
    listed = {}
    for line in body.decode("ascii").split("\n")[:-1]:
        match = re.fullmatch(r"([0-9a-f]{64})  \./(.+)", line)
        require(match is not None, "manifest syntax")
        name = match.group(2)
        require(safe_rel(name) and name not in listed, "manifest path/duplicate")
        listed[name] = match.group(1)
        require(len(listed) <= MAX_HEADERS, "manifest entry cap")
    require(listed and list(listed) == sorted(listed), "manifest generation order")
    for name in listed:
        parts = name.split("/")
        require(all("/".join(parts[:i]) not in listed for i in range(1, len(parts))),
                "manifest file used as ancestor: " + name)
    return listed


class EvidenceHeader(tarfile.TarInfo):
    headers = 0
    ended = False

    @classmethod
    def frombuf(cls, buf, encoding, errors):
        require(len(buf) == 512, "truncated archive header/terminator")
        if buf == bytes(512):
            cls.ended = True
            raise tarfile.EOFHeaderError("archive terminator")
        try:
            header = super().frombuf(buf, encoding, errors)
        except tarfile.HeaderError as error:
            # tarfile otherwise treats some malformed interior headers as EOF.
            raise ValueError("invalid archive header") from error
        cls.headers += 1
        require(cls.headers <= MAX_HEADERS, "archive header count")
        require(header.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE)
                and not header.linkname, "archive extended/link/sparse/type refused")
        require(0 <= header.size <= MAX_MEMBER and len(header.name) <= 255,
                "archive header bounds")
        require(not header.isdir() or header.size == 0, "nonempty directory header")
        return header


class Destination:
    def __init__(self, stack, path):
        require(type(path) is str and os.path.isabs(path)
                and str(pathlib.Path(path)) == path and safe_rel(os.path.basename(path)),
                "destination path")
        parent, name = os.path.split(path)
        canonical(parent)
        self.path, self.stack, self.directories, self.files = path, stack, {}, {}
        self.parent = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, self.parent)
        info = os.fstat(self.parent)
        self.parent_identity = (info.st_dev, info.st_ino, info.st_mode)
        self.make_directory("", self.parent, name)

    def make_directory(self, relative, parent_fd, name):
        os.mkdir(name, 0o700, dir_fd=parent_fd)
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        self.stack.callback(os.close, fd)
        info = os.fstat(fd)
        require(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.geteuid(),
                "destination directory privacy")
        self.directories[relative] = (fd, (info.st_dev, info.st_ino, info.st_mode))

    def create(self, relative):
        parts = relative.split("/")
        parent = ""
        for i, name in enumerate(parts[:-1], 1):
            directory = "/".join(parts[:i])
            if directory not in self.directories:
                self.make_directory(directory, self.directories[parent][0], name)
            parent = directory
        fd = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=self.directories[parent][0])
        self.stack.callback(os.close, fd)
        return self.stack.enter_context(os.fdopen(fd, "wb", closefd=False))

    def finish_file(self, relative, stream, size, digest):
        stream.flush()
        os.fsync(stream.fileno())
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == size,
                "output regular/length/link")
        path = self.path + "/" + relative
        self.files[relative] = (path, stream, info)
        return {"path": path, "bytes": size, "sha256": digest}

    def finish(self):
        for entry in self.files.values():
            check_file(entry)
        for relative, (fd, identity) in reversed(list(self.directories.items())):
            path = self.path + ("/" + relative if relative else "")
            canonical(path)
            before = os.fstat(fd)
            require((before.st_dev, before.st_ino, before.st_mode) == identity,
                    "destination directory identity")
            os.fsync(fd)
            require(stamp(before) == stamp(os.fstat(fd))
                    == stamp(os.stat(path, follow_symlinks=False)), "destination directory drift")
        parent = os.path.dirname(self.path)
        canonical(parent)
        info = os.fstat(self.parent)
        require((info.st_dev, info.st_ino, info.st_mode) == self.parent_identity
                and stamp(info) == stamp(os.stat(parent, follow_symlinks=False)),
                "destination parent drift")
        os.fsync(self.parent)
        require(stamp(info) == stamp(os.fstat(self.parent))
                == stamp(os.stat(parent, follow_symlinks=False)), "destination parent sync drift")
        for entry in self.files.values():
            check_file(entry)


def stream_archive(entry, pin, manifest, selected, destination):
    entry[1].seek(0)
    compressed = Hashing(entry[1], pin["bytes"])
    seen, kinds, extracted = set(), {}, {}
    EvidenceHeader.headers, EvidenceHeader.ended = 0, False
    with gzip.GzipFile(fileobj=compressed, mode="rb") as uncompressed:
        expanded = Hashing(uncompressed, MAX_RAW)
        with tarfile.open(fileobj=expanded, mode="r|", bufsize=512,
                          tarinfo=EvidenceHeader) as archive:
            for member in archive:
                name = member.name
                require(name == "." or name.startswith("./"), "archive path root")
                relative = "" if name == "." else name[2:]
                require(relative == "" or safe_rel(relative), "unsafe archive path")
                require(relative not in kinds, "duplicate archive header: " + relative)
                kinds[relative] = "dir" if member.isdir() else "file"
                if member.isdir():
                    continue
                require(member.isfile() and relative and relative in manifest,
                        "archive type/inventory: " + relative)
                digest, size = hashlib.sha256(), 0
                output = destination.create(relative) if relative in selected else None
                with archive.extractfile(member) as body:
                    while True:
                        block = body.read(CHUNK)
                        if not block:
                            break
                        size += len(block)
                        require(size <= member.size, "member growth")
                        digest.update(block)
                        if output is not None:
                            require(output.write(block) == len(block), "short output write")
                require(size == member.size and digest.hexdigest() == manifest[relative],
                        "member SHA/length/EOF: " + relative)
                seen.add(relative)
                if output is not None:
                    extracted[relative] = destination.finish_file(relative, output, size, digest.hexdigest())
            require(EvidenceHeader.ended, "missing archive terminator")
            tail = 0
            while True:
                block = archive.fileobj.read(CHUNK)
                if not block:
                    break
                tail += len(block)
                require(not any(block), "nonzero archive trailing data")
            require(tail >= 512 and expanded.n % 512 == 0, "archive end blocks")
    require(compressed.n == pin["bytes"] and compressed.h.hexdigest() == pin["sha256"],
            "streamed archive SHA/length/EOF")
    check_file(entry)
    require(seen == set(manifest) and set(extracted) == selected, "closed exact inventory/selection")
    require(kinds.get("", "dir") == "dir", "archive root type")
    for name in kinds:
        parts = name.split("/")
        require(all(kinds.get("/".join(parts[:i]), "dir") == "dir"
                    for i in range(1, len(parts))), "archive file used as ancestor: " + name)
    return extracted


def main():
    require(len(sys.argv) == 4, "usage: AUTHORITY AUTHORITY_SHA NEW_DEST")
    authority_path, authority_sha, target = sys.argv[1:]
    require(hex_sha(authority_sha), "authority SHA")
    with contextlib.ExitStack() as stack:
        authority_file = open_regular(stack, authority_path, 65536)
        authority = json.loads(authenticate(authority_file, authority_sha, capture=True).decode("utf-8"),
                               object_pairs_hook=pairs, parse_constant=no_constant)
        keys(authority, ("schema", "arm", "archive", "manifest"))
        require(authority["schema"] == "borsuk-closed-native-replay-extraction-authority-v1",
                "authority schema")
        arm = authority["arm"]
        require(type(arm) is str and arm in PINS, "authority arm")
        archive_pin, manifest_pin = authority["archive"], authority["manifest"]
        descriptor(archive_pin, MAX_ARCHIVE)
        descriptor(manifest_pin, MAX_MANIFEST)
        size, archive_sha, manifest_sha, result = PINS[arm]
        require((archive_pin["bytes"], archive_pin["sha256"], manifest_pin["sha256"])
                == (size, archive_sha, manifest_sha), "frozen arm artifact pins")
        manifest_file = open_regular(stack, manifest_pin["path"], MAX_MANIFEST, manifest_pin["bytes"])
        manifest = parse_manifest(authenticate(manifest_file, manifest_sha, capture=True))
        selected = COMMON | {result}
        require(selected <= set(manifest), "missing required replay member")
        archive_file = open_regular(stack, archive_pin["path"], MAX_ARCHIVE, size)
        inputs = (authority_file, manifest_file, archive_file)
        require(len({(entry[2].st_dev, entry[2].st_ino) for entry in inputs}) == 3,
                "aliased input files")
        authenticate(archive_file, archive_sha)
        destination = Destination(stack, target)
        extracted = stream_archive(archive_file, archive_pin, manifest, selected, destination)
        destination.finish()
        for entry in inputs:
            check_file(entry)
        receipt = {"schema": "borsuk-closed-native-replay-extraction-v1",
                   "status": "ARCHIVE_INVENTORY_EXTRACTED", "arm": arm,
                   "authority_sha256": authority_sha, "archive": archive_pin,
                   "manifest": manifest_pin, "inventory_files": len(manifest),
                   "destination": target, "extracted": extracted}
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("INVALID archive inventory extraction: " + str(error), file=sys.stderr)
        sys.exit(98)
