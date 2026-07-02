import { execFileSync } from "node:child_process";
import { join } from "node:path";
import test from "node:test";

test("local TypeScript example runs", () => {
  execFileSync(process.execPath, [join(import.meta.dirname, "..", "examples", "local-index.js")], {
    encoding: "utf8"
  });
});
