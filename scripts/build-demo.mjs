import { build } from "esbuild";
import { mkdir, copyFile } from "node:fs/promises";
await mkdir("demo/dist", { recursive: true });
await Promise.all(
  ["index.html", "style.css"].map((file) =>
    copyFile(`demo/${file}`, `demo/dist/${file}`),
  ),
);
await build({
  entryPoints: ["demo/app.ts"],
  bundle: true,
  format: "esm",
  target: "es2022",
  outfile: "demo/dist/app.js",
  sourcemap: true,
});
await build({
  entryPoints: ["tests/browser/react-entry.ts"],
  bundle: true,
  format: "esm",
  target: "es2022",
  outfile: "demo/dist/react-test.js",
});
