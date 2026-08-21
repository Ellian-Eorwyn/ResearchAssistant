#!/usr/bin/env node
// Batched bridge to the UPC reference library.
//
// Every algorithm whose output must be bit-identical across implementations lives
// in Node and is reached through here: canonical JSON (RFC 8785), URL
// normalization, slug generation, and every id recipe. Reimplementing those in
// Python is the one failure mode that silently corrupts a corpus — measured on a
// real repository, a Python port of URL canonicalization would mint 36 of 101
// source ids differently, and every one of those is an `id_mismatch`.
//
// Protocol: NDJSON on stdin, NDJSON on stdout, one result per input line, in
// order. One process handles a whole conversion, so this costs one spawn, not one
// spawn per object.
//
//   {"fn":"mintSrcId","arg":{"canonicalUrl":"https://example.org/a"}}
//   -> {"i":0,"ok":true,"result":"src-e691742f0fda"}

import { createInterface } from "node:readline";

const UPC_HOME = process.env.UPC_HOME;
if (!UPC_HOME) {
  process.stderr.write("UPC_HOME is not set\n");
  process.exit(2);
}

const U = await import(new URL("skill/universal-provenance/scripts/upc_common.mjs", "file://" + UPC_HOME + "/").href);

// Only these are callable. An allow-list keeps the bridge from becoming an
// arbitrary-eval surface and documents exactly what Python is not allowed to
// reimplement.
const FNS = {
  canonicalUrl: (a) => U.canonicalUrl(a.url),
  slugify: (a) => U.slugify(a.text, a.maxLen),
  checkFilename: (a) => U.checkFilename(a.path),
  jcs: (a) => U.jcs(a.value),
  mintSrcId: (a) => U.mintSrcId(a),
  mintRepId: (a) => "rep-" + U.bareHash(a.sha256).slice(0, 12),
  mintImgId: (a) => "img-" + U.bareHash(a.sha256).slice(0, 12),
  mintExtId: (a) => U.mintExtId(a),
  mintGenId: (a) => U.mintGenId(a),
  mintSynId: (a) => U.mintSynId(a),
  mintCbkId: (a) => U.mintCbkId(a),
  mintCodId: (a) => U.mintCodId(a),
  codebookRevisionDigest: (a) => U.codebookRevisionDigest(a.codes),
  computeInputDigest: (a) => U.computeInputDigest(a.inputs),
  specVersion: () => {
    // Read the packaged VERSION rather than trusting a constant.
    return U.__specVersion || null;
  },
};

const out = [];
let i = 0;
const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  const t = line.trim();
  if (!t) continue;
  const idx = i++;
  try {
    const { fn, arg } = JSON.parse(t);
    const f = FNS[fn];
    if (!f) throw new Error(`unknown fn ${JSON.stringify(fn)}`);
    out.push({ i: idx, ok: true, result: f(arg || {}) });
  } catch (e) {
    out.push({ i: idx, ok: false, error: e.message });
  }
}
process.stdout.write(out.map((o) => JSON.stringify(o)).join("\n") + (out.length ? "\n" : ""));
