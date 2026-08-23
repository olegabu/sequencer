# tools/dumper/

A genuinely standalone binary (specification.md
[§9](../../docs/specification.md#9-repository-layout-and-build-tooling)):
dumps raw journal records — sequence numbers, lengths, a hex-or-text
preview — for human inspection, without interpreting payload meaning.
Unlike `tools/replay`, it needs no `StateMachine` and no application
code at all; it links only `journal/`.

## Building

```sh
cmake --preset debug
cmake --build --preset debug
```

No dedicated test suite — it's a thin, mechanical formatting layer over
`journal/`'s own (already-tested) reader, and its own correctness is
exercised directly wherever it's used, e.g. `tools/replay/README.md`'s
worked example.

## Seeing it in action

```sh
./build/debug/tools/dumper/dumper --data_dir=/path/to/a/node/data_dir
```

```
journal: /path/to/a/node/data_dir committedCount=5
seq=1 input_len=8 input=0x0500000000000000 output_count=1
  output[0] len=8 0x0500000000000000
seq=2 input_len=8 input=0xfeffffffffffffff output_count=1
  output[0] len=8 0x0300000000000000
...
```

(That trace is the counter example's own format — an 8-byte
little-endian signed delta in, the running total out; `0xfeff...ff` is
`-2`, `0x03...` is `3`.)

Flags: `--from` / `--to` (sequence-number range, inclusive; `--to=0`,
the default, means "through the journal's committed count"), and
`--preview_bytes` (default 64; a field longer than this is truncated
with a trailing `...`; `0` disables previews entirely, showing only
lengths). A field's bytes are shown as quoted text if every previewed
byte is printable ASCII, hex otherwise — dumper never tries to
interpret what the bytes *mean*, only whether they happen to render.
