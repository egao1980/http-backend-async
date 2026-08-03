# Carrier / cl-async disposition (#31)

**Rejected as a dependency.**

[Carrier](https://github.com/orthecreedence/carrier) is an async HTTP client on
[cl-async](https://github.com/orthecreedence/cl-async) (hard-wired **libuv** loop
+ Blackbird). That conflicts with cl-stack’s locked choice:

- app I/O runs on **`event-protocol`** (≥2 backends: libuv **and** libev)
- protocol primitives = callback + cancel; promises only at the `http` facade

**Reuse ideas, not the stack:** `fast-http` parse, request wire shape, promise DX
at the facade. Do **not** pull cl-async drivers or Carrier’s loop.

This repo is the thin rewrite: portable **usocket** TCP +
`event-protocol:register-io` + HTTP/1.1 (not iolib — Unix-only / no Windows).
