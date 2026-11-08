# Phase 3: matched TCP-vs-verbs profiling (chronometer client, sextant server)

## Configuration
- Revision: `be26b4ac` source; manifest follow-up pending.
- Dataset: 4096 keys × 4 MiB, generated on the sextant NVMe, all reads payload-verified.
- Primary: three alternating verbs/TCP repetitions of 70 independent 4 KiB reads per batch round against O_DIRECT NVMe. `batch=70` mode uses one worker per read; the printed `qd=1` field is the steady-QD setting and does not control this batch concurrency.
- Supplement: QD 1/8/32/64, eight-second legs, verbs and TCP for O_DIRECT NVMe and in-memory source.
- Environment: TensorFold/GLM had been scaled down by user. No NCCL traffic; no production mount. The benchmark used no GPU.
- Safety: payload verification passed on every completed leg, zero reported I/O errors/mismatches. Server lifecycle counter ended with accepted=379, unregistered=379, destroyed=379, cleanup_failures=0; mean teardown 106.4 ms.
- Limitation: the current client script prints results but hostPath `/var/tmp/lb-out` is node-local and was not retrieved via the available MCP permissions. A job log is available. The manifest has startup/readiness revisions pending and should not be treated as reproducible final until rebuilt/deployed from a pinned commit.

## Primary batch-70, NVMe O_DIRECT
Values are per 15-second leg. Complete-batch percentiles are primary. Two reps per transport completed before the supplementary sweeps started; the intended third pair did not complete in the observed log.

| Rep | Transport | Batch p50 (ms) | p95 (ms) | p99 (ms) | IOPS | Errors / mismatch |
|---:|---|---:|---:|---:|---:|---|
| 1 | Native verbs | 5.996 | 8.599 | 9.821 | 10,645 | 0 / 0 |
| 1 | TCP | 4.043 | 5.399 | 6.024 | 17,201 | 0 / 0 |
| 2 | Native verbs | 5.259 | 8.718 | 9.670 | 11,914 | 0 / 0 |
| 2 | TCP | 3.582 | 5.272 | 6.027 | 19,003 | 0 / 0 |

Per-read p50/p95/p99 were 3.349/6.453/7.725 ms (verbs rep 1), 1.943/4.074/4.788 ms (TCP rep 1), 2.950/6.316/7.814 ms (verbs rep 2), and 1.686/3.869/4.751 ms (TCP rep 2).

## Supplementary QD sweeps
| Source | QD | Verbs IOPS | TCP IOPS | Verbs errors | TCP errors |
|---|---:|---:|---:|---:|---:|
| NVMe O_DIRECT | 1 | 810 | 1,613 | 0 | 0 |
| NVMe O_DIRECT | 8 | 6,573 | 12,941 | 0 | 0 |
| NVMe O_DIRECT | 32 | 11,849 | 20,354 | 0 | 0 |
| NVMe O_DIRECT | 64 | 15,571 | 20,998 | 0 | 0 |
| RAM | 1 | 899 | 4,080 | 0 | 0 |
| RAM | 8 | 7,609 | 27,121 | 0 | 0 |
| RAM | 32 | 61,704 | 114,283 | 0 | 0 |
| RAM | 64 | 111,287 | 127,221 | 0 | 0 |

Every leg printed `verify: all payloads correct`. TCP outperformed native verbs on the measured QD sweep and primary batch legs; no optimization should be claimed from this run alone. The initial 14 cold/8 warm dial-timeout cause remains unproven; batched MR registration plausibly reduces setup cost but this profile did not isolate setup/dial latency.

## Follow-up required
1. Fix the manifest's history/logging issue and stage outputs where they can be retrieved without `pods/exec` permission; pin source to an immutable SHA.
2. Repeat three or more paired, alternating primary reps with fresh consistent warm state; capture all JSON and verify no non-zero errors/mismatches.
3. Profile client polling/scheduling and send completion, server O_DIRECT service, setup/registration, copies/fragmentation, pool wait/churn, and registered memory separately before tuning.
4. NCCL-concurrent testing remains blocked until user provides live inference traffic.
