# Paper plan: the orchestration tax of scientific workflow managers, and how to reduce it

**Working title:** *Measuring and Reducing the Orchestration Tax of Python Workflow Managers on HEP Analysis Workloads*

**Frameworks:** Snakemake, Luigi, Metaflow. **Workloads:** BSM-Search (CPU-bound), CMS-Analysis (network-bound).

Novelty claims below are hypotheses; they have not yet been checked against the literature (see Section 9).

---

## 1. Thesis in one paragraph

Workflow managers add resource and latency overhead that is rarely quantified separately from the science payload. We (1) define an overhead metric that is independent of the framework, namely the minimum effective task granularity (METG) of each manager, using a real HEP payload and a no-orchestrator baseline; and (2) show that on I/O-bound workflows the default static concurrency leaves the machine mostly idle, then fix this with a small framework-agnostic, telemetry-driven concurrency controller. Part 1 says where orchestration cost matters; Part 2 says what to do about it.

## 2. Motivation from existing data (pilot, n=1 per cell)

Computed from `*/Results/*/*-Resources.txt` (4 cores, 1 Hz, system-wide `psutil`).

| Run | Wall | Core-s | Mean CPU | Time CPU<10% |
|---|---|---|---|---|
| Snakemake BSM | 298 s | 164 | 13.8% | 81% |
| Luigi BSM | 297 s | 230 | 19.4% | 55% |
| Metaflow BSM | 358 s | 455 | 31.7% | 56% |
| Snakemake CMS | 306 min | 10,576 | 14.5% | 46% |
| Luigi CMS | 299 min | 11,413 | 16.0% | 32% |
| Metaflow CMS | 295 min | 13,854 | 19.7% | 21% |

Observations that motivate the paper:
- Identical payload, 2.8x spread in core-seconds on the short workflow (BSM) and 1.3x on the long one (CMS): overhead is large when tasks are short.
- CMS keeps the CPU below 10% for 21-46% of the run with concurrency fixed at 4, while about 27 GB is transferred.
- Luigi/Metaflow service processes add bursty load (Metaflow server CPU CV 335% on BSM).

These are single runs and not statistically supported; Section 6 fixes this.

## 3. Research questions and hypotheses

- **RQ1.** For each manager, what is the minimum task duration at which efficiency stays above 50% / 90% (METG-50, METG-90)?
  - H1: METG ordering differs between managers; Metaflow (per-step process and metadata-service cost) has the largest METG, Snakemake the smallest.
- **RQ2.** How does overhead scale with DAG width and fan-out, independent of task duration?
  - H2: Per-task overhead is roughly constant for Snakemake and Luigi but grows with width for Metaflow's local scheduler.
- **RQ3.** On network-bound workflows, how much makespan and core-second waste is attributable to static concurrency?
  - H3: A controller that raises concurrency while CPU and network headroom exist reduces makespan by at least 25% on CMS-Analysis with no increase in failures.
- **RQ4.** Does one controller design port across all three managers with only a thin adapter?
  - H4: Yes, because all three accept a worker/core limit and expose task start/end events.

## 4. Contributions

1. A METG-style methodology for Python workflow managers, with a synthetic-granularity workload built on the real BSM-Search payload.
2. A no-orchestrator baseline (plain bash + GNU parallel, same containers) giving a true zero-overhead reference.
3. A per-process/cgroup-level overhead attribution (payload / manager / server / container runtime).
4. A framework-agnostic adaptive concurrency controller with adapters for the three managers.
5. A released dataset and scripts (all traces, configs, notebooks) for reproduction.

## 5. Part 1 experimental design: granularity and scale

**Workload.** Parameterise BSM-Search (`njobs`, `nevents` already exist in the configs) and add a `sleep`/busy-loop calibrated payload for the very short end.

**Factors.**
- Task duration d in {0.1, 0.3, 1, 3, 10, 30, 100} s.
- DAG width w in {4, 16, 64, 256, 1024} tasks per layer; depth in {2, 5}.
- Shape: embarrassingly parallel, fork-join (BSM-like), chain.
- Manager in {baseline, Snakemake, Luigi, Metaflow}; workers fixed at 4 for Part 1.

**Metrics.**
- Efficiency E = (sum of task payload time) / (cores x makespan).
- METG-50 and METG-90 per manager and shape.
- Scheduling latency: gap between a task's dependencies finishing and its start.
- Orchestration tax: (core-s_manager - core-s_baseline) / core-s_baseline, split into manager, server, container runtime.
- Peak memory of the manager process tree.

**Instrumentation.** Replace system-wide `psutil` with per-cgroup accounting (systemd scopes or a docker cgroup) plus task start/end timestamps from each manager's logs. Keep the system-wide 1 Hz trace as a cross-check. Add I/O bytes/IOPS in place of disk percent (disk percent is nearly constant and its CV is an artefact).

## 6. Statistical protocol

- At least 10 repetitions per cell (30 for the headline cells); randomised run order; interleaved managers so day/thermal drift is not confounded with the manager.
- Warm vs cold start recorded separately (image pull, bytecode caches, Metaflow service up/down).
- Report medians with bootstrap 95% CIs; compare managers with Mann-Whitney U and report effect sizes (Cliff's delta), not only p-values.
- Fix CPU governor, disable turbo or record frequency, pin the monitor to one core.
- Verify network counters (sent about equals received at 27 GB on CMS is suspicious: check loopback/docker bridge).

## 7. Part 2: adaptive concurrency controller

**Problem.** CMS-Analysis is bound by remote XRootD reads; with concurrency 4 the CPU idles for hours.

**Controller (v1).**
- Inputs, 1 Hz: CPU utilisation, memory headroom, network throughput (and its derivative), task failure/retry rate.
- Policy: AIMD on the concurrency limit. Additive increase while CPU < threshold, memory headroom > threshold and aggregate throughput still rising; multiplicative decrease on memory pressure, throughput collapse, or failure spikes.
- Guardrails: hard max concurrency, memory ceiling, back-off after OOM/retry.
- Variants to ablate: static-N sweep (oracle best N), AIMD, a gradient/hill-climbing controller, and a simple utilisation-target rule.

**Adapters.** Snakemake (restart with updated `--cores` between DAG segments or via a resource-token wrapper), Luigi (worker count / resource limits), Metaflow (`--max-workers`). Where a manager cannot change concurrency at runtime, gate task launch with an external token pool shared by the shell wrappers; this is manager-independent and is itself a design finding.

**Evaluation.**
1. *Trace-driven simulation first.* Replay the existing 1 Hz traces and per-task timings to estimate gains cheaply and tune thresholds.
2. *Real runs* on CMS-Analysis (and a reduced CMS subset for fast iteration): makespan, core-seconds, throughput, failure/retry count, peak memory, energy.
3. *Generalisation:* run the controller on BSM and the synthetic workloads from Part 1 to show it does not hurt CPU-bound jobs.

**Energy (optional but strong).** Record package energy with RAPL or a wall-power meter; report joules per analysis. At 15% utilisation idle power dominates, so shorter makespan usually saves energy.

## 8. Threats to validity

- Single machine (4 cores, 16 GB): state limits and, ideally, repeat on one second machine class.
- Remote data service (CERN Open Data) variability: interleave runs and record per-file transfer rates.
- Manager versions and configurations: pin versions, document defaults, and justify retry/backoff settings (`--restart-times 4`, `@retry(times=4)`).
- Workload representativeness: two HEP workflows plus synthetic ones; say so explicitly.
- Controller tuned on the same workflow it is evaluated on: hold out a workflow or a data subset for evaluation.

## 9. Before committing: novelty check

Search, in this order: Task Bench / METG (Slaughter et al., SC'20) and follow-ups; workflow-manager comparison studies; adaptive concurrency in Pegasus, Parsl, Nextflow, Dask, Ray; HEP workflow-system papers (CHEP/vCHEP proceedings). Narrow the claim to whatever is genuinely uncovered: Python workflow managers + real HEP payload + a cross-manager controller.

## 10. Paper outline

1. Introduction (orchestration tax, two gaps, contributions)
2. Background and related work (METG/Task Bench, WMS surveys, adaptive scheduling)
3. Methodology (workloads, baseline, instrumentation, statistics)
4. Part 1 results: granularity and scale (METG curves, overhead attribution)
5. Part 2: controller design
6. Part 2 results: simulation, real runs, ablations, energy
7. Discussion (recommendations: when to use which manager, minimum task size, concurrency guidance)
8. Threats to validity
9. Conclusion and artifact availability

Candidate venues: eScience, WORKS (SC workshop), CHEP/vCHEP, FGCS, JOSS-style artifact companion.

## 11. Milestones

| Phase | Work | Output |
|---|---|---|
| 0 | Literature check; fix paths; parameterise monitor; cgroup accounting | Narrowed claims, reproducible harness |
| 1 | Baseline + synthetic workload; Part 1 sweeps with repetitions | METG curves, overhead table |
| 2 | Trace-driven simulator; controller v1 | Simulated gains, tuned thresholds |
| 3 | Adapters; real CMS runs; ablations; energy | Part 2 results |
| 4 | Writing, artifact packaging | Draft + dataset |

## 12. Immediate next actions

1. Run the literature check (Section 9).
2. Make `monitor_resources.py` take an output path and add per-cgroup sampling.
3. Build the baseline runner (bash + GNU parallel) for BSM-Search.
4. Write the synthetic-granularity workload generator on top of the BSM configs.
5. Re-run the six existing experiments with at least 10 repetitions to turn the pilot table into real statistics.
