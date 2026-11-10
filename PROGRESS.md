# Milestone status

Phase 1 passed the core and brute-force oracle gate. Phases 2–9 are unfinished.

## Phase 1 verification

Measured in the Linux amd64 development image built with `docker build -f Dockerfile.dev -t fraud-ring-dev .`, Debian Bookworm, GCC 12.2.0, CMake Release. The suite includes 11 behavioral GoogleTests and the original toolchain check.

```text
> docker run --rm -w /workspace/build fraud-ring-dev ctest --output-on-failure
100% tests passed, 0 tests failed out of 12
Total Test time (real) =   0.21 sec

> docker run --rm fraud-ring-dev ./build/phase1_bench
commands=200000 records=259904 expiries=59904 alerts=256 elapsed_s=0.377 commands_per_s=530208
```

Single measured run, one in-memory writer with the BFS oracle. The deterministic workload cycles through 64 independent five-account groups and includes upserts, extensions, vertex overwrites, manual deletes and ticks. The timer covers command processing, expiry and alert generation, plus record counting. Container startup is outside the timed region. Large connected components, hub churn and distributed throughput require separate measurements.

## Phase 0 verification

Windows, CMake 4.3.3, MSVC 19.50.35728.0, Debug configuration. Actual command output, with build paths omitted:

```text
> cmake -B build
-- Configuring done (0.1s)
-- Generating done (0.1s)
> cmake --build build
  toolchain_smoke.vcxproj -> D:\Fraud Ring Detection Engine\build\Debug\toolchain_smoke.exe
> ctest --test-dir build -C Debug --output-on-failure
1/1 Test #1: toolchain_smoke ..................   Passed    1.55 sec
100% tests passed, 0 tests failed out of 1
Total Test time (real) =   1.56 sec
```

The Phase 0 test checked compilation and linking. Windows Application Control blocked direct execution of the unsigned smoke executable. Phase 1 subsequently built and ran the behavioral suite in Docker.
