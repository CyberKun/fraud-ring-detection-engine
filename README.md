# Streaming fraud-ring detection engine

Phase 0 supplies the implementation plan, build configuration, and a C++20 compile-and-link check. The graph engines, service, console and benchmarks are unfinished. Review [PLAN.md](PLAN.md) and [DECISIONS.md](DECISIONS.md) before approving Phase 1.

```sh
cmake -B build
cmake --build build
ctest --test-dir build -C Debug --output-on-failure
```

Build the Linux development image with `docker build -f Dockerfile.dev -t fraud-ring-dev .`. Its build includes the same toolchain check with Ninja. See [PROGRESS.md](PROGRESS.md) for verification status.
