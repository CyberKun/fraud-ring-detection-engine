# Design decisions for review

* Use a compile-and-link test for the initial toolchain gate. The C++20 assertions are evaluated by the compiler. The first behavioral tests arrive with the core library.
* Keep offline history as a batch interface and dispatch the live engine at the tenant boundary. Replay uses the same structural transitions as the live writer.
* Assign handles per tenant and LSNs per shard. Normalize endpoint order while retaining edge type. Parallel edges of different types remain distinct. Self-edges count as one incident key and never affect connectivity. Reject the reserved entity prefix 11.
* A qualifying component requires both configured thresholds. A vertex without a previous alert is immediately eligible. Deterministic vertex order breaks ties between eligible representatives. Historical first crossing means the earliest qualifying component containing the queried vertex within retained history.
* Reject already-expired external upserts at the assigned event time. Equal or shorter TTLs on live keys are no-ops. Unknown vertices return absent stats/subgraph and false connectivity. Read-only queries never create vertices.
* Use absolute-time bucket keys so long idle periods skip empty buckets. Followers reconstruct scheduling metadata on promotion and run scheduling only after fencing and reconciliation.
* Require monotonically increasing sequences per tenant and producer, with ordered delivery per partition. Persist accepted sequence and response metadata in the WAL-backed state. Returning producers must keep their sequence namespace across restarts.
* Use fixed replica membership and durable epoch promises. Quorum intersection requires the log selection and prefix rules in PLAN.md. Persist the acknowledged commit boundary for safe retention. Do not use lease ownership alone as proof a log is current.
* Keep the container build on a dated Debian base and package snapshot. System packages supply service dependencies. Phase 0 only consumes the C++ toolchain.
