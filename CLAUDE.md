# CLAUDE.md

Guidance for working on the HORD project on this host.

## Git workflow

Commit changes **directly on `main`** by default. Only create a branch when
explicitly asked for one. (This overrides the usual harness default of branching
off the default branch before committing.)

## Development environment: Soft-RoCE

This host has no RDMA-capable NIC, so HORD is tested against **Soft-RoCE**
(the in-kernel `rdma_rxe` software RDMA transport, RoCEv2). It presents a real
verbs/RDMA-CM device that emulates RoCE over a regular Ethernet NIC.

### Device

| Item            | Value                                            |
| --------------- | ------------------------------------------------ |
| RDMA device     | `rxe0`                                            |
| Backing netdev  | `<NETDEV>`                                         |
| Protocol        | RoCE v2 (RDMA over UDP, dst port **4791**)        |
| RoCEv2 GID      | `::ffff:<HOST_IP>` (GID index 1)                  |
| Kernel module   | `rdma_rxe` (in-tree)                             |

`<NETDEV>` and `<HOST_IP>` are host-specific — substitute your machine's
RoCE-backing Ethernet interface and its RoCEv2 IP (from `ip addr` /
`rdma link show`).

Point HORD's RDMA connection setup at device `rxe0` / GID index 1, or connect
to the host's RoCEv2 IP (`<HOST_IP>`). For single-host tests, run both endpoints
against `rxe0` and connect over `localhost` — the data path loops back internally.

### Persistence (survives reboot)

Two pieces, both installed system-wide:

- `/etc/modules-load.d/rdma_rxe.conf` — loads the `rdma_rxe` module at boot.
- `/etc/systemd/system/soft-roce.service` — oneshot unit that recreates the
  `rxe0` link on `<NETDEV>` after `network-online.target`. Enabled.

The unit is idempotent (it deletes any existing `rxe0` before re-adding), so
`systemctl restart soft-roce` is safe.

### Firewall

`<NETDEV>` carries a public IP and RoCEv2 has no authentication, so inbound
RoCE from the internet is blocked:

- ufw default incoming policy is `DROP` (blocks 4791 already), **plus**
- an explicit `ufw deny in on <NETDEV> ... port 4791 proto udp` rule (v4 + v6).

Local loopback testing is unaffected (looped traffic traverses `lo`, not the
`<NETDEV>` ingress path). If you later want **multi-host** RoCE over `<NETDEV>`,
you must replace the blanket deny with an `allow ... from <peer>` rule.

### Operating it

```sh
# Status / inspect
rdma link show                       # expect: rxe0/1 state ACTIVE ... netdev <NETDEV>
ibv_devices                          # rxe0 should be listed
ibv_devinfo -d rxe0                  # PORT_ACTIVE, link_layer Ethernet, RoCE v2
systemctl status soft-roce.service

# Loopback smoke test (perftest)
ib_send_bw -d rxe0 &                 # server
ib_send_bw -d rxe0 localhost         # client; expect ~1+ GB/s

# Manual control
sudo systemctl restart soft-roce     # recreate rxe0
sudo systemctl stop soft-roce        # tear down rxe0 (keeps it gone until start)

# Full teardown (also disable persistence)
sudo systemctl disable --now soft-roce.service
sudo rm /etc/modules-load.d/rdma_rxe.conf /etc/systemd/system/soft-roce.service
sudo ufw delete deny in on <NETDEV> to any port 4791 proto udp
sudo modprobe -r rdma_rxe
```

### Caveats

- **Tooling**: `ibverbs-utils` (ibv_*) and `perftest` (ib_*_bw) are installed.
- **Pending kernel upgrade**: as of setup the running kernel was `6.8.0-117`
  while `6.8.0-124` was installed. After a reboot you land on 124; the
  module-autoload + systemd unit will re-establish `rxe0` automatically, but if
  `rdma link show` is ever empty, the module just needs reloading
  (`sudo systemctl restart soft-roce`).

## Testing & coverage

Most of the meaningful tests (the actual RDMA data path) are `#[ignore]`d
because they need the `rxe0` device above. They do **not** run under a bare
`cargo test`. Always include them when `rxe0` is up:

The loopback tests and demos connect over the rxe device's RoCEv2 IP, read from
the `HORD_TEST_IP` environment variable and defaulting to `192.0.2.1` (a reserved
RFC 5737 documentation address — no real host IP is baked into the tree). So
either point the tests at this host's device IP:

```sh
HORD_TEST_IP=<HOST_IP> cargo test --workspace -- --include-ignored --test-threads=1
```

or assign the default address to `<NETDEV>` once so the bare command resolves to a
GID (`rdma link show` then lists `::ffff:192.0.2.1`):

```sh
sudo ip addr add 192.0.2.1/32 dev <NETDEV>   # then HORD_TEST_IP is unnecessary
```

```sh
# Full suite incl. the RDMA loopback tests. --test-threads=1 because a few
# in-binary tests reuse the same RDMA-CM port and would otherwise race on bind().
cargo test --workspace -- --include-ignored --test-threads=1

# Just the device-free logic tests (wire format, handshake, parsers):
cargo test --workspace
```

### Line coverage

Coverage uses `cargo-llvm-cov` (`cargo install cargo-llvm-cov`, plus the
`llvm-tools-preview` rustup component):

```sh
cargo llvm-cov --workspace --summary-only -- --include-ignored --test-threads=1
# lcov for editors/Codecov:
cargo llvm-cov --workspace --lcov --output-path lcov.info -- --include-ignored --test-threads=1
```

Library coverage sits around **~90% lines** (`hord-core` ~93%, `hord-stream`
~91–100%, `hord-zerocopy` ~92%, `hord-async` ~83%). The `hord-demo` binaries are
run by hand and show 0% (the demo *lib* — the codec — is ~47%), which pulls the
workspace *total* down to ~65% — read the per-file table, not the total. Add
`--ignore-filename-regex 'hord-demo/'` to exclude the demos from the figure.

### CI

`.github/workflows/ci.yml` runs **device-free** on hosted runners (three jobs):
a `test` job (`cargo test --workspace`, no `--include-ignored`), a `coverage`
job (uploads `lcov.info`, prints the per-file table to the run summary), and the
`codec` job (the Milestone-1 zero-dep build guarantee). The `test`/`coverage`
jobs install only the RDMA *build* libraries (`libibverbs-dev librdmacm-dev` +
`clang`/`libclang-dev` for bindgen) — enough to compile the FFI crates, no
device.

**The RDMA loopback suite does NOT run in CI.** GitHub's hosted-runner kernel
(the `azure` flavour, as of `6.17.0-1015-azure`) dropped the `rdma_rxe` module —
its `linux-modules-extra` ships `siw` but no `rxe`, so Soft-RoCE can't be brought
up, and `siw`/iWARP can't substitute (HORD's data path needs RDMA
write-with-immediate, which iWARP lacks). The 13 `#[ignore]`d data-path tests
therefore run only where an `rxe0` device exists: **this dev host** (the full
`--include-ignored` command at the top of this section), or a **self-hosted
runner**. The bring-up recipe is preserved in `.github/actions/setup-soft-roce`
(retained but no longer referenced by `ci.yml`); re-add it to a job that runs on
an rxe-capable host to test the data path in CI again.

## Shared listener PD

`hord-async::HordListener` binds its underlying core listener with
`Listener::bind_with_shared_pd`, so every accepted QP on the same resolved RDMA
device is built from the same protection domain. That is the register-once path
for long-lived server arenas: call `HordListener::register_external` before
`serve`, keep the returned `Mr` alive for the arena lifetime, and use its
`lkey`/address with `WriteSegment::from_raw` or `WriteSegment::from_mr`.

Low-level `hord_core::Listener::bind` intentionally preserves the historical
per-connection-PD behavior. Use `bind_with_shared_pd` when a listener-shared MR
must be valid across multiple accepted connections. A wildcard bind may not have
a resolved device at bind time; listener-level registration then requires either
a concrete RDMA bind address or a future device-selection layer.
