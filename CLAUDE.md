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
| Backing netdev  | `enp14s0`                                         |
| Protocol        | RoCE v2 (RDMA over UDP, dst port **4791**)        |
| RoCEv2 GID      | `::ffff:77.40.251.67` (GID index 1)              |
| Kernel module   | `rdma_rxe` (in-tree)                             |

Point HORD's RDMA connection setup at device `rxe0` / GID index 1, or connect
to IP `77.40.251.67`. For single-host tests, run both endpoints against `rxe0`
and connect over `localhost` — the data path loops back internally.

### Persistence (survives reboot)

Two pieces, both installed system-wide:

- `/etc/modules-load.d/rdma_rxe.conf` — loads the `rdma_rxe` module at boot.
- `/etc/systemd/system/soft-roce.service` — oneshot unit that recreates the
  `rxe0` link on `enp14s0` after `network-online.target`. Enabled.

The unit is idempotent (it deletes any existing `rxe0` before re-adding), so
`systemctl restart soft-roce` is safe.

### Firewall

`enp14s0` carries a public IP and RoCEv2 has no authentication, so inbound
RoCE from the internet is blocked:

- ufw default incoming policy is `DROP` (blocks 4791 already), **plus**
- an explicit `ufw deny in on enp14s0 ... port 4791 proto udp` rule (v4 + v6).

Local loopback testing is unaffected (looped traffic traverses `lo`, not the
`enp14s0` ingress path). If you later want **multi-host** RoCE over `enp14s0`,
you must replace the blanket deny with an `allow ... from <peer>` rule.

### Operating it

```sh
# Status / inspect
rdma link show                       # expect: rxe0/1 state ACTIVE ... netdev enp14s0
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
sudo ufw delete deny in on enp14s0 to any port 4791 proto udp
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

`.github/workflows/ci.yml` runs both of the above on every push/PR: a `test`
job (full suite incl. ignored) and a `coverage` job (uploads `lcov.info`,
prints the per-file table to the run summary). Both bring up Soft-RoCE via the
`.github/actions/setup-soft-roce` composite action, which **fails the job** if
`rxe0` cannot be made ACTIVE — so the RDMA suite can never silently skip in CI.
If a hosted runner's kernel lacks `rdma_rxe`, the fix is the
`linux-modules-extra-$(uname -r)` install the action already attempts.
