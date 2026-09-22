# snarkos-node-metrics

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-metrics.svg?color=neon)](https://crates.io/crates/snarkos-node-metrics)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-metrics` crate provides access to metrics for the `snarkos` node.

## Instructions

#### Quick Start (via `scripts/devnet.sh`)

`scripts/devnet.sh` brings this stack up automatically: it regenerates `prometheus.yml`
to match the validator count you choose, runs `docker compose up --detach` (falling
back to `docker-compose` if that's what's installed), and opens
`http://localhost:3000/d/snarkos` once Grafana is ready. The Prometheus datasource and
the snarkOS dashboard are pre-provisioned (`node/metrics/provisioning/`), so no manual
datasource or import steps are needed. Grafana still requires authentication for any
URL, so the first thing you'll see is its login page — log in with `admin`/`admin` (you
can skip the "change password" prompt) and it redirects you straight to the dashboard.
This is a one-time-per-browser-session login, not a bug in the automation.

If Docker isn't installed, `devnet.sh` skips this step with a warning and the devnet
still starts normally; install Docker and re-run.

#### Running the stack standalone

To start up Grafana and Prometheus without `devnet.sh` (e.g. against nodes you started
yourself with `--metrics`):
```bash
cd node/metrics
docker compose up --detach
```

To check that the metrics are running, go to http://localhost:9000.

Then go to [http://localhost:3000/](http://localhost:3000/) — the initial login is
`admin`/`admin`. The Prometheus datasource and snarkOS dashboard are already
provisioned (same as above), so the dashboard should be populated right away. The
scrape targets in the committed `prometheus.yml` default to a single node on `:9000`;
edit it if you're running more than one node with metrics enabled.

#### Manual import (fallback)

If you'd rather configure Grafana by hand, or need to import the dashboard into a
different Grafana instance, `node/metrics/snarkOS-grafana.json` is still the
importable, manual-import version (it prompts you to pick a datasource on import,
rather than assuming the provisioned one):

1. In Grafana, go to `Connections` → `Data sources` → `Add data source` → `Prometheus`,
   set the URL to `http://prometheus:9090`, and `Save & test`.
2. Go to `Dashboards` → `New` → `Import`, drag in `node/metrics/snarkOS-grafana.json`,
   pick the datasource you just added, and `Import`.
