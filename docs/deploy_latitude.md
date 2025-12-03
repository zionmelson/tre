# Deploying `tre-arb-bot` on Latitude.sh

Latitude.sh instances run vanilla Ubuntu images, so the bot can be containerized or executed directly with Rust. The instructions below assume Ubuntu 22.04+ and focus on the Docker flow introduced in this repo.

---

## 1. Prep the server

```bash
# connect to the Latitude box (replace with your hostname/IP)
ssh ubuntu@your-latitude-host

# refresh base packages and install dependencies
sudo apt-get update
sudo apt-get install -y git curl build-essential pkg-config protobuf-compiler libssl-dev

# install Docker + Buildx
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker

# optional: install Rust toolchain if you plan to run binaries directly
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

> **Tip:** Latitude servers usually ship without swap. Pinning Stage threads to dedicated cores yields the best latency; select an instance with at least 4 vCPUs and 8+ GB RAM.

---

## 2. Clone the repo & build the image

```bash
cd /opt
sudo git clone https://github.com/zionmelson/tre.git
sudo chown -R $USER tre
cd tre

# (optional) pull the latest changes
git pull

# build the release image
docker build -t tre-arb-bot:latest .
```

This Dockerfile compiles the binary in a Rust builder stage and emits a slim Debian runtime that expects config and ALT data to be mounted.

---

## 3. Create the runtime folders

```bash
sudo mkdir -p /etc/tre-arb-bot /var/lib/tre-arb-bot
sudo chown -R $USER /etc/tre-arb-bot /var/lib/tre-arb-bot
```

- `/etc/tre-arb-bot/config.json` → runtime config (copy the repo's `config.json` and adjust values + `$ENV{VAR}` markers as needed)
- `/var/lib/tre-arb-bot/alt_registry.json` → persisted ALT cache (initially empty)

```bash
cp config.json /etc/tre-arb-bot/config.json
```

Swap any sensitive literals for `$ENV{VAR}` placeholders so the loader can resolve them from environment variables.

---

## 4. Define environment variables

Create `/etc/tre-arb-bot/tre.env` with the sensitive values required at runtime:

```bash
cat <<'EOF' | sudo tee /etc/tre-arb-bot/tre.env
TRE_AUTH_WALLET_PATH=/secrets/jito-auth.json
TRE_PAYER_WALLET_PATH=/secrets/payer.json
TRE_CORECAST_ADDRESS=corecast.bitquery.io
TRE_CORECAST_AUTHORIZATION=ory_at_your_token
TRE_JITO_ENDPOINT_PRIMARY=https://ny.mainnet.block-engine.jito.wtf
TRE_JITO_ENDPOINT_SECONDARY=https://la.mainnet.block-engine.jito.wtf
TRE_JITO_CA_PEM=/secrets/jito_ca.pem
TRE_JITO_TIP_FLOOR_URL=https://bundles.jito.wtf/api/v1/bundles/tip_floor
TRE_JITO_TIP_STREAM_URL=wss://bundles.jito.wtf/api/v1/bundles/tip_stream
TRE_JITO_API_TOKEN=your_token
TRE_BAM_VALIDATOR_CSV=/etc/tre-arb-bot/bam_validators.csv
TRE_QUICKNODE_RPCS=https://node-1,...,https://node-N
PROMETHEUS_METRICS_ADDR=0.0.0.0:9898
RUST_LOG=info,tre-arb-bot=debug
EOF
```

- Store wallet keypairs and CA files under `/secrets` (or any mounted path) and ensure permissions are `600`.
- Any list-style variables (`TRE_JITO_ENDPOINTS`, `TRE_QUICKNODE_RPCS`) can be comma- or newline-separated.
- The legacy Jito auction stream endpoint is no longer available, so the bundled tracker keeps that listener disabled by leaving `jito.auction_stream_url` unset.

---

## 5. Run the container

```bash
docker run -d --name tre-arb-bot \
  --restart unless-stopped \
  --env-file /etc/tre-arb-bot/tre.env \
  -e ARBITRAGE_CONFIG=/config/config.json \
  -e TRE_ALT_REGISTRY=/data/alt_registry.json \
  -v /etc/tre-arb-bot/config.json:/config/config.json:ro \
  -v /var/lib/tre-arb-bot/alt_registry.json:/data/alt_registry.json \
  -v /etc/tre-arb-bot:/etc/tre-arb-bot:ro \
  -v /secrets:/secrets:ro \
  -p 9898:9898 \
  tre-arb-bot:latest
```

- `ARBITRAGE_CONFIG` and `TRE_ALT_REGISTRY` already default to these paths inside the container, but the explicit env vars make the setup self-documenting.
- Prometheus metrics are exposed on `9898/tcp`. Keep the firewall open if you scrape remotely.

Check logs:

```bash
docker logs -f tre-arb-bot
```

---

## 6. (Optional) Systemd wrapper

Create `/etc/systemd/system/tre-arb-bot.service` to ensure Docker starts the bot on boot:

```ini
[Unit]
Description=tre-arb-bot container
After=network-online.target docker.service
Wants=network-online.target

[Service]
Restart=always
ExecStart=/usr/bin/docker start -a tre-arb-bot
ExecStop=/usr/bin/docker stop tre-arb-bot
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable tre-arb-bot.service
sudo systemctl start tre-arb-bot.service
```

---

## 7. CI/CD considerations

- Add the env vars above as Latitude project secrets or GitHub Actions secrets so the `.github/workflows/ci.yml` pipeline can build/test without hardcoding credentials.
- Push Docker images to a registry (`ghcr.io`, `registry.latitude.sh`, etc.) and pull them from the Latitude server if you want to skip on-host builds (replace the `docker build` step with `docker pull ghcr.io/…`).
- Snapshot `/var/lib/tre-arb-bot/alt_registry.json` and `/config/config.json` before redeploying to keep ALT cache warm and configuration consistent.

---

## 8. Troubleshooting checklist

| Symptom                                | Action                                                                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `bundle contains an expired blockhash` | Confirm logs show rate-limited retries; ensure the server clock is in sync (`sudo systemctl restart systemd-timesyncd`). |
| `ResourceExhausted` spikes             | Verify multiple `TRE_JITO_ENDPOINT_*` values and that firewall rules allow outbound to both regions.                     |
| Empty metrics endpoint                 | Confirm `PROMETHEUS_METRICS_ADDR` is set and port 9898 is allowed in Latitude firewall (Project → Security → Firewalls). |
| Container exits immediately            | Run `docker logs tre-arb-bot` and make sure every `$ENV{VAR}` from `config.json` exists in `/etc/tre-arb-bot/tre.env`.   |

Following the above steps keeps secrets out of the repo, ensures reproducible Docker builds, and matches the deployment profile described in `README.md`.
