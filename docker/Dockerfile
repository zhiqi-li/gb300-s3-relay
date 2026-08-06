FROM python:3.12-slim AS runtime

ARG TARGETARCH
ARG S5CMD_VERSION=2.3.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && case "$TARGETARCH" in \
         amd64) asset="s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz"; checksum="de0fdbfa3aceae55e069ba81a0fc17b2026567637603734a387b2fca06c299b4" ;; \
         arm64) asset="s5cmd_${S5CMD_VERSION}_Linux-arm64.tar.gz"; checksum="1439f0d00ecedcd2a2f1f2c6749bbb0152b2257bf5086f29646ec8ae38798e24" ;; \
         *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 2 ;; \
       esac \
    && curl --fail --location --retry 5 --output /tmp/s5cmd.tar.gz \
         "https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/${asset}" \
    && printf '%s  %s\n' "$checksum" /tmp/s5cmd.tar.gz | sha256sum --check --status \
    && tar -xzf /tmp/s5cmd.tar.gz -C /usr/local/bin s5cmd \
    && rm -f /tmp/s5cmd.tar.gz \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/gb300-s3-relay
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[openai]" \
    && useradd --create-home --uid 10001 relay \
    && mkdir -p /var/tmp/gb300-relay \
    && chown relay:relay /var/tmp/gb300-relay

USER relay
ENTRYPOINT ["gb300-relay"]
