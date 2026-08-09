ARG NODE_IMAGE=node:22.17.0-bookworm-slim@sha256:b04ce4ae4e95b522112c2e5c52f781471a5cbc3b594527bcddedee9bc48c03a0
ARG PYTHON_IMAGE=python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7
ARG POSTGRES_IMAGE=postgres:16.9-bookworm@sha256:253815cf7579ffa05e1673d92e78d37273e61be0e4414e9a1449337d7925be94

FROM ${NODE_IMAGE} AS codex
ARG CODEX_CLI_VERSION=0.144.5
RUN npm install --global --omit=dev "@openai/codex@${CODEX_CLI_VERSION}" \
    && codex --version | grep -F "codex-cli ${CODEX_CLI_VERSION}"

FROM ${POSTGRES_IMAGE} AS postgres-client
RUN install -d /pg-client/opt/postgresql-16/bin /pg-client/opt/postgresql-16/lib \
    && cp /usr/lib/postgresql/16/bin/createdb \
          /usr/lib/postgresql/16/bin/dropdb \
          /usr/lib/postgresql/16/bin/pg_dump \
          /usr/lib/postgresql/16/bin/pg_restore \
          /usr/lib/postgresql/16/bin/psql \
          /pg-client/opt/postgresql-16/bin/ \
    && ldd /usr/lib/postgresql/16/bin/createdb \
           /usr/lib/postgresql/16/bin/dropdb \
           /usr/lib/postgresql/16/bin/pg_dump \
           /usr/lib/postgresql/16/bin/pg_restore \
           /usr/lib/postgresql/16/bin/psql \
       | awk '/=> \// {print $3} $1 ~ /^\// && $1 !~ /:$/ {print $1}' \
       | sort -u \
       | xargs -r -I '{}' cp -L '{}' /pg-client/opt/postgresql-16/lib/

FROM ${PYTHON_IMAGE} AS wheel
ARG UV_VERSION=0.11.7
WORKDIR /build
RUN python -m pip install --disable-pip-version-check --no-cache-dir "uv==${UV_VERSION}"
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt \
      --output-file /requirements.txt \
    && uv export --frozen --only-group build --no-emit-project --format requirements-txt \
      --output-file /build-requirements.txt \
    && uv pip install --system --require-hashes --no-cache -r /build-requirements.txt \
    && uv build --no-build-isolation --wheel --out-dir /wheels

FROM ${PYTHON_IMAGE} AS runtime
ARG APP_UID=10001
ARG APP_GID=10001
ARG CODEX_CLI_VERSION=0.144.5

LABEL org.opencontainers.image.title="GapForge worker" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.description="Non-root evidence research worker with Codex CLI ${CODEX_CLI_VERSION}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CODEX_HOME=/var/lib/gapforge/codex \
    REPORTS_DIR=/var/lib/gapforge/reports \
    BACKUPS_DIR=/var/lib/gapforge/backups \
    PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

RUN groupadd --gid "${APP_GID}" gapforge \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home gapforge \
    && install -d -o gapforge -g gapforge \
       /app /var/lib/gapforge/codex /var/lib/gapforge/reports /var/lib/gapforge/backups

COPY --from=wheel /usr/local/bin/uv /usr/local/bin/uv
COPY --from=wheel /requirements.txt /requirements.txt
COPY --from=wheel /wheels /wheels
RUN uv pip install --system --require-hashes --no-cache -r /requirements.txt \
    && uv pip install --system --no-deps --no-cache /wheels/gapforge-*.whl \
    && rm -rf /requirements.txt /wheels /usr/local/bin/uv

COPY --from=codex /usr/local/bin/node /usr/local/bin/node
COPY --from=codex /usr/local/lib/node_modules /usr/local/lib/node_modules
COPY --from=postgres-client /pg-client/ /
COPY scripts/postgres-tool /usr/local/libexec/gapforge-postgres-tool
RUN ln -s /usr/local/lib/node_modules/@openai/codex/bin/codex.js /usr/local/bin/codex \
    && chmod 755 /usr/local/libexec/gapforge-postgres-tool \
    && ln -s /usr/local/libexec/gapforge-postgres-tool /usr/local/bin/createdb \
    && ln -s /usr/local/libexec/gapforge-postgres-tool /usr/local/bin/dropdb \
    && ln -s /usr/local/libexec/gapforge-postgres-tool /usr/local/bin/pg_dump \
    && ln -s /usr/local/libexec/gapforge-postgres-tool /usr/local/bin/pg_restore \
    && ln -s /usr/local/libexec/gapforge-postgres-tool /usr/local/bin/psql \
    && python --version | grep -F "Python 3.12.11" \
    && codex --version | grep -F "codex-cli ${CODEX_CLI_VERSION}" \
    && pg_dump --version | grep -F "pg_dump (PostgreSQL) 16.9"

WORKDIR /app
COPY --chown=gapforge:gapforge alembic.ini ./
COPY --chown=gapforge:gapforge migrations/ migrations/
COPY --chown=gapforge:gapforge scripts/ scripts/

USER gapforge:gapforge
CMD ["gap", "worker", "--continuous"]
