# syntax=docker/dockerfile:1
#
# rmow-bench's campaign image (docker/run.sh; docker/README.md).
#
# **The image contains the built jar.** Build it on the machine that runs the campaign, from a
# commit: `docker/run.sh build` sends `git archive HEAD` as the context, so the image is that
# commit and nothing else. A run then starts at once and never shares the machine with a
# compiler. Results and the local store are bind-mounted; nothing else is.

# ---------------------------------------------------------------------------------------------
# builder: the jar and its dependencies (target/rmow-bench-*.jar, target/lib/)
# ---------------------------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /src
COPY pom.xml ./
# Dependencies first, so a source change does not download them again.
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.m2 mvn -q -B package -DskipTests

# ---------------------------------------------------------------------------------------------
# bench: a JRE, the jar, the configurations and campaigns, and python3 for the campaign runner
# ---------------------------------------------------------------------------------------------
FROM eclipse-temurin:17-jre AS bench
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*

# The record's provenance: there is no .git in the context, so docker/run.sh passes the commit.
ARG GIT_SHA=unknown
ARG GIT_DIRTY=unknown
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      rmow-bench.dirty="${GIT_DIRTY}"
ENV RMOW_GIT_SHA="${GIT_SHA}" RMOW_GIT_DIRTY="${GIT_DIRTY}"

WORKDIR /app
COPY --from=builder /src/target/rmow-bench-*.jar /app/target/
COPY --from=builder /src/target/lib/ /app/target/lib/
COPY bin/ /app/bin/
COPY configs/ /app/configs/
COPY docker/campaign.py docker/cores.py /app/docker/
COPY docker/campaigns/ /app/docker/campaigns/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "/app/docker/campaign.py"]
