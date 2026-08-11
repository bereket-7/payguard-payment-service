# Multi-stage so the image is built from source in CI rather than from a target/ directory that
# happens to be sitting on the builder's disk. The previous single-stage form did
# `COPY target/payment-service-*.jar`, which silently baked whatever jar was last built locally —
# including a stale one, or one built from uncommitted code.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# Dependencies resolve in their own layer so a source-only change does not re-download the world.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

# The pod spec sets runAsNonRoot, which refuses to start a container whose image would run as root.
# uid 10001 matches runAsUser in k8s/base/payment-service/deployment.yml.
RUN useradd --system --uid 10001 --create-home appuser

COPY --from=build /workspace/target/payment-service-*.jar /app/app.jar

USER appuser

# Container-aware heap sizing: without this the JVM on older runtimes reads the host's memory rather
# than the cgroup limit and gets OOM-killed by the kubelet instead of throwing OutOfMemoryError.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
