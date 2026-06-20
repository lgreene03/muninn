# Base images are pinned to an explicit major-version tag (25). For stronger
# supply-chain guarantees these can be digest-pinned (FROM ...@sha256:...) once a
# digest is resolved in CI; left as a follow-up to avoid a build break here.
FROM eclipse-temurin:25-jdk-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven && mvn package -DskipTests -q

# Runtime must be glibc-based, not Alpine/musl: DuckDB's JNI native library is
# glibc-linked and segfaults under musl's gcompat shim (init_have_lse_atomics on
# aarch64). eclipse-temurin:25-jre (Ubuntu) ships glibc + libstdc++6, so the
# DuckDB JDBC driver loads and runs cleanly.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
# Run as a non-root user. The eclipse-temurin (Ubuntu) base does not ship a
# dedicated app user, so create an unprivileged one and own the workdir.
RUN groupadd --system --gid 10001 muninn \
    && useradd --system --uid 10001 --gid muninn --no-create-home muninn \
    && chown -R muninn:muninn /app
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
