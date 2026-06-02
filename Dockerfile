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
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
