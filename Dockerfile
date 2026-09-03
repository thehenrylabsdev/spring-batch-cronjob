# Multi-stage build: run non-root, read-only filesystem (SPEC 50).
# Build stage
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./mvnw -B -ntp -DskipTests package

# Runtime stage: minimal JRE, non-root, no shell needed
FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 batchuser
USER 10001
WORKDIR /app
COPY --from=build /workspace/target/spring-batch-cronjob-*.jar app.jar

# INPUT: data volumes are mounted explicitly (k8s manifests) — /app is read-only.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+UseContainerSupport"

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
