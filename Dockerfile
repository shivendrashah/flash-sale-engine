# =====================================================================================================
# docker-compose.yml builds it for the two app instances behind nginx (profile "cluster"):
#
#   docker compose --profile cluster up -d --build     build this image, start PostgreSQL, Redis, Kafka,
#                                                       app1, app2 and nginx on http://localhost:8080
#   docker build -t flash-sale-engine .                 just build the image
#
# Two stages: the first has the full JDK and Maven and builds the jar; the second has only a Java runtime and the
# jar. The final image never contains the source code, Maven or its downloads.
# Versions are pinned (not "21-jre"), so a rebuild next month gives the same Java.
# =====================================================================================================

# ---- Stage 1: build the jar ----
FROM eclipse-temurin:21.0.8_9-jdk AS build
WORKDIR /src

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B dependency:go-offline

COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -DskipTests package \
    && cp target/flash-sale-engine-0.0.1-SNAPSHOT.jar /app.jar

# ---- Stage 2: the image that runs ----
FROM eclipse-temurin:21.0.8_9-jre
WORKDIR /app
RUN useradd --system --uid 10001 flashsale
USER flashsale
COPY --from=build /app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
