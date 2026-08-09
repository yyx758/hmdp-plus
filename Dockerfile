FROM docker.1ms.run/library/maven:3.9.9-eclipse-temurin-8 AS build

WORKDIR /workspace

COPY docker/maven-settings.xml /opt/maven-settings.xml
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -s /opt/maven-settings.xml -B -DskipTests package

FROM docker.1ms.run/library/eclipse-temurin:8-jre-jammy

WORKDIR /app

COPY --from=build /workspace/target/hm-dianping-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
