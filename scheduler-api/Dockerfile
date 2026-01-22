FROM maven:3.9.6-eclipse-temurin-21 as builder
WORKDIR /app
# Copy all project definition files
COPY pom.xml .
COPY common ./common
COPY worker-node ./worker-node
COPY scheduler-api ./scheduler-api

# Build everything
RUN mvn clean install -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=builder /app/scheduler-api/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
