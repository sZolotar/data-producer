FROM maven:3.6.0-jdk-11-slim as maven
ARG WORK_DIR
COPY ${WORK_DIR}/pom.xml ./pom.xml
RUN mvn dependency:go-offline -B
COPY ${WORK_DIR}/src ./src
RUN mvn package -DskipTests

FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=maven ./target/*.jar ./
EXPOSE 9780
CMD ["java", "-jar", "./data-producer-0.0.1-SNAPSHOT.jar"]
