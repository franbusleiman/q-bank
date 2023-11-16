FROM openjdk:17

WORKDIR /app
VOLUME /app/tmp

COPY ./mvnw .
COPY ./.mvn .mvn
COPY ./pom.xml .

RUN ./mvnw dependency:go-offline

COPY ./src ./src

RUN ./mvnw clean package

CMD ["java", "-jar", "./target/q-bank-0.0.1-SNAPSHOT.jar"]