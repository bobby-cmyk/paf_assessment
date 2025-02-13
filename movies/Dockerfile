FROM eclipse-temurin:23-noble AS builder

WORKDIR /src

# copy files
COPY mvnw .
COPY pom.xml .

COPY .mvn .mvn
COPY src src

# make mvnw executable
RUN chmod a+x mvnw && /src/mvnw package -Dmaven.test.skip=true

FROM eclipse-temurin:23-jre-noble

WORKDIR /app

COPY --from=builder /src/target/movies-0.0.1-SNAPSHOT.jar app.jar

# check if curl command is available
RUN apt update && apt install -y curl

ENV PORT=8080
ENV MYSQL_URL=
ENV MYSQL_USERNAME=
ENV MYSQL_PASSWORD=
ENV MONGODB_URI=
ENV MONGODB_DB=
ENV DATA_PATH=

EXPOSE ${PORT}

ENTRYPOINT SERVER_PORT=${PORT} java -jar app.jar