FROM ubuntu:trusty

MAINTAINER Mattias Petter Johansson <mpj@mpj.me>

RUN apt-get update && apt-get install -y openjdk-7-jdk

COPY . /app

WORKDIR /app
RUN ./gradlew install
EXPOSE 4444
CMD ./gradlew run

