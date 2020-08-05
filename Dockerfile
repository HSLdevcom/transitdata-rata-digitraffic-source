FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-rata-digitraffic-source.jar /usr/app/transitdata-rata-digitraffic-source.jar
ENTRYPOINT ["java", "-Xms256m", "-Xmx4096m", "-jar", "/usr/app/transitdata-rata-digitraffic-source.jar"]
