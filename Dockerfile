FROM eclipse-temurin:17-jre
COPY target/payment-service-*.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
