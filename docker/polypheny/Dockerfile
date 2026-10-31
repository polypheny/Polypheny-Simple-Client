FROM eclipse-temurin:21-jre-jammy

WORKDIR /app
COPY dbms/build/libs/dbms-0.10.1-SNAPSHOT.jar polypheny.jar

EXPOSE 20590 7659
ENTRYPOINT ["java", "-jar", "/app/polypheny.jar"]
CMD ["-noAutoDocker", "-defaultStore", "hsqldb", "-daemon"]
