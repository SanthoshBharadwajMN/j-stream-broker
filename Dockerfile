FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Copy the pom.xml and source code into the container
COPY pom.xml .
COPY src ./src

# Run the maven build to create the .jar file
RUN mvn clean package -DskipTests

# --- STAGE 2: Create the Final Production Image ---
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy ONLY the compiled .jar file from the 'builder' stage
COPY --from=builder /app/target/broker-1.0-SNAPSHOT.jar app.jar

# Create the data directory for AppendOnlyLogs
RUN mkdir data

# Expose ports
EXPOSE 8080
EXPOSE 7000

ENTRYPOINT ["java", "-jar", "app.jar"]
