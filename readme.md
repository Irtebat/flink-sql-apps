# Flink Application Sandbox

This repository serves as a starter-template for developing, packaging, and deploying Apache Flink applications using the Table API and SQL. Specifically, it is designed for experimentation and iteration of deployments using Flink Kubernetes Operator.
## Repository Structure

- `src/main/java/`: Java source files for Flink jobs.
- `pom.xml`: Maven configuration for building and packaging Flink applications.
- `Dockerfile`: Container definition for deploying the application to a Flink cluster.
- `README.md`: This file.

---
## Run your Flink Application Locally

The provided pom.xml is configured to support local development and testing using IntelliJ IDEA.

For local testing, use the included Docker Compose file to start a Kafka cluster: ```docker-compose up -d ```

To list services: ```docker-compose ps```

To stop the services:```docker-compose down```

Kafka services are exposed on the following ports:
- Kafka broker: 9092 
- Kafka JMX: 9101

**Activating the Maven Profile**
The local Maven profile includes dependencies required for development:

- Option 1: Automatically activated within IntelliJ IDEA.
- Option 2: Activate manually using: ```mvn clean package -Plocal ```

**Note:** Update Kafka connection defaults in *src/main/java/irtebat/flink/utils/KafkaConstants.java* as needed for local development.

---
## Packaging a Flink Application

### Step 1: Package your Flink application

Refer to the provided `pom.xml` file as a reference. It is configured to:

- Use the Flink Table API and SQL.
- Include all necessary dependencies using the Maven Shade Plugin.

To build the application fat JAR:

```bash
mvn clean package
```

### Step 2: Build and Push the Docker Image
Use the provided Dockerfile to containerize your application.
Update the base image (cp-flink) if necessary to match your Flink runtime version.
Modify the COPY instruction if the JAR file name has changed.

Example:

```bash
docker build . -t irtebat/cp-flink-app-sandbox:DatagenToKafkaUpsertJob --platform=linux/amd64
```

Then push the image to your registry. For example:
```bash
docker push irtebat/cp-flink-app-sandbox:DatagenToKafkaUpsertJob
```

### Step 3: Deploy Your Application using Flink Operator
Update your FlinkApplication Custom Resource Definition.
Ensure the spec.image field points to the image tag used above; change the job.entryClass to the fully qualified name of your main class.

Example snippet:
```yaml
  image: irtebat/cp-flink-app-sandbox:DatagenToKafkaUpsertJob
```
```yaml
  entryClass: irtebat.flink.apps.archive.DatagenToKafkaTransformationJob
```
Apply the CRD using kubectl:

```bash
kubectl apply -f flink-app.yaml
```

## Notes
This project is not production-grade. It is a safe place to iterate on job logic, connectors, and deployment workflows.
