# HDFS Source for Spring Cloud Data Flow

A Spring Cloud Data Flow compatible source application that reads files from a directory on HDFS and emits their contents as messages. Built with Spring Boot 3.4.5 and Java 21.

## Features
- Watches a configurable directory on HDFS
- Emits file contents as messages (like the file source)
- Configurable poll interval, HDFS URI, and directory
- SCDF 2.11.5 compatible

## Usage

1. Configure `application.yml` or pass properties via environment/command line:
    - `hdfs.uri` (e.g., `hdfs://namenode:8020`)
    - `hdfs.directory` (e.g., `/user/data/incoming`)
    - `hdfs.poll-interval` (e.g., `5000` ms)
2. Build and run:
    ```sh
    mvn clean package
    java -jar target/hdfs-source-0.0.1-SNAPSHOT.jar
    ```
3. Register and deploy in Spring Cloud Data Flow as a source app.

## References
- [Spring Stream Applications Guide](https://docs.spring.io/stream-applications/docs/current/reference/html/index.html#)
- [Spring for Apache Hadoop](https://docs.spring.io/spring-hadoop/docs/current/reference/html/)

---

## Development
- Java 21
- Spring Boot 3.4.5
- Maven

## License
MIT
