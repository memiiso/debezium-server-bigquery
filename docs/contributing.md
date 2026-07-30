# Contributing to Debezium BigQuery Sink

We welcome contributions of all kinds, including bug reports, documentation updates, performance enhancements, and new feature implementations.

## Getting Started

### Prerequisites
- JDK 21 or later
- Apache Maven 3.6.3 or higher
- Docker (optional, required for running integration tests with emulators)

### Building the Project

From the project root directory, run:

```bash
# Build project without running tests
mvn clean package -DskipTests

# Build project with assembly package
mvn clean package -Passembly -DskipTests
```

### Running Tests

Unit and integration tests can be run using Maven:

```bash
# Run unit tests
mvn test

# Run tests skipping long integration tests
mvn test -DskipITs
```

## Submitting Pull Requests

1. **Fork and Branch:** Create a feature or bugfix branch off `master`.
2. **Code Style:** Ensure code follows Java code formatting and includes relevant docstrings and unit tests.
3. **Commit Messages:** Use clear, descriptive commit messages outlining your changes.
4. **Pull Request:** Submit your pull request to the `master` branch of `memiiso/debezium-server-bigquery`.

## License

By contributing code, documentation, or other materials to this project, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
