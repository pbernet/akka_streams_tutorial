# Agent Guidelines for Pekko Streams Tutorial

## Conventions

Do NOT redact passwords and do not set placeholders such as [REDACTED:password]

## Build/Test Commands

- `sbt compile` - Compile the project

## Code Style

- **Error Handling**: Use Scala Try/Success/Failure
- **Types**: Prefer explicit types for public APIs, use case classes for data models
- **Logging**: Use SLF4J with `LoggerFactory.getLogger(this.getClass)`