# apiary-extensions

A collection of optional extensions to [Apiary](https://github.com/ExpediaGroup/apiary) — a managed Hive Metastore solution. Extensions integrate with Hive Metastore event/hook APIs to provide GlueSync, Kafka/SNS event publishing, metastore auth, Ranger auth, and metrics. Published to Maven Central as a library, not deployed as a service.

## Goals

- Provide reliable, well-tested Hive Metastore extensions consumed by downstream Apiary deployments
- Maintain backward compatibility: this is a library with external consumers
- Keep extensions loosely coupled — each module is independently consumable
- Support both Hive 2.x and Hive 3.x consumers where relevant (check per-module pom.xml)

## Constraints

- **Java 11** — no Java 12+ language features
- **Hive/Hadoop versions are pinned** in the parent POM — do not bump without cross-checking all modules
- **No Spring Boot or DI framework** — configuration is via `org.apache.hadoop.conf.Configuration` and environment variables
- **Apache 2.0 OSS project** — all new files require the standard copyright header (see existing files for the template)
- **JUnit 4** — new tests should follow existing JUnit 4 patterns (`@RunWith(MockitoJUnitRunner.class)`, `@Before`, `@Test`)

## Project Structure

```
apiary-extensions/
├── pom.xml                              # Parent POM — all dependency versions declared here
├── apiary-metastore-events/             # Event model
│   ├── apiary-hive-events/              # Core ApiaryListenerEvent abstractions
│   ├── kafka-metastore-events/          # Kafka serialization of events
│   └── sns-metastore-events/            # SNS serialization of events
├── apiary-metastore-metrics/            # Micrometer metrics support (Prometheus)
├── hive-event-listeners/                # Hive MetaStoreEventListener / PreEventListener impls
│   ├── apiary-gluesync-listener/        # Sync Hive metadata → AWS Glue Data Catalog
│   ├── apiary-metastore-auth/           # Auth listener
│   └── apiary-ranger-metastore-plugin/  # Apache Ranger authorization plugin
└── hive-hooks/                          # MetaStoreFilterHook implementations
```

Dependency versions are always declared as properties in the **root `pom.xml`** and referenced via `${property.version}` in child modules — never hard-coded in child POMs.

## Build Configuration

```bash
mvn clean install          # Build all modules
mvn clean test             # Run all tests
mvn clean verify           # Tests + integration tests
mvn clean package -DskipTests  # Build without tests
mvn spotless:apply         # Format code
mvn spotless:check         # Check formatting (run in CI)
mvn versions:display-dependency-updates  # Check for dependency updates
```

Requires **JDK 11** — use `sdk env install` in the repo root (see `.sdkmanrc`).

## Extension Patterns

### Listeners

All event listeners extend either `MetaStoreEventListener` (post-event) or `MetaStorePreEventListener` (pre-event, can veto). Constructors accept `org.apache.hadoop.conf.Configuration` only — no other injection.

```java
public class MyListener extends MetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(MyListener.class);

  public MyListener(Configuration config) {
    super(config);
  }

  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    // handle event
  }
}
```

### Configuration

Use environment variables for runtime configuration; read them via `System.getenv()` in the constructor or a dedicated config class. Do not use system properties or Hadoop config keys unless already established for a given listener.

```java
private final String skipArchive = System.getenv("GLUE_SKIP_ARCHIVE");
```

### Events (apiary-hive-events)

Events are modeled as `ApiaryListenerEvent` subclasses (one per Hive metastore event type). When adding a new event type, extend `ApiaryListenerEvent`, add a corresponding factory method to `ApiaryListenerEventFactory`, and add serialization support in both `kafka-metastore-events` and `sns-metastore-events` if the event should be published.

### Shading

Some modules shade dependencies to avoid classpath conflicts on the Hive Metastore classpath. The shade prefix is `${expediagroup.shaded.prefix}` (defined in parent POM). Only shade when necessary; document why in the module's `pom.xml`.

## Dependency Management

All versions are properties in the **root `pom.xml`**:

```xml
<properties>
  <aws-java-sdk.version>1.12.276</aws-java-sdk.version>
  <hive.version>2.3.7</hive.version>
  <hadoop.version>2.7.1</hadoop.version>
</properties>
```

- Declare new dependencies in `<dependencyManagement>` of the root POM with a version property
- Reference in child POMs without a version: `<groupId>...</groupId><artifactId>...</artifactId>`
- Use `provided` scope for Hive and Hadoop dependencies — they are on the classpath at runtime in the metastore JVM

## Testing

### Frameworks (JUnit 4)

- **JUnit 4** — `@RunWith(MockitoJUnitRunner.class)`, `@Before`, `@Test`
- **Mockito 2** — `@Mock`, `when().thenReturn()`, `verify()`
- **Hamcrest** — `assertThat(x, is(...))`, `assertThat(list, hasSize(...))`
- **beeju** — in-memory Hive Metastore for integration tests that need a real HMS

### Test structure

```java
@RunWith(MockitoJUnitRunner.class)
public class MyListenerTest {

  @Mock
  private SomeDependency dependency;

  private MyListener listener;

  @Before
  public void init() {
    listener = new MyListener(new Configuration());
  }

  @Test
  public void onCreateTable_whenValidEvent_delegatesToService() throws MetaException {
    // Given
    when(dependency.doThing()).thenReturn(expectedResult);

    // When
    listener.onCreateTable(mockEvent);

    // Then
    assertThat(result, is(expectedResult));
    verify(dependency).doThing();
  }
}
```

### Copyright header

Every new `.java` file requires the Apache 2.0 header. Copy from any existing source file — the year range must include the current year.

```java
/**
 * Copyright (C) 2018-2026 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 */
```

## Logging

SLF4J facade with reload4j as the implementation. One logger per class, `static final`, named after the class.

```java
private static final Logger log = LoggerFactory.getLogger(MyClass.class);
```

Log levels: `DEBUG` for trace/diagnostics, `INFO` for lifecycle events, `WARN` for recoverable failures, `ERROR` for exceptions. Always use parameterized form — never string concatenation.

## Git Workflow

### Branching

**REQUIRED**: Always create a branch before starting work. Never commit directly to `main`.

Branch format: `{type}/EGDL-XXXX/<short-name>` (see `.claude/rules/git-branches.md`)

### Commit messages

```
type: short description

Longer body if needed.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

**REQUIRED**: Include `Co-Authored-By` in every AI-assisted commit.

### Releases

Releases are managed by the **maven-release-plugin** (`[maven-release-plugin] prepare release ...` commits). Do not manually bump the version in `pom.xml` files — use `mvn release:prepare` and `mvn release:perform`. Version follows semver; changes are tracked in `CHANGELOG.md`.

## Boundary Conditions

### Always ask before

- Bumping pinned Hive, Hadoop, or AWS SDK versions — these have downstream compatibility implications
- Adding or removing shading from a module
- Making changes to `ApiaryListenerEvent` or `ApiaryListenerEventFactory` that affect serialization (breaking change for SNS/Kafka consumers)
- Modifying the parent POM's `dependencyManagement` to remove or change an existing entry
- Adding a new top-level Maven module to the parent POM

### Never do

- Commit directly to `main`
- Hard-code credentials, AWS account IDs, or region strings — use env vars or config
- Use `System.out.println` — always use SLF4J
- Add `<version>` tags to child POM dependencies that are already in parent `<dependencyManagement>`
- Upgrade AWS SDK to v2 without explicit discussion
- Remove the Apache 2.0 copyright header from any file
- Skip `spotless:check` in CI or commit unformatted code

## Documentation Standards

- **CHANGELOG.md** — update for every user-visible change (new feature, bug fix, breaking change)
- Each module has its own `README.md` — update it when adding new env vars, config, or behavior
- Public API classes (especially event types and listener interfaces) require Javadoc focused on contract and usage
- Keep examples in READMEs realistic — use actual class/method names, not placeholder code
