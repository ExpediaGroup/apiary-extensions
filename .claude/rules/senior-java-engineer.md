# Java AGENTS.md Guidelines
<!-- guideline-version: 1.0 — increment when modifying; /agents-md update uses this to detect stale project rule files -->

Java-specific patterns for JVM projects. Use with [jvm.md](jvm.md) for build, dependency management, and shared practices.

---

## Language Version & Style

- **Java 11** — do not use language features introduced after Java 11 (no records, sealed classes, text blocks, pattern matching, var in lambdas, etc.)
- Follow [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- Use **Spotless** with Google Java Format (configured in `pom.xml`); run `mvn spotless:apply` / `mvn spotless:check`
- Naming: `PascalCase` classes, `camelCase` methods/variables, `SCREAMING_SNAKE_CASE` constants, `lowercase.dotted` packages

---

## Java 11 Features Available

- **`var`** for local variable type inference (avoid where it hurts readability)
- **`List.of()`, `Set.of()`, `Map.of()`** for immutable collections
- **`Optional`** chaining, `String` methods (`strip()`, `isBlank()`, `lines()`, `repeat()`)
- **Switch expressions** (preview in 11, use with caution — check if enabled in `pom.xml`)
- **No** records, sealed classes, text blocks, pattern matching — these require Java 14+/17+

---

## Functional Java

- Prefer **immutable collections**: `List.of()`, `Set.of()`, `Map.of()`, `List.copyOf()`
- Use `final` on local variables and fields to signal immutability intent
- Use **Streams** for collection transformations instead of imperative loops:

```java
var activeUserNames = users.stream()
    .filter(User::isActive)
    .map(User::name)
    .sorted()
    .toList();
```

- Use **method references** over lambdas where the intent is clearer: `User::name` not `u -> u.name()`
- Chain **Optional** operations rather than null-checking:

```java
// ❌ Avoid
String name = null;
if (user != null && user.address() != null) {
  name = user.address().city();
}

// ✅ Prefer
String name = Optional.ofNullable(user)
    .map(User::address)
    .map(Address::city)
    .orElse("unknown");
```

---

## Architecture

- Prefer **composition over inheritance**; use constructor injection for dependencies
- Keep **domain logic pure and framework-agnostic** (hexagonal/ports-and-adapters)
- Keep framework code (Spring, etc.) at the edges
- Maximum method length: ~40 lines; maximum class length: ~500 lines
- Use `@NonNull` / `@Nullable` annotations at API boundaries; avoid returning `null` from public methods — use `Optional<T>` instead

---

## Error Handling

Prefer unchecked exceptions for domain errors; use `Optional<T>` as a return type when absence is a normal outcome.

```java
// Domain exceptions — unchecked
public class UserNotFoundException extends RuntimeException {
  public UserNotFoundException(String userId) {
    super("User not found: " + userId);
  }
}

// Optional for normal absence
public Optional<User> findUser(String id) {
  return repository.findById(id);
}

// Custom exception with context — preferred for domain errors
public class UserNotFoundException extends RuntimeException {
  public UserNotFoundException(String userId) {
    super("User not found: " + userId);
  }
}
```

- Use checked exceptions only when the caller **must** handle the failure and recovery is meaningful
- Do not use exceptions for control flow
- Always include context in exception messages (`"User not found: userId=" + id`)

---

## Concurrency

- Use **`CompletableFuture`** for async pipelines:

```java
CompletableFuture.supplyAsync(() -> repository.findById(id))
    .thenApply(User::toDto)
    .exceptionally(e -> UserDto.empty());
```

- Avoid `synchronized` on broad scopes; prefer `java.util.concurrent` types (`ConcurrentHashMap`, `AtomicReference`)
- Virtual threads are not available in Java 11 — use thread pools from `Executors` where concurrency is needed

---

## Logging

Use **SLF4J** as the facade (`LoggerFactory.getLogger`) with **reload4j** as the implementation (bound via `slf4j-reload4j`).

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserService {
  private static final Logger log = LoggerFactory.getLogger(UserService.class);

  public User getUser(String id) {
    log.debug("Fetching user: userId={}", id);
    try {
      User user = repository.findById(id);
      log.info("User found: userId={}", user.getId());
      return user;
    } catch (Exception e) {
      log.error("Failed to fetch user: userId={}", id, e);
      throw new UserNotFoundException(id);
    }
  }
}
```

Log levels: `DEBUG` for diagnostic traces, `INFO` for lifecycle events, `WARN` for recoverable failures, `ERROR` for unhandled exceptions. Use parameterised logging (`{}`) — never string concatenation.

---

## Testing

### Frameworks
- **JUnit 4** — `@RunWith(MockitoJUnitRunner.class)`, `@Before`, `@Test`
- **Mockito 2** — `@Mock`, `when().thenReturn()`, `verify()`
- **Hamcrest** — `assertThat(x, is(...))`, `assertThat(list, hasSize(...))`

### Test structure
```java
@RunWith(MockitoJUnitRunner.class)
public class UserServiceTest {

  @Mock
  private UserRepository repository;

  private UserService service;

  @Before
  public void init() {
    service = new UserService(repository);
  }

  @Test
  public void getUser_whenFound_returnsUser() {
    // Given
    when(repository.findById("123")).thenReturn(Optional.of(mockUser));

    // When
    Optional<User> user = service.getUser("123");

    // Then
    assertThat(user.isPresent(), is(true));
    assertThat(user.get().getId(), is("123"));
    verify(repository).findById("123");
  }

  @Test(expected = UserNotFoundException.class)
  public void getUser_whenNotFound_throwsException() {
    when(repository.findById("999")).thenReturn(Optional.empty());
    service.getUser("999");
  }
}
```

### Mockito
- Use `@RunWith(MockitoJUnitRunner.class)` with `@Mock` fields — do not use inline `mock()` calls
- Use `when().thenReturn()` for stubbing, `verify()` for interaction checks
- Prefer specific matchers over `any()` where intent matters
- Prefer concrete inputs; avoid randomness and time-dependent logic in tests

---

## Anti-patterns

- ❌ Returning `null` from public methods — use `Optional<T>`
- ❌ Catching `Exception` or `Throwable` generically
- ❌ Catching, logging, and re-throwing — pick one: either handle it, or let it propagate (log at the boundary where you handle it)
- ❌ Checked exceptions for control flow
- ❌ Mutable DTOs with getters/setters — use records or immutable classes
- ❌ Raw types (`List` instead of `List<String>`)
- ❌ String concatenation in loops — use `StringBuilder` or streams
- ❌ `new HashMap<>()` for read-only maps — use `Map.of()`
- ❌ `static` mutable state
- ❌ Deep inheritance hierarchies — prefer composition
- ❌ `System.out.println` or `java.util.logging` — use SLF4J
- ❌ Overusing `@SuppressWarnings("unchecked")` — fix the root cause
- ❌ Overusing inline `mock()` calls — prefer `@Mock` fields with `@RunWith(MockitoJUnitRunner.class)`
