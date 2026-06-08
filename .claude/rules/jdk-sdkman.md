# JDK 11 and sdkman

This project uses **JDK 11**. Java is managed with sdkman (`.sdkmanrc` specifies the version).

## When running tests or Maven

- Prefer running from the project root so workspace settings apply.
- In a fresh shell, run `sdk env install` in the project root first, then `mvn test` or `mvn clean install`.
- Always use JDK 11 — do not assume a different version (e.g. system default).
