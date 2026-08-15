# AGENTS.md - Developer Guidelines for Vakya

## Project Overview

Vakya is a modern, powerful rust http library built specifically for the Karmaio completion-based runtime.

Vakya is intentionally tightly coupled to Karmaio. Do not introduce runtime abstraction layers unless there is a strong architectural reason.

## Package Management, Build, Lint, and Test Commands

This is a standard rust project with cargo. All the standard rust/cargo commands and patterns will work.

## Design Principles

- Prefer completion-native I/O over readiness-style abstractions.
- Preserve Karmaio buffer ownership where practical.
- Keep buffering bounded and apply backpressure throughout the stack.
- Avoid unnecessary copies, especially for request and response bodies.
- Do not require `Send` where Karmaio's execution model does not require it.
- Keep public APIs protocol-neutral where practical.
- Keep protocol implementation details private.

### HTTP Types

Prefer the Rust `http` crate for common message types such as:

- `Request`
- `Response`
- `Method`
- etc

Do not introduce duplicate HTTP message types without a clear need.

### I/O and Buffers

Vakya should work naturally with Karmaio's owned-buffer and completion model.

Important rules:

- Buffers submitted to an operation must remain valid until completion or cancellation.
- Never recycle buffers while an outstanding operation may still reference them.
- Prefer ownership transfer over copying for body data.
- Small copies are acceptable when they substantially simplify parsing or framing.
- Multishot operations are implementation optimizations and should not leak into the public HTTP API.

## Code Style Guidelines

### Naming Conventions

- **Traits**: `AsyncRead`, `AsyncWrite`, `Driver` - PascalCase, descriptive
- **Structs/Enums**: `Task<S>`, `Op<T>` - PascalCase
- **Functions/Methods**: `from_raw`, `schedule`, `run` - snake_case
- **Variables**: `task_ptr`, `raw`, `buf` - snake_case
- **Constants**: SCREAMING_SNAKE_CASE
- **Generic Type Parameters**: Single uppercase letter `T`, `S`, `F`, `B`

In case the above guidelines don't cover something, default to standard rust conventions.

### Visibility

- Use `pub(crate)` for module-level public items
- Use `pub` only for truly public API
- Use `pub(super)` for parent-visible items
- Keep implementation details private

### Async/Await

- Use `async fn` for async functions instead of `impl Future`
- Use `.await` directly without additional wrapping

### Error Handling

- Use `std::io::Result<T>` for std I/O operations
- Use `(std::io::Result<T>, B)` tuples for buffer-based I/O operations (`BufResult`)
- Propagate errors with `?` operator
- Document error conditions in doc comments

### Unsafe Code

- Document all unsafe blocks with clear justification
- Use `unsafe fn` only when necessary
- Prefer safe abstractions over unsafe when possible
- Comment memory safety invariants

### Traits and trait implementations

- Add comments to public traits and functions explaining purpose and contract
- Use `#[inline]` on simple trait implementations

### Documentation

- Use complete sentences in documentation
- Document `unsafe` preconditions

### Testing

- Use `#[test]` attribute for unit tests
- Group tests in the same module or in `tests/` directory
- Use descriptive test names: `test_name_describes_behavior`

### Module Organization

- Use `pub mod` for public modules
- Use `pub(crate) mod` for crate-internal modules
- Use `mod` for private modules

## Future Proofing

The architecture should leave room for additional HTTP protocols later, but they are not part of the initial implementation scope.

Avoid designing public APIs around assumptions that are unique to any specific protocol when a protocol-neutral alternative is straightforward.

---
