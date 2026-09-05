# Code style

Write succinct, to-the-point code. Minimal and easy to read beats clever,
defensive, or micro-optimized. When in doubt, write less.

## Principles

- Write succinct, to-the-point code.
- Avoid unnecessary early outs: checking for empty collections before
  iterating, zeros before adding, ones before multiplying, and similar.
- Let the general case handle the degenerate one.
- Prefer simple clear code over micro-optimized code.
- Don't add abstraction, configuration, or error handling for cases that
  don't exist yet.
- No dead code, no commented-out code, no "just in case" helpers.
- After a refactor, recursively follow up with cleanup to avoid code
  structure we would not create from scratch.

## Naming

- Name domain concepts with type aliases, even trivial ones. Prefer an
  alias over a struct when the concept is just a tuple of numbers.
- Short names for short scopes (`x`, `y`, `n`, `f`, `dx`), full words for
  anything that escapes a function.
- Name files after the concept they hold: `camera`, `sdf`, `octree`, `elo`.

## Structure

- One concept per file.
- Split serialization out of logic, in separate files: `camera` and
  `camera_io`, `sdf` and `sdf_io`.
- Keep pure computation free of I/O and framework imports, so it stays
  directly testable.
- Keep functions small enough to read at once. Files stay well under a few
  hundred lines; split by concept when they grow.
- Free functions taking the data they need are fine; don't wrap everything
  in a type just to get a method.
- Tunable constants go at the top as named constants, not buried as
  literals.
- Callees come before callers in a file.
- Prefer repo-relative paths over absolute paths.

## Functional style

Comprehensions, generators, `map`/`filter`, and iterator chains all count as
functional constructs here.

- Never use a functional construct for side effects. If the work of the loop
  is to mutate, print, or write, use a plain loop. A functional construct
  produces a value; that is the only thing it is for.
- When producing a value, prefer the functional form — but only while it
  stays one short, readable expression.
- If it won't fit, don't reach for a loop yet: first try naming the inner
  step as its own function and applying the functional form to that. Loops
  are for when it genuinely can't be factored short.
- A long single expression is worse than a loop. Length is the deciding
  factor, so judge the result, not the style.

## Formatting

- Keep lines short. Most lines land under 60 characters; ~100 is the hard
  ceiling.
- Don't reformat files you didn't otherwise have to touch.

## Comments and docs

Comments are sparse and earn their place.

- Comment *why*, or a subtlety a reader would otherwise get wrong. Never
  restate the signature or narrate the next line.
- No section banners, no decorative separators, no changelogs in comments.
- Doc comments go on public API and non-obvious math. A one-line module
  header stating scope is good.
- Where the reasoning is genuinely subtle (a derivation, an invariant, a
  normalization choice), a real paragraph is welcome — but in the doc
  comment or the README, not scattered inline.

## Dependencies

Default to zero new dependencies. Keep it to at most one per project, and
write renderers, parsers, optimizers, and file I/O from scratch. Writing 40
lines beats taking a dependency. Ask before introducing one.

## Tests

- Tests must be useful and test something meaningful. Content and names
  describe the current state of the code.
- Name tests after the behavior asserted, as a sentence:
  `stats_match_unit_square_area`, `triangle_cycle_becomes_three_quads`.
  Never `test_1`, never restate the function name alone.
- Assert on invariants and relationships (symmetry, conservation, scaling),
  not just golden numbers.
- Build small fixture helpers for test data instead of repeating literals.
- Delete tests that no longer describe the code instead of nursing them.

## Performance

Speed comes from design, not from tightening code. Work this list in order;
each step beats everything below it.

1. The right algorithm. No amount of tuning recovers a bad one.
2. The right data structures for how the data is actually accessed.
3. A design that minimizes how much data gets processed at all — don't
   recompute, don't copy, don't allocate per item, don't walk the same data
   twice.
4. A memory layout the cache likes: contiguous arrays over pointer chasing,
   sequential access over random, and the fields a hot loop reads kept
   together.
5. Micro-optimizations, last and least. Sometimes genuinely needed, but only
   in a spot measurement has shown to be hot, and only once everything above
   is already right. This is the one case that outranks preferring clear
   code over optimized code — when it does, leave the measurement that
   justifies it in a comment.

- Don't optimize on speculation. A clear implementation that is fast enough
  is finished.
- When asked to optimize, measure before and after and report both. After
  changing a hot loop, run a performance comparison.

## Committing

- Split separable work into separate commits: one step or concern each.
- Always separate refactors from behavior changes.
- Prefix refactoring commits with `refactor: `. Other prefixes in use:
  `bugfix: `, `docs: `, `build: `. Skip conventional-commit ceremony
  otherwise; don't use `feat:` or `chore:`.
- Every commit must pass the repo's check — see the language sections.

## Rust

- Type aliases for domain concepts: `type Position = (i32, i32);`,
  `pub type Color = [u8; 4];`.
- No `unsafe`.
- Derive what's needed and no more; `Clone, Debug, Copy, PartialEq` cover
  almost everything.
- Propagate with `?` in library code. `unwrap()`/`expect()` are acceptable
  in binaries, tools, and tests where failure should just stop the program.
- Don't use `for_each` to run side effects; that is what `for` is for.
- `main.rs`: `mod` declarations, then `use`, then `const` configuration,
  then `main`.
- Tests live in `#[cfg(test)] mod tests` at the bottom of the file they
  test, with `use super::*` and small fixture functions.
- Commits must pass `cargo check`, or `cargo test` where the repo says so.

## Python

- Type-hint signatures. Use builtin generics (`list[str]`,
  `dict[str, float]`, `tuple[int, int]`) in new code.
- Type aliases for domain concepts: `Grid = dict[Position, int]`.
- `yield` for streaming results rather than building and returning a list.
- A one-line module docstring stating scope is good, e.g.
  `"""Pure Elo rating math. No I/O, no framework dependencies."""`
- Keep pure logic in its own module, separate from FastAPI/Flask wiring
  and from persistence.
- Tests: pytest, plain `def test_...` functions, no test classes,
  `pytest.approx` for floats.

## C

- `#pragma once` in headers.
- `snake_case`; `typedef struct { ... } thing_t;`.
- Prefix functions with their type: `csr_create`, `csr_destroy`, `csr_ok`.
- Trailing comments to document array lengths on struct fields:
  `int *colind;  // nnz`
- Plain `Makefile`, `-Wall -Wextra`, no build-system generators.

## READMEs

- Open with one paragraph saying what it is and what it's built on.
- Hard-wrap prose around 76 columns.
- Document the actual commands to run it.
- An image is a great way to explain things.

## Precedence

A repo's own `AGENTS.md` overrides this file. Read it first; it carries
repo-specific exceptions (formatting opt-outs, allowed duplication,
conversion scripts).
