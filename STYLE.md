# dqlite style guide

## Introduction

This document records style guidance for new dqlite code. Due to inconsistent
style preferences by the authors over time and imperfect enforcement, existing
code may not follow these guidelines.

## Enforcement

The repository includes `.clang-format` and `.clang-tidy` files for use with
the corresponding LLVM tools. These can be used to enforce many of the
guidelines listed below. The remaining guidelines are intended to be enforced
manually by authors and maintainers, or broken when necessary for practical or
aesthetic reasons.

## Guidelines

### Whitespace

Tabs to indent at the start of a line; spaces to align after that. Tab stops
are every eight spaces.

### Braces

Opening braces on the same line for `struct`, `enum`, `if`, `for`, `while`, and
`do`, but on a separate line for function definitions.

`if`, `for`, `while`, and `do` always use braces, even when the body is a
single statement.

### Identifiers

`lower_snake_case` for all identifiers except macros, which use
`UPPER_SNAKE_CASE`. No double underscores.

### Comments

Use only `/* */` in committed code, not `//`.

Align end-of-line comments after struct field and enum variant declarations.

### Predicates

Use explicit comparisons to 0 and `NULL`: `if (int_variable == 0)` and `if
(ptr_variable != NULL)`, not `if (!int_variable)` and `if (ptr_variable)`.
Leave out the explicit comparison only for `bool` variables.

The predicate of an `if` or `while` should not mutate state or have side
effects.

### Macros

Prefer functions to function-like macros in new code.

Use defensive parentheses in the definitions of function-like macros, so that
extra parentheses are never required around the arguments at call sites.

### Functions

In function bodies, cast an argument to `void` if it is unused.
