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

K&R brace style: opening braces on the same line for `if`, `for`, `while`, and
`do`, but on a separate line for function definitions.

`if`, `for`, `while`, and `do` always use braces, even when the body is a
single statement.

### Identifiers

`lower_snake_case` for all identifiers except macros, which use
`UPPER_SNAKE_CASE`. No double underscores.

### Comments

Use only `/* */` in committed code, not `//`.
