# Code Style (NebulaFS)

## Commenting Guidelines

We use comments to explain intent and non-obvious decisions. Avoid line-by-line commentary.

### Doxygen (`///`)
Use Doxygen only for public API surfaces where generated docs are valuable:
- Public structs/classes in `include/`
- Public functions in `include/` that are part of the API

Keep these concise (one sentence). Avoid repeating the identifier name.

### Plain comments (`//`)
Use `//` in implementation files for:
- Why a design choice exists
- Non-obvious behavior (e.g., atomic writes, range parsing)
- Subtle or tricky logic

Avoid commenting trivial statements or obvious code.

## Naming
- Types: `PascalCase`
- Functions/vars: `snake_case`
- Constants: `kCamelCase`

## Error Handling
- Use `Result<T>` for cross-module errors
- Map errors to HTTP JSON envelope at the boundary

## Formatting
- `clang-format` applies to all C++ sources
- Keep lines under 100 columns
