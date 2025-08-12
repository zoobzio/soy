# Cereal

Cereal is a universal data access layer that provides a consistent CRUD API across different storage providers. It uses ASTQL (Abstract Syntax Tree Query Language) internally to build type-safe queries while exposing a simple interface to users.

## Architecture

### Core Design Principles

1. **Simple CRUD API** - Standard Get/Set/Delete/Exists operations work the same across all providers
2. **ASTQL-Powered** - Providers implement CRUD operations using ASTQL queries internally
3. **AST Storage** - Named queries store AST structures, not rendered SQL, allowing for provider-specific optimizations
4. **Direct Results** - No encoding/decoding layer - providers return raw bytes for users to unmarshal
5. **Extensible** - Execute method allows running custom ASTQL queries beyond standard CRUD

### Key Components

- **Provider Interface** (`provider.go`) - Defines the contract all providers must implement
- **Query Structure** (`types.go`) - Supports both named queries and direct AST execution
- **Common Errors** (`errors.go`) - Standardized error types across providers
- **Explicit Parameters** - No URI parsing, direct table/keyField/keyValue parameters

### Provider Implementation

Each provider (e.g., postgres) implements the Provider interface by:

1. Converting CRUD operations to ASTQL AST structures
2. Rendering AST to provider-specific query format
3. Executing queries and returning JSON-encoded results
4. Managing named query storage as AST structures

## Usage Example

```go
// Initialize provider
provider, err := postgres.New("postgres://localhost/mydb")
defer provider.Close()

// Basic CRUD with explicit parameters
err = provider.Set(ctx, "users", "id", "123", userData)
data, err := provider.Get(ctx, "users", "id", "123")
exists, err := provider.Exists(ctx, "users", "id", "123")
err = provider.Delete(ctx, "users", "id", "123")

// Works with any primary key field
err = provider.Set(ctx, "products", "sku", "ABC-123", productData)
data, err = provider.Get(ctx, "accounts", "email", "user@example.com")

// Batch operations
keys := []string{"100", "101", "102"}
results, errs := provider.BatchGet(ctx, "orders", "order_id", keys)

// Custom queries with ASTQL
query := postgres.Select(astql.T("users")).
    Where(astql.C(astql.F("age"), astql.GT, astql.P("min_age")))

ast, _ := query.Build()
provider.RegisterQuery("adult_users", ast)

result, err := provider.Execute(ctx, cereal.Query{
    Name: "adult_users",
    Parameters: map[string]interface{}{"min_age": 18},
})
```

## Providers

### PostgreSQL

The postgres provider demonstrates the architecture:

- CRUD operations build ASTQL queries internally
- ON CONFLICT for upserts in Set operation
- Batch operations with efficient IN queries
- Named query storage and execution
- Direct AST execution support

### Future Providers

The same pattern can be implemented for:
- MongoDB (using ASTQL's document query builders)
- DynamoDB (using ASTQL's key-value patterns)
- Redis (using ASTQL's cache-oriented operations)
- SQLite (reusing postgres patterns)

## Benefits

1. **Consistent API** - Same interface regardless of underlying storage
2. **Type Safety** - ASTQL provides compile-time query validation via Sentinel
3. **Query Reuse** - Store complex queries as AST for repeated execution
4. **Provider Flexibility** - Each provider can optimize AST rendering for its backend
5. **No Magic** - Direct control over data marshaling/unmarshaling