---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: 'bug'
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Create Soy instance with '...'
2. Execute query '...'
3. See error

**Code Example**
```go
// Minimal code example that reproduces the issue
package main

import (
    "context"
    "github.com/zoobzio/soy"
)

type User struct {
    ID   int64  `db:"id" type:"bigserial primary key"`
    Name string `db:"name" type:"text"`
}

func main() {
    // Your code here
}
```

**Expected behavior**
A clear and concise description of what you expected to happen.

**Actual behavior**
What actually happened, including any error messages or stack traces.

**Environment:**
 - OS: [e.g. macOS, Linux, Windows]
 - Go version: [e.g. 1.23.2]
 - soy version: [e.g. v0.1.0]
 - Database: [e.g. PostgreSQL 16.0]

**Additional context**
Add any other context about the problem here.
