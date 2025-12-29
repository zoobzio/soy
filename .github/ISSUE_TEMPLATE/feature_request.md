---
name: Feature request
about: Suggest an idea for soy
title: '[FEATURE] '
labels: 'enhancement'
assignees: ''

---

**Is your feature request related to a problem? Please describe.**
A clear and concise description of what the problem is. Ex. I'm always frustrated when [...]

**Describe the solution you'd like**
A clear and concise description of what you want to happen.

**Describe alternatives you've considered**
A clear and concise description of any alternative solutions or features you've considered.

**Example usage**
```go
// Show how the feature would be used
type User struct {
    ID   int64  `db:"id" type:"bigserial primary key"`
    Name string `db:"name" type:"text"`
}

c, _ := soy.New[User](db, "users")

// Your proposed usage
```

**Additional context**
Add any other context, diagrams, or screenshots about the feature request here.
