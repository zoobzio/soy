package cereal

// ConditionSpec represents a WHERE condition in a serializable format.
// This can be used to build queries from JSON or other external sources (e.g., LLM-generated).
type ConditionSpec struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Param    string `json:"param"`
	IsNull   bool   `json:"is_null,omitempty"`
}

// OrderBySpec represents an ORDER BY clause in a serializable format.
type OrderBySpec struct {
	Field     string `json:"field"`
	Direction string `json:"direction"` // "asc" or "desc"
}

// QuerySpec represents a SELECT query that returns multiple records in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "fields": ["id", "email", "name"],
//	  "where": [
//	    {"field": "age", "operator": ">=", "param": "min_age"},
//	    {"field": "status", "operator": "=", "param": "status"}
//	  ],
//	  "order_by": [{"field": "name", "direction": "asc"}],
//	  "limit": 10,
//	  "offset": 20
//	}
type QuerySpec struct {
	Fields   []string        `json:"fields,omitempty"`
	Where    []ConditionSpec `json:"where,omitempty"`
	OrderBy  []OrderBySpec   `json:"order_by,omitempty"`
	Limit    *int            `json:"limit,omitempty"`
	Offset   *int            `json:"offset,omitempty"`
	Distinct bool            `json:"distinct,omitempty"`
}

// SelectSpec represents a SELECT query that returns a single record in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "fields": ["id", "email"],
//	  "where": [
//	    {"field": "email", "operator": "=", "param": "user_email"}
//	  ],
//	  "order_by": [{"field": "created_at", "direction": "desc"}],
//	  "limit": 1
//	}
type SelectSpec struct {
	Fields   []string        `json:"fields,omitempty"`
	Where    []ConditionSpec `json:"where,omitempty"`
	OrderBy  []OrderBySpec   `json:"order_by,omitempty"`
	Limit    *int            `json:"limit,omitempty"`
	Offset   *int            `json:"offset,omitempty"`
	Distinct bool            `json:"distinct,omitempty"`
}

// UpdateSpec represents an UPDATE query in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "set": {
//	    "name": "new_name",
//	    "age": "new_age"
//	  },
//	  "where": [
//	    {"field": "id", "operator": "=", "param": "user_id"}
//	  ]
//	}
type UpdateSpec struct {
	Set   map[string]string `json:"set"`
	Where []ConditionSpec   `json:"where"`
}

// DeleteSpec represents a DELETE query in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "where": [
//	    {"field": "id", "operator": "=", "param": "user_id"}
//	  ]
//	}
type DeleteSpec struct {
	Where []ConditionSpec `json:"where"`
}

// AggregateSpec represents an aggregate query (COUNT/SUM/AVG/MIN/MAX) in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON for COUNT:
//
//	{
//	  "where": [
//	    {"field": "status", "operator": "=", "param": "active"}
//	  ]
//	}
//
// Example JSON for SUM/AVG/MIN/MAX:
//
//	{
//	  "field": "amount",
//	  "where": [
//	    {"field": "status", "operator": "=", "param": "paid"}
//	  ]
//	}
type AggregateSpec struct {
	Field string          `json:"field,omitempty"` // Required for SUM/AVG/MIN/MAX, not used for COUNT
	Where []ConditionSpec `json:"where,omitempty"`
}
