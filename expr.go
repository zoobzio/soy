package cereal

import (
	"github.com/zoobzio/astql"
)

// CastType represents SQL cast types.
// Re-exported from astql for convenience.
type CastType = astql.CastType

// Cast type constants.
const (
	CastText            CastType = astql.CastText
	CastInteger         CastType = astql.CastInteger
	CastBigint          CastType = astql.CastBigint
	CastSmallint        CastType = astql.CastSmallint
	CastNumeric         CastType = astql.CastNumeric
	CastReal            CastType = astql.CastReal
	CastDoublePrecision CastType = astql.CastDoublePrecision
	CastBoolean         CastType = astql.CastBoolean
	CastDate            CastType = astql.CastDate
	CastTime            CastType = astql.CastTime
	CastTimestamp       CastType = astql.CastTimestamp
	CastTimestampTZ     CastType = astql.CastTimestampTZ
	CastInterval        CastType = astql.CastInterval
	CastUUID            CastType = astql.CastUUID
	CastJSON            CastType = astql.CastJSON
	CastJSONB           CastType = astql.CastJSONB
	CastBytea           CastType = astql.CastBytea
)
