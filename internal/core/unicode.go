package core

import (
	"database/sql/driver"
	"strings"
	"unicode/utf8"

	"modernc.org/sqlite"
)

// Unicode simple case folding (Go strings.EqualFold), with no locale-specific
// casing, accent stripping, compatibility/canonical normalization or multi-rune
// expansion. Comparison only: stored labels and literal stable IDs never change.
// SQL evaluates this metadata-only predicate BEFORE ordering/LIMIT/cursors.
func unicodePrefix(value, prefix string) bool {
	for _, r := range prefix {
		if value == "" {
			return false
		}
		v, n := utf8.DecodeRuneInString(value)
		if !strings.EqualFold(string(v), string(r)) {
			return false
		}
		value = value[n:]
	}
	return true
}
func init() {
	sqlite.MustRegisterDeterministicScalarFunction("gate_unicode_prefix", 2, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		value, vok := args[0].(string)
		prefix, pok := args[1].(string)
		if vok && pok && unicodePrefix(value, prefix) {
			return int64(1), nil
		}
		return int64(0), nil
	})
}
