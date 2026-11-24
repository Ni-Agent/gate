package core

import "testing"

func TestUnicodePrefixPolicy(t *testing.T) {
	for _, tc := range []struct {
		value, prefix string
		want          bool
	}{
		{"École", "éC", true}, {"Σχολείο", "σχ", true}, {"Kelvin", "kEL", true},
		{"Ab%_[X]", "aB%_[", true}, {"Ab%_[X]", "ab_", false},
		{"gate:tg:literal", "GATE:TG:L", true}, {"École", "E\u0301", false},
		{"E\u0301cole", "é", false}, {"Straße", "strass", false}, {"İstanbul", "i", false},
		{"short", "shorter", false}, {"anything", "", true},
	} {
		if got := unicodePrefix(tc.value, tc.prefix); got != tc.want {
			t.Errorf("value=%q prefix=%q got=%v want=%v", tc.value, tc.prefix, got, tc.want)
		}
	}
}
