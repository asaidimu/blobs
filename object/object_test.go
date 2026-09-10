package object

import (
	"strings"
	"testing"
)

func TestValidateNamespaceID(t *testing.T) {
	valid := []string{
		"ab",
		"abc-def",
		"abc_def",
		"a1-_b2",
		"admissions",
		strings.Repeat("a", 63),
		"a" + strings.Repeat("_", 61) + "b",
	}
	for _, id := range valid {
		if err := ValidateNamespaceID(id); err != nil {
			t.Errorf("ValidateNamespaceID(%q) = %v, want nil", id, err)
		}
	}

	invalid := []string{
		"",
		"a",
		"Abc",
		"abc def",
		"abc/def",
		"-abc",
		"abc-",
		"_abc",
		"abc_",
		"ABC_DEF",
		strings.Repeat("a", 64),
	}
	for _, id := range invalid {
		if err := ValidateNamespaceID(id); err == nil {
			t.Errorf("ValidateNamespaceID(%q) = nil, want error", id)
		}
	}
}
