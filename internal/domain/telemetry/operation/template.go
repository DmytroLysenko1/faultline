package operation

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validateRoute(route string) string {
	if reason := validateRouteText(route); reason != "" || route == rootPath {
		return reason
	}
	return validateSegments(route[1:])
}

func validateRouteText(route string) string {
	switch {
	case len(route) > MaxTemplateBytes:
		return fmt.Sprintf("longer than %d bytes", MaxTemplateBytes)
	case !strings.HasPrefix(route, rootPath):
		return "does not start with /"
	case !utf8.ValidString(route) || strings.ContainsFunc(route, unicode.IsControl):
		return "is not printable UTF-8"
	}
	return ""
}

func validateSegments(path string) string {
	count := 0
	for segment := range strings.SplitSeq(path, "/") {
		count++
		if reason := validateSegment(segment); reason != "" {
			return reason
		}
	}
	if count > MaxPathSegments {
		return fmt.Sprintf("more than %d segments", MaxPathSegments)
	}
	return ""
}

func validateSegment(segment string) string {
	switch {
	case segment == "":
		return "has an empty segment"
	case isParameter(segment):
		return ""
	case strings.ContainsAny(segment, "{}"):
		return fmt.Sprintf("segment %q is neither a literal nor {name}", segment)
	case strings.ContainsAny(segment, "?#"):
		return fmt.Sprintf("segment %q holds a query or fragment character", segment)
	}
	return ""
}

func isParameter(segment string) bool {
	inner, ok := strings.CutPrefix(segment, "{")
	if !ok {
		return false
	}
	parameter, ok := strings.CutSuffix(inner, "}")
	return ok && parameter != "" && isParameterName(parameter)
}

func isParameterName(parameter string) bool {
	return onlyOf(parameter, letters+digits+"_")
}
