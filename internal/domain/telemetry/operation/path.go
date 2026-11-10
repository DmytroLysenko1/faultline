package operation

import "strings"

const (
	rootPath        = "/"
	identifier      = "{id}"
	overflowSegment = "…"
	schemeSeparator = "://"
	schemeAlphabet  = letters + digits + "+.-"
)

type segments struct {
	list  [MaxPathSegments + 1]string
	count int
}

func (s *segments) items() []string {
	return s.list[:s.count]
}

func splitPath(path string) segments {
	var split segments
	segment, rest := nextSegment(path)
	for segment != "" && split.count < MaxPathSegments {
		split.add(segment)
		segment, rest = nextSegment(rest)
	}
	if segment != "" {
		split.add(overflowSegment)
	}
	return split
}

func (s *segments) add(segment string) {
	s.list[s.count] = segment
	s.count++
}

func nextSegment(path string) (string, string) {
	path = strings.TrimLeft(path, "/")
	segment, rest, _ := strings.Cut(path, "/")
	return segment, rest
}

func withoutQuery(path string) string {
	if end := strings.IndexAny(path, "?#"); end >= 0 {
		return path[:end]
	}
	return path
}

func pathOfURL(url string) string {
	scheme, rest, ok := strings.Cut(url, schemeSeparator)
	if !ok || !isScheme(scheme) {
		return withoutQuery(url)
	}
	end := strings.IndexAny(rest, "/?#")
	if end < 0 || rest[end] != '/' {
		return rootPath
	}
	return withoutQuery(rest[end:])
}

func isScheme(scheme string) bool {
	return scheme != "" && isLetter(scheme[0]) && onlyOf(scheme, schemeAlphabet)
}

func (f *Facts) path() (string, bool) {
	switch {
	case f.Path != "":
		return withoutQuery(f.Path), true
	case f.Target != "":
		return pathOfURL(f.Target), true
	case f.URL != "":
		return pathOfURL(f.URL), true
	}
	return "", false
}
