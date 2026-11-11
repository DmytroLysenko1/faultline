package operation

import (
	"fmt"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

const (
	MaxTemplateBytes    = MaxNameBytes - len(methodOther) - len(" ")
	MaxRoutesPerService = 256
	MaxRoutes           = 4096
	MaxMatchVisits      = 256
	noRoute             = -1
)

type ServiceRoutes struct {
	Service resource.ServiceRef
	Routes  []string
}

type RuleError struct {
	Service int
	Route   int
	Reason  string
}

func (e *RuleError) Error() string {
	if e.Route == noRoute {
		return fmt.Sprintf("%s: service %d: %s", ErrInvalidRules, e.Service, e.Reason)
	}
	return fmt.Sprintf("%s: service %d, route %d: %s", ErrInvalidRules, e.Service, e.Route, e.Reason)
}

func (e *RuleError) Unwrap() error {
	return ErrInvalidRules
}

type Rules struct {
	services map[resource.ServiceRef]*node
}

type node struct {
	literals map[string]*node
	param    *node
	template string
}

func NewRules(services []ServiceRoutes) (*Rules, error) {
	rules := &Rules{
		services: make(map[resource.ServiceRef]*node, len(services)),
	}
	total := 0
	for i, service := range services {
		if err := rules.addService(i, service); err != nil {
			return nil, err
		}
		total += len(service.Routes)
		if total > MaxRoutes {
			return nil, &RuleError{
				Service: i,
				Route:   noRoute,
				Reason:  fmt.Sprintf("more than %d routes", MaxRoutes),
			}
		}
	}
	return rules, nil
}

func (r *Rules) addService(index int, service ServiceRoutes) error {
	if err := r.checkService(index, service); err != nil {
		return err
	}
	root := &node{}
	for i, route := range service.Routes {
		if reason := root.insert(route); reason != "" {
			return &RuleError{
				Service: index,
				Route:   i,
				Reason:  reason,
			}
		}
	}
	if visits := root.worstWalk(); visits > MaxMatchVisits {
		return &RuleError{
			Service: index,
			Route:   noRoute,
			Reason:  fmt.Sprintf("rules too ambiguous: a path can visit %d nodes, at most %d", visits, MaxMatchVisits),
		}
	}
	r.services[service.Service] = root
	return nil
}

func (r *Rules) checkService(index int, service ServiceRoutes) error {
	switch {
	case service.Service.Name == "":
		return &RuleError{
			Service: index,
			Route:   noRoute,
			Reason:  "service name is missing",
		}
	case r.services[service.Service] != nil:
		return &RuleError{
			Service: index,
			Route:   noRoute,
			Reason:  "service is listed twice",
		}
	case len(service.Routes) > MaxRoutesPerService:
		return &RuleError{
			Service: index,
			Route:   noRoute,
			Reason:  fmt.Sprintf("more than %d routes", MaxRoutesPerService),
		}
	}
	return nil
}

func (n *node) insert(route string) string {
	if reason := validateRoute(route); reason != "" {
		return reason
	}
	current := n
	if route != rootPath {
		for segment := range strings.SplitSeq(route[1:], "/") {
			current = current.child(segment)
		}
	}
	if current.template != "" {
		return fmt.Sprintf("duplicates %q", current.template)
	}
	current.template = route
	return ""
}

func (n *node) child(segment string) *node {
	if isParameter(segment) {
		if n.param == nil {
			n.param = &node{}
		}
		return n.param
	}
	if n.literals == nil {
		n.literals = make(map[string]*node)
	}
	if n.literals[segment] == nil {
		n.literals[segment] = &node{}
	}
	return n.literals[segment]
}

func (r *Rules) match(service resource.ServiceRef, items []string) (string, bool) {
	if r == nil {
		return "", false
	}
	root := r.services[service]
	if root == nil {
		return "", false
	}
	return root.match(items)
}

func (n *node) match(items []string) (string, bool) {
	if n == nil {
		return "", false
	}
	if len(items) == 0 {
		return n.template, n.template != ""
	}
	if template, ok := n.literals[items[0]].match(items[1:]); ok {
		return template, true
	}
	return n.param.match(items[1:])
}

// worstWalk bounds match statically: a path follows at most one literal child
// and then the parameter child at every node, so rejecting rules whose worst
// walk is too long keeps every match cheap without a runtime cut-off.
func (n *node) worstWalk() int {
	if n == nil {
		return 0
	}
	widest := 0
	for _, child := range n.literals {
		widest = max(widest, child.worstWalk())
	}
	return 1 + widest + n.param.worstWalk()
}
