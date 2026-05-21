package relay

import (
	"net/http"
	"strings"
)

func (s *Server) HTTPRouteAllowed(method string, path string) bool {
	return routeAllowed(s.cfg.HTTPAllowedRoutes, method, path)
}

func (s *Server) WSRouteAllowed(method string, path string) bool {
	return routeAllowed(s.cfg.WSAllowedRoutes, method, path)
}

func routeAllowed(rules []RouteRule, method string, path string) bool {
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	normalizedPath := cleanRoutePath(path)
	for _, rule := range rules {
		if rule.Method == normalizedMethod && rule.Path == normalizedPath {
			return true
		}
	}
	return false
}

func cleanRoutePath(path string) string {
	normalized := strings.TrimSpace(path)
	if normalized == "" {
		return "/"
	}
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	if normalized != "/" {
		normalized = strings.TrimRight(normalized, "/")
	}
	return normalized
}

func methodPathFromRequest(r *http.Request) (string, string) {
	if r == nil || r.URL == nil {
		return "", ""
	}
	return r.Method, r.URL.Path
}
