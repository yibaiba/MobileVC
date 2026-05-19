package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

const (
	defaultHTTPPort  = "80"
	defaultHTTPSPort = "443"
)

func NormalizeOrigin(origin string) (string, error) {
	parsed, err := url.Parse(origin)
	if err != nil {
		return "", fmt.Errorf("invalid origin %q: %w", origin, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid origin %q: scheme must be http or https", origin)
	}
	if parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("invalid origin %q: expected scheme://host[:port]", origin)
	}
	return canonicalOrigin(parsed), nil
}

func ValidateRelayURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid relay url %q: %w", raw, err)
	}
	switch parsed.Scheme {
	case "wss":
		return validateRelayURLShape(parsed, raw)
	case "ws":
		return validateInsecureRelayURL(parsed, raw)
	default:
		return fmt.Errorf("relay url must use ws:// or wss://")
	}
}

func validateInsecureRelayURL(parsed *url.URL, raw string) error {
	if err := validateRelayURLShape(parsed, raw); err != nil {
		return err
	}
	if !isInsecureRelayHostAllowed(parsed.Hostname()) {
		return fmt.Errorf("ws:// relay urls are allowed only for loopback or LAN hosts")
	}
	return nil
}

func validateRelayURLShape(parsed *url.URL, raw string) error {
	if parsed.Host == "" || parsed.User != nil {
		return fmt.Errorf("invalid relay url %q: expected scheme://host[:port]", raw)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("invalid relay url %q: path is not allowed", raw)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("invalid relay url %q: query and fragment are not allowed", raw)
	}
	return nil
}

func isInsecureRelayHostAllowed(host string) bool {
	normalized := strings.ToLower(strings.Trim(host, "[]"))
	if normalized == "localhost" {
		return true
	}
	ip := net.ParseIP(normalized)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		strings.HasPrefix(strings.ToLower(ip.String()), "fc") ||
		strings.HasPrefix(strings.ToLower(ip.String()), "fd")
}

func canonicalOrigin(parsed *url.URL) string {
	scheme := strings.ToLower(parsed.Scheme)
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if port == "" || isDefaultOriginPort(scheme, port) {
		return scheme + "://" + formatOriginHost(host)
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}

func isDefaultOriginPort(scheme string, port string) bool {
	return (scheme == "http" && port == defaultHTTPPort) || (scheme == "https" && port == defaultHTTPSPort)
}

func formatOriginHost(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}
