package fileaccess

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrOutsideRoot = errors.New("path is outside trusted file roots")

type Policy struct {
	workspaceRoot    string
	baseTrustedRoots []string
	roots            []string
}

func NewPolicy(workspaceRoot string, trustedRoots []string) (Policy, error) {
	workspace, err := cleanRoot(workspaceRoot)
	if err != nil {
		return Policy{}, err
	}
	trusted, err := cleanTrustedRoots(trustedRoots)
	if err != nil {
		return Policy{}, err
	}
	return newPolicyFromCleanRoots(workspace, trusted, nil), nil
}

func (p Policy) WithClientTrustedRoots(clientRoots []string) (Policy, error) {
	clientTrusted, err := cleanTrustedRoots(clientRoots)
	if err != nil {
		return Policy{}, err
	}
	return newPolicyFromCleanRoots(p.workspaceRoot, p.baseTrustedRoots, clientTrusted), nil
}

func (p Policy) Roots() []string {
	return append([]string(nil), p.roots...)
}

func newPolicyFromCleanRoots(workspaceRoot string, baseTrustedRoots, clientTrustedRoots []string) Policy {
	trustedRoots := append([]string(nil), baseTrustedRoots...)
	trustedRoots = append(trustedRoots, clientTrustedRoots...)
	rawRoots := append([]string{workspaceRoot}, trustedRoots...)
	roots := make([]string, 0, len(rawRoots))
	seen := make(map[string]struct{}, len(rawRoots))
	for _, root := range rawRoots {
		if root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		roots = append(roots, root)
	}
	return Policy{
		workspaceRoot:    workspaceRoot,
		baseTrustedRoots: append([]string(nil), baseTrustedRoots...),
		roots:            roots,
	}
}

func cleanTrustedRoots(rawRoots []string) ([]string, error) {
	roots := make([]string, 0, len(rawRoots))
	seen := make(map[string]struct{}, len(rawRoots))
	for _, rawRoot := range rawRoots {
		if strings.TrimSpace(rawRoot) == "" {
			continue
		}
		root, err := cleanRoot(rawRoot)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		roots = append(roots, root)
	}
	return roots, nil
}

func (p Policy) Resolve(rawPath string) (string, error) {
	if len(p.roots) == 0 {
		return "", fmt.Errorf("trusted file roots are not configured")
	}
	target := strings.TrimSpace(rawPath)
	if target == "" {
		return p.roots[0], nil
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(p.roots[0], target)
	}
	absPath, err := filepath.Abs(filepath.Clean(target))
	if err != nil {
		return "", fmt.Errorf("invalid path: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", err
	}
	for _, root := range p.roots {
		if isWithinRoot(resolvedPath, root) {
			return resolvedPath, nil
		}
	}
	return "", ErrOutsideRoot
}

func cleanRoot(rawRoot string) (string, error) {
	root := strings.TrimSpace(rawRoot)
	if root == "" {
		root = "."
	}
	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", fmt.Errorf("invalid trusted file root %q: %w", rawRoot, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", fmt.Errorf("invalid trusted file root %q: %w", rawRoot, err)
	}
	info, err := os.Stat(resolvedRoot)
	if err != nil {
		return "", fmt.Errorf("invalid trusted file root %q: %w", rawRoot, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("trusted file root is not a directory: %s", resolvedRoot)
	}
	return resolvedRoot, nil
}

func isWithinRoot(path, root string) bool {
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
