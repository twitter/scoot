package dialer

import (
	"fmt"
	"os"
)

// Resolver resolves a service, getting an address or URL.
type Resolver interface {
	// Resolve resolves a service, getting an address or URL
	// If a Resolve() call completes successfully but finds no addresses, it will return ("", nil)
	Resolve() (string, error)
	// ResolveAll resolves a slice of all known addresses or URLs
	// If a ResolveAll() call completes successfully but finds no addresses, it will return ([]string{}, nil)
	ResolveAll() ([]string, error)
}

// ConstantResolver always returns the same value
type ConstantResolver struct {
	s string
}

// NewConstantResolver creates a ConstantResolver
func NewConstantResolver(s string) *ConstantResolver {
	return &ConstantResolver{s: s}
}

// Resolve returns the constant
func (r *ConstantResolver) Resolve() (string, error) {
	return r.s, nil
}

// ResolveAll returns the constant in a slice
func (r *ConstantResolver) ResolveAll() ([]string, error) {
	all := []string{}
	if r.s != "" {
		all = append(all, r.s)
	}
	return all, nil
}

// EnvResolver resolves by looking for a key in the OS Environment
type EnvResolver struct {
	key string
}

// NewEnvResolver creates a new EnvResolver
func NewEnvResolver(key string) *EnvResolver {
	return &EnvResolver{key: key}
}

// Resolve resolves by looking for a key in the OS Environment
func (r *EnvResolver) Resolve() (string, error) {
	return os.Getenv(r.key), nil
}

// ResolveAll retuns the env key in a slice
func (r *EnvResolver) ResolveAll() ([]string, error) {
	all := []string{}
	s, err := r.Resolve()
	if s != "" {
		all = append(all, s)
	}
	return all, err
}

// CompositeResolves resolves by resolving, in order, via delegates
type CompositeResolver struct {
	dels []Resolver
}

// NewCompositeResolves creates a new CompositeResolve that resolves by looking through delegates (in order)
// A Resolver that returns ("", nil) is ignored, otherwise its result is retuned
// CompositeResolver will error if no delegate returned a non-empty result
func NewCompositeResolver(dels ...Resolver) *CompositeResolver {
	return &CompositeResolver{dels: dels}
}

// Resolve resolves by resolving, in order, via delegates
func (r *CompositeResolver) Resolve() (string, error) {
	for _, r := range r.dels {
		if s, err := r.Resolve(); s != "" || err != nil {
			return s, err
		}
	}
	return "", fmt.Errorf("could not resolve: no delegate resolved: %v", r.dels)
}

// ResolveAll resolves by resolving, in order, via delegates
func (r *CompositeResolver) ResolveAll() ([]string, error) {
	for _, r := range r.dels {
		if s, err := r.ResolveAll(); len(s) != 0 || err != nil {
			return s, err
		}
	}
	return []string{}, fmt.Errorf("could not resolve: no delegate resolved: %v", r.dels)
}
