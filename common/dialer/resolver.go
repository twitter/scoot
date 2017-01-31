package dialer

import (
	"fmt"
	"os"
)

// Resolver resolves a service, getting an address or URL.
type Resolver interface {
	// Resolve resolves a service, getting an address or URL (or an error)
	Resolve() (string, error)
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

// CompositeResolves resolves by resolving, in order, via delegates
type CompositeResolver struct {
	dels []Resolver
}

// NewCompositeResolves creates a new CompositeResolve that resolves by looking through delegates (in order)
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
