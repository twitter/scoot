/*
ice is a lightweight Dependency Injection Framework

ice's cenral metaphor is a "Magic Bag".

It's a Bag because you put things in and then take things out.

Imagine a bag where you put in building materials and an Ikea instruction manual,
and then you pull out a fully-formed desk. The bag did the assembly! Magic!

ice works not with Noric flat pack furniture but with Go.
Our object graph is composed of Go values (the nodes) and Providers (the edges).

Lifecycle

1) Create an Empty Bag
2) Insert Provider Functions
  a) or a Module, which can insert many Provider at once
3) Extract Values

Terms

Key: what ice knows how to create. Currently a Go type, but this may be extended.

Provider: a function that creates a foo. It may either return foo or (foo, error).

Magic Bag: binds Keys to Providers.

Extract: Use the bindings in a Magic Bag to create and wire together complex structs

Module: utility to install multiple Providers at once

Notes

ice uses reflection heavily.

(MagicBag in ice is the equivalent of Guice's Injector or
Dagger 1's ObjectGraph)
*/
package ice
