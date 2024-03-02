package util

type Nullable[T any] struct {
	Value T
	Valid bool
}

func NewNull[T any]() Nullable[T] {
	return Nullable[T]{Valid: false}
}

func NewNotNull[T any](v T) Nullable[T] {
	return Nullable[T]{Value: v, Valid: true}
}
