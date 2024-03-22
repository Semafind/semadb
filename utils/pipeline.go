/*
This package helps organise an all or nothing pipeline. If an error occurs at
any point in the pipeline, we assume the entire operation should be cancelled.

The context is checked when reading or writing to a channel. If the context is
cancelled, the operation is stopped whether the channel is closed or not.

Based on: https://go.dev/blog/pipelines
*/
package utils

import (
	"context"
	"sync"
)

func ProduceWithContext[T any](ctx context.Context, in []T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, t := range in {
			select {
			case out <- t:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func ProduceWithContextMapKeys[K comparable, V any](ctx context.Context, in map[K]V) <-chan K {
	out := make(chan K)
	go func() {
		defer close(out)
		for k := range in {
			select {
			case out <- k:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func TransformWithContext[A, B any](ctx context.Context, in <-chan A, transformFn func(A) (out B, skip bool, err error)) (<-chan B, <-chan error) {
	out := make(chan B)
	errC := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errC)
		for {
			select {
			// Is the context cancelled?
			case <-ctx.Done():
				errC <- ctx.Err()
				return
			case a, ok := <-in:
				// Is the channel closed?
				if !ok {
					errC <- nil
					return
				}
				b, skip, err := transformFn(a)
				if skip {
					continue
				}
				if err != nil {
					errC <- err
					return
				}
				// Can we send? It may be the context is cancelled and there are
				// no receivers.
				select {
				case out <- b:
				case <-ctx.Done():
					errC <- ctx.Err()
					return
				}
			}
		}
	}()
	return out, errC
}

func TransformWithContextMultiple[A, B any](ctx context.Context, in <-chan A, transformFn func(A) (out []B, err error)) (<-chan B, <-chan error) {
	out := make(chan B)
	errC := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errC)
		for {
			select {
			// Is the context cancelled?
			case <-ctx.Done():
				errC <- ctx.Err()
				return
			case a, ok := <-in:
				// Is the channel closed?
				if !ok {
					errC <- nil
					return
				}
				bs, err := transformFn(a)
				if err != nil {
					errC <- err
					return
				}
				for _, b := range bs {
					// Can we send? It may be the context is cancelled and there are
					// no receivers.
					select {
					case out <- b:
					case <-ctx.Done():
						errC <- ctx.Err()
						return
					}
				}
			}
		}
	}()
	return out, errC
}

func MergeWithContext[T any](ctx context.Context, cs ...<-chan T) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case t, ok := <-c:
					if !ok {
						return
					}
					select {
					case out <- t:
					case <-ctx.Done():
						return
					}
				}
			}
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func MergeErrorsWithContext(ctx context.Context, cs ...<-chan error) <-chan error {
	errC := make(chan error, 1)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancelCause(ctx)
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan error) {
			select {
			case <-ctx.Done():
				cancel(ctx.Err())
			case err := <-c:
				if err != nil {
					cancel(err)
				}
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		errC <- context.Cause(ctx)
		close(errC)
	}()
	return errC
}

func SinkWithContext[T any](ctx context.Context, in <-chan T, sinkFn func(T) error) <-chan error {
	errC := make(chan error, 1)
	go func() {
		defer close(errC)
		for {
			select {
			case <-ctx.Done():
				errC <- ctx.Err()
				return
			case b, ok := <-in:
				if !ok {
					errC <- nil
					return
				}
				if err := sinkFn(b); err != nil {
					errC <- err
					return
				}
			}
		}
	}()
	return errC
}
