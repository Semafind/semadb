/*
This package helps organise an all or nothing pipeline. If an error occurs at
any point in the pipeline, we assume the entire operation should be cancelled.

The context is checked when reading or writing to a channel. If the context is
cancelled, the operation is stopped whether the channel is closed or not.

Two common use cases:
  - Synchronous pipeline: Use the functions directly and check for an error.
  - Asynchronous pipeline: Use the functions in a goroutine and check for an error
    in the goroutine.

Based on: https://go.dev/blog/pipelines
*/
package utils

import (
	"context"
)

func TransformWithContext[A, B any](ctx context.Context, in <-chan A, out chan<- B, transformFn func(A) (B, error)) error {
	for {
		select {
		// Is the context cancelled?
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-in:
			// Is the channel closed?
			if !ok {
				return nil
			}
			b, err := transformFn(a)
			if err != nil {
				return err
			}
			// Can we send? It may be the context is cancelled and there are
			// no receivers.
			select {
			case out <- b:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func SinkWithContext[T any](ctx context.Context, in <-chan T, sinkFn func(T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b, ok := <-in:
			if !ok {
				return nil
			}
			if err := sinkFn(b); err != nil {
				return err
			}
		}
	}
}
