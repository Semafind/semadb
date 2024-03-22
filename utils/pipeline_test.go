package utils_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
)

func Test_TransformWithContext_NoInput(t *testing.T) {
	in := make(chan int)
	transformFn := func(a int) (string, bool, error) {
		return fmt.Sprintf("%d-%d", a, a), false, nil
	}
	// ---------------------------
	// No input data
	ctx, cancel := context.WithCancel(context.Background())
	_, errC := utils.TransformWithContext(ctx, in, transformFn)
	cancel()
	require.Error(t, <-errC)
}

func Test_TransformWithContext_NoReciever(t *testing.T) {
	in := make(chan int, 1)
	in <- 1
	transformFn := func(a int) (string, bool, error) {
		return fmt.Sprintf("%d-%d", a, a), false, nil
	}
	// ---------------------------
	// No reciever
	ctx, cancel := context.WithCancel(context.Background())
	_, errC := utils.TransformWithContext(ctx, in, transformFn)
	cancel()
	require.Error(t, <-errC)
	// ---------------------------
}

func Test_SinkWithContext_NoInput(t *testing.T) {
	in := make(chan int)
	sinkFn := func(a int) error {
		return nil
	}
	// ---------------------------
	// No input data
	ctx, cancel := context.WithCancel(context.Background())
	errC := utils.SinkWithContext(ctx, in, sinkFn)
	cancel()
	require.Error(t, <-errC)
}

func Test_MergeErrorsWithContext(t *testing.T) {
	errC1 := make(chan error, 1)
	errC2 := make(chan error, 1)
	errC1 <- fmt.Errorf("error 1")
	errC2 <- fmt.Errorf("error 2")
	// ---------------------------
	// Merge errors
	ctx, cancel := context.WithCancel(context.Background())
	errC := utils.MergeErrorsWithContext(ctx, errC1, errC2)
	cancel()
	require.Error(t, <-errC)
}
