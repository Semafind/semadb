package utils_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/assert"
)

func Test_TransformWithContext_NoInput(t *testing.T) {
	in := make(chan int)
	out := make(chan string)
	transformFn := func(a int) (string, error) {
		return fmt.Sprintf("%d-%d", a, a), nil
	}
	// ---------------------------
	// No input data
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := utils.TransformWithContext(ctx, in, out, transformFn)
		assert.Error(t, err)
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func Test_TransformWithContext_NoReciever(t *testing.T) {
	in := make(chan int, 1)
	in <- 1
	out := make(chan string)
	transformFn := func(a int) (string, error) {
		return fmt.Sprintf("%d-%d", a, a), nil
	}
	// ---------------------------
	// No reciever
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := utils.TransformWithContext(ctx, in, out, transformFn)
		assert.Error(t, err)
		wg.Done()
	}()
	cancel()
	wg.Wait()
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := utils.SinkWithContext(ctx, in, sinkFn)
		assert.Error(t, err)
		wg.Done()
	}()
	cancel()
	wg.Wait()
}
