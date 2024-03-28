package conversion_test

import (
	"testing"

	"github.com/semafind/semadb/conversion"
	"github.com/stretchr/testify/require"
)

func TestNodeKey(t *testing.T) {
	id := uint64(42)
	key := conversion.NodeKey(id, 'a')
	readId, ok := conversion.NodeIdFromKey(key, 'a')
	require.True(t, ok)
	require.Equal(t, id, readId)
	_, ok = conversion.NodeIdFromKey(key, 'b')
	require.False(t, ok)
}
