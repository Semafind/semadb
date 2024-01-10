package shard

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
)

/* When we use internal integer ids (uint64), we can use more efficient data
 * structures compared to raw UUIDs when traversing the graph. For example, a
 * bitset can more efficiently check whether we visisted a node by using its
 * integer id. But if the maximum node id gets really large the bitset consumes
 * too much memory. By re-using node ids, we can at least be sure a shard of a
 * million maximum points during its lifetime can stay efficient. For example,
 * we add 1 million points, remove 500k, re-add 500k and so on, despite having a
 * total of 1 million points the node ids would increase. Since one of the major
 * reasons for using integer ids is efficiency, we optimise for the search case.
 * The other advantage of having internal integer ids separate from the UUID is
 * we can manipulate the internal graph without affecting the external user
 * facing mappings, for example if we wanted to re-index, partition etc.
 * */

// Used for storing the next free id and the list of free ids, actually the main
// goal is to ensure the node Ids don't spiral out of control after many
// deletions and insertions.
type IdCounter struct {
	bucket     *bbolt.Bucket
	freeIds    []uint64
	nextFreeId uint64
}

func NewIdCounter(bucket *bbolt.Bucket) (*IdCounter, error) {
	// ---------------------------
	freeIdsBytes := bucket.Get(FREEIDSKEY)
	freeIdsMap := make(map[uint64]struct{})
	if freeIdsBytes != nil {
		for i := 0; i < len(freeIdsBytes); i += 8 {
			freeId := bytesToUint64(freeIdsBytes[i : i+8])
			freeIdsMap[freeId] = struct{}{}
		}
	}
	freeIds := make([]uint64, 0, len(freeIdsMap))
	for freeId := range freeIdsMap {
		freeIds = append(freeIds, freeId)
	}
	// ---------------------------
	nextFreeId := uint64(1) // we start from 1 because 0 can be used for nil
	nextFreeIdBytes := bucket.Get(NEXTFREEIDKEY)
	if nextFreeIdBytes != nil {
		nextFreeId = bytesToUint64(nextFreeIdBytes)
	}
	// ---------------------------
	log.Debug().Uint64("nextFreeId", nextFreeId).Int("freeIds", len(freeIds)).Msg("NewIdCounter")
	idCounter := &IdCounter{
		bucket:     bucket,
		freeIds:    freeIds,
		nextFreeId: nextFreeId,
	}
	return idCounter, nil
}

func (ic *IdCounter) MaxId() uint64 {
	return ic.nextFreeId - 1
}

func (ic *IdCounter) NextId() uint64 {
	if len(ic.freeIds) == 0 {
		ic.nextFreeId++
		return ic.nextFreeId - 1
	}
	// ---------------------------
	freeId := ic.freeIds[0]
	ic.freeIds = ic.freeIds[1:]
	return freeId
}

func (ic *IdCounter) FreeId(id uint64) {
	ic.freeIds = append(ic.freeIds, id)
}

func (ic *IdCounter) Flush() error {
	if err := ic.bucket.Put(NEXTFREEIDKEY, uint64ToBytes(ic.nextFreeId)); err != nil {
		return fmt.Errorf("could not set next free id: %w", err)
	}
	// ---------------------------
	freeIdsBytes := make([]byte, len(ic.freeIds)*8)
	for i, freeId := range ic.freeIds {
		copy(freeIdsBytes[i*8:], uint64ToBytes(freeId))
	}
	if err := ic.bucket.Put(FREEIDSKEY, freeIdsBytes); err != nil {
		return fmt.Errorf("could not set free ids: %w", err)
	}
	// ---------------------------
	return nil
}
