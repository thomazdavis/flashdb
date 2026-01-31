package memtable

import (
	"bytes"
	"math/rand"
	"time"
)

const (
	MaxLevel    = 20 // Hold upto 2^20 items
	Probability = 0.5
)

type Node struct {
	Key   []byte
	Value []byte

	Next []*Node // Holds points to the next node at different levels
}

type SkipList struct {
	Head  *Node
	Level int // Current max level in the list
	Size  int
	rand  *rand.Rand // For randomness
}

func NewSkipList() *SkipList {
	return &SkipList{
		Head: &Node{
			Next: make([]*Node, MaxLevel),
		},
		Level: 1,
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Determines how many levels a new node will have
func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rand.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key, value []byte) {
	// Array to track the prev node at each level
	update := make([]*Node, MaxLevel)
	current := sl.Head

	// Search downwards from the highest level
	for i := sl.Level - 1; i >= 0; i-- {
		for current.Next[i] != nil && bytes.Compare(current.Next[i].Key, key) < 0 {
			current = current.Next[i]
		}
		update[i] = current
	}

	// Move to the next node at level 0 to check if key exists
	current = current.Next[0]
	if current != nil && string(current.Key) == string(key) {
		current.Value = value
		return
	}

	// If key doesn't exist, create a new node
	newLevel := sl.randomLevel()

	// If the new random level is higher than current list level,
	// initialize the "update" array for those new upper levels to Head
	if newLevel > sl.Level {
		for i := sl.Level; i < newLevel; i++ {
			update[i] = sl.Head
		}
		sl.Level = newLevel
	}

	// Create the new node
	newNode := &Node{
		Key:   key,
		Value: value,
		Next:  make([]*Node, newLevel),
	}

	// Link the new node into the list at every level it exists on
	for i := 0; i < newLevel; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	sl.Size++
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	current := sl.Head

	// Travel down from the highest level
	for i := sl.Level - 1; i >= 0; i-- {
		for current.Next[i] != nil && bytes.Compare(current.Next[i].Key, key) < 0 {
			current = current.Next[i]
		}
	}

	current = current.Next[0]

	if current != nil && bytes.Equal(current.Key, key) {
		return current.Value, true
	}

	return nil, false
}
