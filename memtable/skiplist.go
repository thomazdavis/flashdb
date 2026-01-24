package memtable

import (
	"math/rand"
	"time"
)

const (
	MaxLevel    = 20 // Hold upto 2^12 items
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
