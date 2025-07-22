package embedtsdb

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// partitionList represents a linked list for partitions.
// Each partition is arranged in order order of newest to oldest.
// That is, the head node is always the newest, the tail node is the oldest.
//
// Head and its next partitions must be writable to accept out-of-order data points
// even if it's inactive.
type partitionList interface {
	// insert appends a new node to the head.
	insert(partition partition)
	// remove eliminates the given partition from the list.
	remove(partition partition) error
	// swap replaces the old partition with the new one.
	swap(old, new partition) error
	// getHead gives back the head node which is the newest one.
	getHead() partition
	// size returns the number of partitions of itself.
	size() int
	// newIterator gives back the iterator object fot this list.
	// If you need to inspect all nodes within the list, use this one.
	newIterator() partitionIterator

	String() string
}

// Iterator represents an iterator for partition list. The basic usage is:
/*
  for iterator.next() {
    partition, err := iterator.value()
    // Do something with partition
  }
*/
type partitionIterator interface {
	// next positions the iterator at the next node in the list.
	// It will be positioned at the head on the first call.
	// The return value will be true if a value can be read from the list.
	next() bool
	// value gives back the current partition in the iterator.
	// If it was called even though next() returns false, it will return nil.
	value() partition

	currentNode() *partitionNode
}

type partitionListImpl struct {
	numPartitions int64
	head          *partitionNode
	tail          *partitionNode
	mu            sync.RWMutex
	// Cache for fast head access (atomic pointer)
	headCache unsafe.Pointer
}

func newPartitionList() partitionList {
	return &partitionListImpl{}
}

func (p *partitionListImpl) getHead() partition {
	if p.size() <= 0 {
		return nil
	}

	// Fast path: try atomic read first
	if headNode := (*partitionNode)(atomic.LoadPointer(&p.headCache)); headNode != nil {
		return headNode.value()
	}

	// Slow path: acquire lock and update cache
	p.mu.RLock()
	head := p.head
	if head != nil {
		atomic.StorePointer(&p.headCache, unsafe.Pointer(head))
	}
	p.mu.RUnlock()

	if head != nil {
		return head.value()
	}
	return nil
}

func (p *partitionListImpl) insert(partition partition) {
	node := &partitionNode{
		val: partition,
	}
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	if head != nil {
		node.next = head
	}

	p.setHead(node)
	atomic.AddInt64(&p.numPartitions, 1)
}

func (p *partitionListImpl) remove(target partition) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), target) {
			prev = current
			continue
		}

		// remove the current node.

		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// removing the head node
			p.setHead(next)
		case next == nil:
			// removing the tail node
			prev.setNext(nil)
			p.setTail(prev)
			// Invalidate head cache if removing something that might affect it
			atomic.StorePointer(&p.headCache, nil)
		default:
			// removing the middle node
			prev.setNext(next)
		}
		atomic.AddInt64(&p.numPartitions, -1)

		if err := current.value().clean(); err != nil {
			return fmt.Errorf("failed to clean resources managed by partition to be removed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func (p *partitionListImpl) swap(old, new partition) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), old) {
			prev = current
			continue
		}

		// swap the current node.

		newNode := &partitionNode{
			val:  new,
			next: current.getNext(),
		}
		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// swapping the head node
			p.setHead(newNode)
		case next == nil:
			// swapping the tail node
			prev.setNext(newNode)
			p.setTail(newNode)
		default:
			// swapping the middle node
			prev.setNext(newNode)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func samePartitions(x, y partition) bool {
	return x.minTimestamp() == y.minTimestamp()
}

func (p *partitionListImpl) size() int {
	return int(atomic.LoadInt64(&p.numPartitions))
}

func (p *partitionListImpl) newIterator() partitionIterator {
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	// Put a dummy node so that it positions the head on the first next() call.
	dummy := &partitionNode{
		next: head,
	}
	return &partitionIteratorImpl{
		current: dummy,
	}
}

func (p *partitionListImpl) setHead(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = node
	// Update cache atomically
	atomic.StorePointer(&p.headCache, unsafe.Pointer(node))
}

func (p *partitionListImpl) setTail(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tail = node
}

func (p *partitionListImpl) String() string {
	b := stringBuilderPool.Get().(*strings.Builder)
	b.Reset() // Clear any previous content
	defer stringBuilderPool.Put(b)

	iterator := p.newIterator()
	for iterator.next() {
		part := iterator.value()
		if _, ok := part.(*memoryPartition); ok {
			b.WriteString("[Memory Partition]")
		} else if _, ok := part.(*diskPartition); ok {
			b.WriteString("[Disk Partition]")
		} else {
			b.WriteString("[Unknown Partition]")
		}
		b.WriteString("->")
	}
	result := b.String()
	if len(result) > 2 {
		return result[:len(result)-2] // Remove trailing "->"
	}
	return result
}

// partitionNode wraps a partition to hold the pointer to the next one.
type partitionNode struct {
	// val is immutable
	val  partition
	next *partitionNode
	mu   sync.RWMutex
}

// value gives back the actual partition of the node.
func (p *partitionNode) value() partition {
	return p.val
}

func (p *partitionNode) setNext(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = node
}

func (p *partitionNode) getNext() *partitionNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.next
}

type partitionIteratorImpl struct {
	current *partitionNode
}

func (i *partitionIteratorImpl) next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *partitionIteratorImpl) value() partition {
	if i.current == nil {
		return nil
	}
	return i.current.value()
}

func (i *partitionIteratorImpl) currentNode() *partitionNode {
	return i.current
}
