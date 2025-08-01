package embedtsdb

import (
	"encoding/binary"
	"io"
)

// bstream is a stream of bits.
type bstream struct {
	stream []byte // the data stream
	count  uint8  // how many bits are valid in current byte
}

func (b *bstream) bytes() []byte {
	return b.stream
}

// reset resets the buffer to be empty,
// but it retains the underlying storage for use by future writes.
func (b *bstream) reset() {
	b.stream = b.stream[:0]
	b.count = 0
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

type bstreamReader struct {
	stream       []byte
	streamOffset int // The offset from which read the next byte from the stream.

	buffer uint64 // The current buffer, filled from the stream, containing up to 8 bytes from which read bits.
	valid  uint8  // The number of bits valid to read (from left) in the current buffer.
}

func newBReader(b []byte) bstreamReader {
	return bstreamReader{
		stream: b,
	}
}

func (b *bstreamReader) readBit() (bit, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(1) {
			return false, io.EOF
		}
	}

	return b.readBitFast()
}

// readBitFast is like readBit but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBit().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitFast() (bit, error) {
	if b.valid == 0 {
		return false, io.EOF
	}

	b.valid--
	bitmask := uint64(1) << b.valid
	return (b.buffer & bitmask) != 0, nil
}

func (b *bstreamReader) readBits(nbits uint8) (uint64, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}

	if nbits <= b.valid {
		return b.readBitsFast(nbits)
	}

	// We have to read all remaining valid bits from the current buffer and a part from the next one.
	bitmask := (uint64(1) << b.valid) - 1
	nbits -= b.valid
	v := (b.buffer & bitmask) << nbits
	b.valid = 0

	if !b.loadNextBuffer(nbits) {
		return 0, io.EOF
	}

	bitmask = (uint64(1) << nbits) - 1
	v = v | ((b.buffer >> (b.valid - nbits)) & bitmask)
	b.valid -= nbits

	return v, nil
}

// readBitsFast is like readBits but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBits().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > b.valid {
		return 0, io.EOF
	}

	bitmask := (uint64(1) << nbits) - 1
	b.valid -= nbits

	return (b.buffer >> b.valid) & bitmask, nil
}

func (b *bstreamReader) ReadByte() (byte, error) {
	v, err := b.readBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// loadNextBuffer loads the next bytes from the stream into the internal buffer.
// The input nbits is the minimum number of bits that must be read, but the implementation
// can read more (if possible) to improve performances.
func (b *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if b.streamOffset >= len(b.stream) {
		return false
	}

	// Handle the case there are more then 8 bytes in the buffer (most common case)
	// in a optimized way. It's guaranteed that this branch will never read from the
	// very last byte of the stream (which suffers race conditions due to concurrent
	// writes).
	if b.streamOffset+8 < len(b.stream) {
		b.buffer = binary.BigEndian.Uint64(b.stream[b.streamOffset:])
		b.streamOffset += 8
		b.valid = 64
		return true
	}

	// We're here if the are 8 or less bytes left in the stream. Since this reader needs
	// to handle race conditions with concurrent writes happening on the very last byte
	// we make sure to never over more than the minimum requested bits (rounded up to
	// the next byte). The following code is slower but called less frequently.
	nbytes := int((nbits / 8) + 1)
	if b.streamOffset+nbytes > len(b.stream) {
		nbytes = len(b.stream) - b.streamOffset
	}

	buffer := uint64(0)
	for i := 0; i < nbytes; i++ {
		buffer = buffer | (uint64(b.stream[b.streamOffset+i]) << uint(8*(nbytes-i-1)))
	}

	b.buffer = buffer
	b.streamOffset += nbytes
	b.valid = uint8(nbytes * 8)

	return true
}
