package syscall

import (
	"os"
	"syscall"
	"unsafe"
)

func mmap(fd, size int) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)
	h, errno := syscall.CreateFileMapping(syscall.Handle(fd), nil, syscall.PAGE_READONLY, high, low, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return (*[maxMapSize]byte)(unsafe.Pointer(addr))[:size], nil
}

func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(&data[0]))
	if errno := syscall.UnmapViewOfFile(addr); errno != nil {
		return os.NewSyscallError("UnmapViewOfFile", errno)
	}
	return nil
}
