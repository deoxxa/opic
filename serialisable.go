package opic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

var (
	expectedMagic = "#opicdb#"
)

// Serialisable extends OPIC with methods to serialise and deserialise a
// binary format representing the dataset.
type Serialisable struct {
	*OPIC
}

// ReadFrom implements io.ReaderFrom
func (s *Serialisable) ReadFrom(r io.Reader) (int64, error) {
	s.m.Lock()
	defer s.m.Unlock()

	n := int64(0)

	var magic [8]byte
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return n, err
	}
	n += 8

	if string(magic[:]) != expectedMagic {
		return n, fmt.Errorf("invalid magic")
	}

	var v uint64
	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return n, err
	}
	n += 8

	if v != 1 {
		return n, fmt.Errorf("invalid version; expected 0 but got %d", v)
	}

	var c uint64

	if err := binary.Read(r, binary.BigEndian, &c); err != nil {
		return n, err
	}
	n += 8

	for i := uint64(0); i < c; i++ {
		var e struct {
			K uint64
			V float64
		}

		if err := binary.Read(r, binary.BigEndian, &e); err != nil {
			return n, err
		}
		n += 16

		s.current[e.K] = e.V
	}

	if err := binary.Read(r, binary.BigEndian, &c); err != nil {
		return n, err
	}
	n += 8

	for i := uint64(0); i < c; i++ {
		var e struct {
			K uint64
			V float64
		}

		if err := binary.Read(r, binary.BigEndian, &e); err != nil {
			return n, err
		}
		n += 16

		s.history[e.K] = e.V
	}

	if err := binary.Read(r, binary.BigEndian, &c); err != nil {
		return n, err
	}
	n += 8

	for i := uint64(0); i < c; i++ {
		var e struct {
			K uint64
			V int64
		}

		if err := binary.Read(r, binary.BigEndian, &e); err != nil {
			return n, err
		}
		n += 16

		s.fetched[e.K] = time.Unix(e.V, 0)
	}

	return n, nil
}

// WriteTo implements io.WriterTo
func (s *Serialisable) WriteTo(w io.Writer) (int64, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	n := int64(0)

	nw, err := w.Write([]byte(expectedMagic))
	if err != nil {
		return n, err
	}
	n += int64(nw)

	if err := binary.Write(w, binary.BigEndian, uint64(1)); err != nil {
		return n, err
	}
	n += 8

	if err := binary.Write(w, binary.BigEndian, uint64(len(s.current))); err != nil {
		return n, err
	}
	n += 8

	for k, v := range s.current {
		if err := binary.Write(w, binary.BigEndian, k); err != nil {
			return n, err
		}
		n += 8

		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return n, err
		}
		n += 8
	}

	if err := binary.Write(w, binary.BigEndian, uint64(len(s.history))); err != nil {
		return n, err
	}
	n += 8

	for k, v := range s.history {
		if err := binary.Write(w, binary.BigEndian, k); err != nil {
			return n, err
		}
		n += 8

		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return n, err
		}
		n += 8
	}

	if err := binary.Write(w, binary.BigEndian, uint64(len(s.fetched))); err != nil {
		return n, err
	}
	n += 8

	for k, v := range s.fetched {
		if err := binary.Write(w, binary.BigEndian, k); err != nil {
			return n, err
		}
		n += 8

		if err := binary.Write(w, binary.BigEndian, v.Unix()); err != nil {
			return n, err
		}
		n += 8
	}

	return n, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (s *Serialisable) UnmarshalBinary(d []byte) error {
	b := bytes.NewBuffer(d)

	if _, err := s.ReadFrom(b); err != nil {
		return err
	}

	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler
func (s *Serialisable) MarshalBinary() ([]byte, error) {
	b := bytes.NewBuffer(nil)

	if _, err := s.WriteTo(b); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
