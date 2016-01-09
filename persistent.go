package opic

import (
	"io/ioutil"
	"os"
)

// Persistent extends OPIC with a disk-based persistency mechanism.
type Persistent struct {
	*Serialisable

	filename string
}

// NewPersistent creates a new Persistent OPIC instance backed by a particular
// file.
func NewPersistent(filename string) *Persistent {
	return &Persistent{
		Serialisable: &Serialisable{OPIC: New()},
		filename:     filename,
	}
}

// Load does what it sounds like. It loads the OPIC state from the file
// associated with this instance.
func (p *Persistent) Load() error {
	f, err := os.OpenFile(p.filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = p.ReadFrom(f); err != nil {
		return err
	}

	p.dirty = false

	return nil
}

// Save does what it sounds like. It saves the OPIC state to the file
// associated with this instance.
func (p *Persistent) Save() error {
	o, err := ioutil.TempFile("", "opic")
	if err != nil {
		return err
	}
	defer o.Close()

	if _, err := p.WriteTo(o); err != nil {
		return err
	}

	if err := o.Close(); err != nil {
		return err
	}

	if err := os.Rename(o.Name(), p.filename); err != nil {
		return err
	}

	p.dirty = false

	return nil
}
