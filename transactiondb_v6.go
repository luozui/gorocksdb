// +build !rocksdb_v5

package gorocksdb

import (
	"errors"
	"runtime"
	"unsafe"
)

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

// OpenTransactionDbColumnFamilies opens a database with the specified options.
func OpenTransactionDbColumnFamilies(
	opts *Options,
	transactionDBOpts *TransactionDBOptions,
	name string,
	cfNames []string,
	cfOpts []*Options,
) (*TransactionDB, []*ColumnFamilyHandle, error) {
	numColumnFamilies := len(cfNames)
	if numColumnFamilies != len(cfOpts) {
		return nil, nil, errors.New("must provide the same number of column family names and options")
	}
	cNames := make([]*C.char, numColumnFamilies)
	for i, s := range cfNames {
		cNames[i] = C.CString(s)
	}
	defer func() {
		for _, s := range cNames {
			C.free(unsafe.Pointer(s))
		}
	}()

	cOpts := make([]*C.rocksdb_options_t, numColumnFamilies)
	for i, o := range cfOpts {
		cOpts[i] = o.c
	}

	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))

	cHandles := make([]*C.rocksdb_column_family_handle_t, numColumnFamilies)

	db := C.rocksdb_transactiondb_open_column_families(
		opts.c,
		transactionDBOpts.c,
		cName,
		C.int(numColumnFamilies),
		&cNames[0],
		&cOpts[0],
		&cHandles[0],
		&cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfHandles := make([]*ColumnFamilyHandle, numColumnFamilies)
	for i, c := range cHandles {
		cfHandles[i] = NewNativeColumnFamilyHandle(c)
	}

	return &TransactionDB{
		name:              name,
		c:                 db,
		opts:              opts,
		transactionDBOpts: transactionDBOpts,
	}, cfHandles, nil
}

// Merge merges the data associated with the key with the actual data in the database.
func (db *TransactionDB) MergeCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_transactiondb_merge_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// NewIterator returns an Iterator over the database that uses the
// ReadOptions given and column family.
func (db *TransactionDB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamilyHandle) *Iterator {
	return NewNativeIterator(
		unsafe.Pointer(C.rocksdb_transactiondb_create_iterator_cf(db.c, opts.c, cf.c)))
}
