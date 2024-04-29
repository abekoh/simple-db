package server

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/transaction"
)

type SimpleDB struct {
	fileMgr     *file.Manager
	bufMgr      *buffer.Manager
	logMgr      *log.Manager
	metadataMgr *metadata.Manager
}

func NewSimpleDBWithParams(dirname string, blockSize int32, bufSize int) (*SimpleDB, error) {
	fm, err := file.NewManager(dirname, blockSize)
	if err != nil {
		return nil, err
	}
	const logFileName = "simpledb.log"
	lm, err := log.NewManager(fm, logFileName)
	if err != nil {
		return nil, err
	}
	bm := buffer.NewManager(fm, lm, bufSize)
	return &SimpleDB{
		fileMgr: fm,
		bufMgr:  bm,
		logMgr:  lm,
	}, nil
}

func NewSimpleDB(dirname string) (*SimpleDB, error) {
	db, err := NewSimpleDBWithParams(dirname, 400, 8)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)
	}
	tx, err := transaction.NewTransaction(db.bufMgr, db.fileMgr, db.logMgr)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)
	}
	isNew := db.fileMgr.IsNew()
	if !isNew {
		// TODO: fix
		if err := tx.Rollback(); err != nil {
			return nil, fmt.Errorf("could not recover: %w", err)
		}
	}
	metadataMgr, err := metadata.NewManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)

	}
	db.metadataMgr = metadataMgr
	// TODO: setup planner
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not recover: %w", err)
	}
	return db, nil
}

func (db *SimpleDB) NewTx() (*transaction.Transaction, error) {
	return transaction.NewTransaction(db.bufMgr, db.fileMgr, db.logMgr)
}

func (db *SimpleDB) MetadataMgr() *metadata.Manager {
	return db.metadataMgr
}
