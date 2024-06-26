package simpledb

import (
	"context"
	"fmt"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/transaction"
)

type DB struct {
	fileMgr     *file.Manager
	bufMgr      *buffer.Manager
	logMgr      *log.Manager
	metadataMgr *metadata.Manager
	planner     *plan.Planner
}

func NewWithParams(ctx context.Context, dirname string, blockSize int32, bufSize int) (*DB, error) {
	fm, err := file.NewManager(dirname, blockSize)
	if err != nil {
		return nil, err
	}
	const logFileName = "simpledb.log"
	lm, err := log.NewManager(fm, logFileName)
	if err != nil {
		return nil, err
	}
	bm := buffer.NewManager(ctx, fm, lm, bufSize)
	return &DB{
		fileMgr: fm,
		bufMgr:  bm,
		logMgr:  lm,
	}, nil
}

func New(ctx context.Context, dirname string) (*DB, error) {
	db, err := NewWithParams(ctx, dirname, 400, 8)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)
	}
	tx, err := transaction.NewTransaction(ctx, db.bufMgr, db.fileMgr, db.logMgr)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)
	}
	isNew := db.fileMgr.IsNew()
	if !isNew {
		if err := tx.Recover(); err != nil {
			return nil, fmt.Errorf("could not recover: %w", err)
		}
	}
	metadataMgr, err := metadata.NewManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("could not create SimpleDB: %w", err)

	}
	db.metadataMgr = metadataMgr
	db.planner = plan.NewPlanner(
		plan.NewBasicQueryPlanner(metadataMgr),
		plan.NewBasicUpdatePlanner(metadataMgr),
	)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not recover: %w", err)
	}
	return db, nil
}

func (db DB) NewTx(ctx context.Context) (*transaction.Transaction, error) {
	return transaction.NewTransaction(ctx, db.bufMgr, db.fileMgr, db.logMgr)
}

func (db DB) MetadataMgr() *metadata.Manager {
	return db.metadataMgr
}

func (db DB) Planner() *plan.Planner {
	return db.planner
}
