package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvDb := engine_util.CreateDB(conf.DBPath, false)
	s := &StandAloneStorage{
		kvDb,
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := &BaseStorageReader{txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := engine_util.WriteBatch{}
	wb.Reset()
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			wb.DeleteCF(modify.Cf(), modify.Key())
		}
	}
	return wb.WriteToDB(s.db)
}

type BaseStorageReader struct {
	txn *badger.Txn
}

func (rd *BaseStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := rd.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (rd *BaseStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, rd.txn)
}

func (rd *BaseStorageReader) Close() {
	rd.txn.Discard()
}
