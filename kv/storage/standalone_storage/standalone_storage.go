package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{db: engine_util.CreateDB(conf.DBPath, false)}

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
	return &BadgerReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// updates := make(map[string]string)
	txn := s.db.NewTransaction(true)
	log.Debug("Write with batch size: ", len(batch))
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			log.Debug("Put CF: ", v.Cf(), " key: ", v.Key(), " value: ", v.Value())
			if err := txn.Set(engine_util.KeyWithCF(v.Cf(), v.Key()), v.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(v.Cf(), v.Key())); err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}

type BadgerReader struct {
	txn *badger.Txn
}

func (s *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		log.Debug("Get CF: ", cf, " key: ", key, " not found")
		return nil, nil
	}
	log.Debug("Get CF: ", cf, " key: ", key, " value: ", val)
	return val, nil
}

func (s *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *BadgerReader) Close() {
	s.txn.Discard()
}
