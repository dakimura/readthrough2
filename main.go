package readthrough2

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"

	"github.com/bradfitz/gomemcache/memcache"
	memcacheWithTag "github.com/dakimura/memctag/memcache"
)

type ReadThroughCache struct {
	cli *memcacheWithTag.Client
}

func NewReadThroughCache(servers ...string) *ReadThroughCache {
	cli := memcacheWithTag.NewClient(servers...)
	return &ReadThroughCache{cli: &cli}
}

func (c *ReadThroughCache) ReadThrough(key string, tags []string, valuePtr interface{}, readFromSource func() (valuePtr interface{}, err error)) error {
	if len(tags) == 0 {
		return errors.New("empty tag")
	}
	if valuePtr == nil {
		return errors.New("nil value pointer")
	}
	// read from cache
	var (
		item *memcache.Item
		err  error
	)

	item, err = c.cli.GetWithTags(key, tags)
	var (
		sourceDataPtr interface{}
		err2          error
	)
	if err != nil {
		// cache miss
		if errors.Is(err, memcache.ErrCacheMiss) {
			// read from the source of truth
			sourceDataPtr, err2 = readFromSource()
			if err2 != nil {
				return fmt.Errorf("get data from source(key=%s, tags=%v): %w", key, tags, err2)
			}
			// set cache (todo: implement an option to make it asynchronous)
			cacheBytes, err2 := encode(sourceDataPtr)
			if err2 != nil {
				return fmt.Errorf("encode value to bytes using gob. check your value struct: %w", err2)
			}
			cacheItem := &memcache.Item{
				Key:   key,
				Value: cacheBytes,
			}
			err2 = c.cli.SetWithTags(cacheItem, tags)
			if err2 != nil {
				return fmt.Errorf("failed to set cache(key=%s, tags=%v): %w", key, tags, err2)
			}

			// set to valuePtr
			val := reflect.ValueOf(valuePtr)
			if val.Kind() != reflect.Ptr {
				return errors.New("valPtr must be a pointer")
			}
			val2 := reflect.ValueOf(sourceDataPtr)
			if val2.Kind() != reflect.Ptr {
				return errors.New("sourceDataPtr must be a pointer")
			}

			val.Elem().Set(val2.Elem())

			return nil
		} else {
			return fmt.Errorf("failed to get cahce(key=%s, tags=%v): %w", key, tags, err)
		}
	}
	// cache hit
	err = decode(item.Value, valuePtr)
	if err != nil {
		return fmt.Errorf("decode cache(key=%s, tag=%v): %w", key, tags, err)
	}
	return nil
}

func encode(valuePtr interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(valuePtr)
	return buf.Bytes(), err
}

func decode(data []byte, hPtr interface{}) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(hPtr)
}

func printCache(item *memcache.Item, err error) {
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(string(item.Value))
	}
}

// Purge deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *ReadThroughCache) Purge(keyOrTag string) error {
	return c.cli.Delete(keyOrTag)
}
