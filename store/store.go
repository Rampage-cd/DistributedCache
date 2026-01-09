package store

import "time"

type Value interface{
	Len() int
}//只要实现了Len()方法，任何数据类型都可以作为Value接口

type Store interface{
	Get(key string) (Value, bool)
	Set(key string, value Value) error
	SetWithExpiration(key string,value Value,expiration time.Duration) error
	Delete(key string) bool
	Clear()
	Len() int
	Close()
}//lruCache和lru2Store都实现了该接口

type CacheType string
//新类型定义

const (
	LRU CacheType = "lru"
	LRU2 CacheType = "lru2"
)//缓存类型选择

//Options为通用缓存配置选项
type Options struct{
	MaxBytes int64 			//最大缓存字节数（lru）
	BucketCount uint16		//缓存桶的数量（lru-2）
	CapPerBucket uint16		//每个桶一级缓存的容量（lru-2）
	Level2Cap uint16		//lru-2中二级缓存的容量
	CleanupInterval time.Duration
	OnEvicted func(key string,value Value)
}

func NewOptions() Options{
	return Options{
		MaxBytes: 8192,
		BucketCount: 16,
		CapPerBucket: 512,
		Level2Cap: 256,
		CleanupInterval: time.Minute,
		OnEvicted: nil,
	}
}//配置初始化（默认配置）

func NewStore(cacheType CacheType,opts Options) Store{
	switch cacheType{
	case LRU2:
		return newLRU2Cache(opts)
	case LRU:
		return newLRUCache(opts)
	default:
		return newLRUCache(opts)
	}
}//根据选择缓存类型的不同，执行不同的初始化函数