package mycache

import (
	"context"
	"github.com/Rampage-cd/DistributedCache/store"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

//Cache是对底层缓存存储的封装
type Cache struct{
	mu 		sync.RWMutex
	store	store.Store		//Store是一个接口类型（lruCache和lru2Cache都实现了该接口）
	opts	CacheOptions	//缓存配置选项
	hits 	int64			//缓存命中和未命中的次数
	misses 	int64			
	initialized int32 		//原子变量，标记缓存是否已初始化
	closed 	int32 			//原子变量，标记缓存是否已关闭
}

//CacheOptions是缓存配置选项
type CacheOptions struct {
	CacheType 			store.CacheType			//缓存类型：LRU，LRU2等
	MaxBytes			int64 					//最大内存使用量
	BucketCount 		uint16 					//缓存桶数量
	CapPerBucket		uint16					//每个缓存桶的一级缓存容量
	Level2Cap			uint16					//二级缓存桶容量
	CleanupTime 		time.Duration			//清理间隔
	OnEvicted			func(key string,value store.Value)//驱逐回调函数
}

//返回默认的缓存配置
func DefaultCacheOptions() CacheOptions{
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024, // 8MB
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

//缓存实例的创建
func NewCache(opts CacheOptions) *Cache{
	return &Cache{
		opts:opts,
	}
}

//绑定方法(其实就是对store中的7个方法进行包装)
//1.确保缓存已经初始化
func (c *Cache) ensureInitialized() {
	if atomic.LoadInt32(&c.initialized) ==1{
		return 
	}//如果已经初始化，则直接返回

	c.mu.Lock()
	defer c.mu.Unlock()//否则，加锁进行初始化

	if c.initialized == 0{
		storeOpts := store.Options{
			MaxBytes:		c.opts.MaxBytes,
			BucketCount:	c.opts.BucketCount,
			CapPerBucket:	c.opts.CapPerBucket,
			Level2Cap:		c.opts.Level2Cap,
			CleanupInterval:c.opts.CleanupTime,
			OnEvicted:		c.opts.OnEvicted,
		}//排除CacheOptions中的CacheType

		//创建存储实例
		c.store = store.NewStore(c.opts.CacheType,storeOpts)

		//标记为已初始化
		atomic.StoreInt32(&c.initialized,1)

		logrus.Infof("Cache initialized with type %s,max bytes: %d",c.opts.CacheOptions,c.opts.MaxBytes)
	}
}

//2.向缓存中插入一个key-value对
func (c *Cache) Add(key string,value ByteView) {
	//确保缓存未关闭
	if atomic.LoadInt32(&c.closed) == 1{
		logrus.Warnf("Attempted to add to a closed cache: %s",key)
		return
	}

	//确保缓存已经初始化
	c.ensureInitialized()

	if err := c.store.Set(key, value); err != nil{//调用store的插入函数
		logrus.Warnf("Failed to add key %s to cache: %v",key,err)
	}
}

//3.Get从缓存中获取值
func (c *Cache) Get(ctx context.Context,key string) (value ByteView,ok bool){
	//检查缓存是否关闭（如果关闭，则无法操作）
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	// 如果缓存未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	//从底层存储获取
	val,found := c.store.Get(key)
	if !found{
		atomic.AddInt64(&c.misses,1)//获取失败
		return ByteView{},false
	}

	// 更新命中计数
	atomic.AddInt64(&c.hits, 1)//获取成功

	// 转换并返回
	if bv, ok := val.(ByteView); ok {
		return bv, true//val为Value接口类型，需要类型断言为具体的ByteView类型
	}

	// 类型断言失败
	logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

//4.带过期时间的插入操作
func (c *Cache) AddWithExpiration(key string,value ByteView,expirationTime time.Time){
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()//确保未关闭，确保已经初始化

	//计算过期时间
	expiration := time.Until(expirationTime)//time.Until(t)用于计算当前时间与t时间的差值，未来时间为正值，当前时间为负值
	if expiration <= 0{
		logrus.Debugf("Key %s already expired,not adding to cache",key)
		return
	}

	//设置到底层存储
	if err := c.store.SetWithExpiration(key,value,expiration); err != nil{
		logrus.Warnf("Failed to add key %s to cache with expiration: %v",key,err)
	}

}

//5.删除操作
func (c *Cache) Delete(key string) bool{
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Delete(key)
}

//6.清空操作
func (c *Cache) Clear(){
	if atomic.LoadInt32(&c.closed)==1 || atomic.LoadInt32(&c.initialized)==0{
		return 
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	//重置统计信息
	atomic.StoreInt64(&c.hits,0)
	atomic.StoreInt64J(&c.misses,0)
}

//7.返回当前缓存项的数量
func (c *Cache) Len() int{
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

//8.关闭缓存，释放资源
func (c *Cache) Close() {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {//比较closed是否为0，如果为0，则变为1，并返回true
		return	
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭底层存储
	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {//接口类型断言，用于检查c.store是否实现了Close()方法
			closer.Close()//转换成功，ok为true
		}
		c.store = nil
	}

	// 重置缓存状态
	atomic.StoreInt32(&c.initialized, 0)

	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

//9.返回缓存统计信息
func (c *Cache) Stats() map[string]interface{} {//值的类型为interface，意味着值可以是任何类型
	func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {//只有在已经初始化的情况下，才能统计缓存容量和命中率
		stats["size"] = c.Len()

		// 计算命中率
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}
}





