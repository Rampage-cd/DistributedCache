package store

import(
	"container/list"
	"sync"
	"time"
)

type lruCache struct{
	mu sync.RWMutex
	list *List.list
	items map[string]*list.Element
	expires map[string]time.Time	//存储的过期时间是一个时刻
	maxBytes int64
	usedBytes int64

	onEvicted func(key string,value Value)
	cleanupInterval time.Duration	//存储的是过期间隔
	cleanupTicker *time.Ticker
	closeCh chan struct{}
}

type lruEntry struct{
	key string
	value Value
}//表示缓存表中的一个条目

//新建实例函数（lruCache）
func newLRUCache(opts Options) *lruCache{
	//设置默认清理间隔时间（如果没有对应传入参数的话）
	cleanupInterval := opts.cleanupInterval
	if cleanupInterval<=0{
		cleanupInterval = time.Minute
	}

	c:= &lruCache{
		list:	list.New(),
		items:	make(map[string]*list.Element),
		expires:	make(map[string]time.Time),
		maxBytes:	opts.maxBytes,

		onEvicted:	opts.onEvicted,
		cleanupInterval:	cleanupInterval,
		closeCh:	make(chan struct{}),
	}

	//启动定期清理协程（创建实例的时候就启动）（同时也不影响主线程）
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

//绑定方法1：查找操作
func (c *lruCache) Get(key string) (Value,bool){
	c.mu.RLock()//加入读锁，方便查找操作并发
	
	//1.检查是否存在
	elem,ok := c.items[key]
	if !ok{
		c.mu.RUnlock()
		return nil,false
	}

	//2.检查是否过期
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime){
		c.mu.RUnlock()

		//异步进行删除过期项，同时避免在读锁内操作（避免死锁）
		go c.Delete()

		return nil,false
	}

	//3.获取值并释放锁
	entry := elem.Value.(*lruEntry)
	value := entry.value
	c.mu.RUnlock()

	//4.更新在双向链表中的位置
	c.mu.Lock()
	if _,ok := c.items[key]; ok{
		c.list.MoveToBack(elem)//以Back作为链表的头
	}//再次检查是为了防止获取到写锁之前，该节点已被异步删除

	return value,false
}

//绑定方法2：添加或更新缓存项
func (c*lruCache) Set(key string,value Value) error{
	return c.SetWithExpiration(key,value,0)
}

//绑定方法3：添加或更新缓存项，并设置过期时间
func (c *lruCache) SetWithExpiration(key string,value Value,expiration time.Duration) error{
	//1.传入的值为空的话，意味着该键失效（需要删除）
	if value == nil{
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	//2.计算并更新过期时间
	var expTime time.Time
	if expiration > 0{
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	}else{
		delete(c.expires,key)
	}

	//3.如果键已存在，则更新值and缓存容量
	if elem,ok := c.items[key]; ok{
		oldEntry := elem.Value.(*lruEntry)

		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value

		c.list.MoveToBack(elem)
		return nil
	}

	//4.不存在的话，则添加新项(哈希表新增，缓存容量增加)
	entry := &lruEntry{key:key,value:value}
	elem := c.list.PushBack(entry)
	//链表的节点中存放的是前后指针和value接口，还有其所属的链表，没有key
	//PushBack传入value，返回新增的节点，操作是将该节点加入链表的Back
	c.items[key] = elem
	c.usedBytes += int64(len(key)+value.Len())

	//5.检查是否需要淘汰旧项
	c.evict()

	return nil
}//方法2是默认添加方式（过期时间默认为0），方法3则是更精确的添加方式（需要手动传入过期时间）
//过期时间为0，表示永不过期

//绑定方法4：删除操作
func (c *lruCache) Delete(key string) bool{
	c.mu.Lock()
	defer c.mu.Unlock()

	//存在就成功删除，否则失败
	if elem,ok := c.items[key]; ok{
		c.removeElement(elem)
		return true
	}
	return false
}

//绑定方法5：清空缓存
func (c *lruCache) Clear(){
	c.mu.Lock()
	defer c.mu.Unlock()

	//如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil{
		for _,elem := range c.items{
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key,entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0//链表哈希表缓存值均清空
}

//绑定方法6：返回缓存中的项数
func (c *lruCache) Len() int{
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

//绑定方法7：从缓存中删除元素（方法4调用，调用前已加锁）
func (c *lruCache) removeElement(elem *list.Element){
	//所有操作需要考虑的四个量：items，expires，usedBytes，list
	entry := elem.Value.(*lruEntry)

	c.list.Delete(elem)
	delete(c.items,entry.key)
	delete(c.expires,entry.key)
	c.usedBytes -= int64(len(entry.key) + len(entry.value))

	if c.onEvicted != nil{
		c.onEvicted(entry.key,entry,value)
	}
	//删除和清空操作需要考虑是否有回调
}

//绑定方法8：清理过期和超出内存限制的缓存（方法2，3，9调用，调用前已加锁）
func (c *lruCache) evict(){
	//1.清理过期项
	now := time.Now()
	for key,expTime := range c.expirs{
		if now.After(expTime){
			if elem,ok := c.items[key]; ok{
				c.removeElement(elem)
			}
		}
	}

	//2.根据lru算法清理最近最少使用的项（Front）
	for c.maxBytes>0 && c.usedBytes>c.maxBytes && c.list.Len()>0{
		elem := c.list.Front()
		if elem != nil{
			c.removeElement(elem)
		}
	}
}//区别定时器清理（每个一个时间间隔清理一次），该方法是进行添加或更改操作后，防止缓存溢出从而主动进行清理

//绑定方法9：定期清理过期缓存的协程（创建lruCache实例时调用）
func (c *lruCache) cleanupLoop() {
	for{
		select{
		case <-c.cleanupTicker.C://只读通道，每隔一定时间，向通道中发送一个数据，触发清理逻辑
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh://空结构体通道，不占用内存，仅用于传递结束“信号”
			return 
		}//内层select用于让函数同时监听两个信号源，不会阻塞在某一个信号上（通道多路复用）
	}//外层是一个无限循环，用与持续监听“定时清理信号”和“关闭信号”
}

//绑定方法10：关闭缓存（停止清理协程）
func (c *lruCache) Close(){
	if c.cleanupTicker != nil{
		c.cleanupTicker.Stop()
		close(c.closeCh)//close()为go全局内置函数，专门用来关闭通道
	}
}

//绑定方法11：获取缓存项及其剩余过期时间
func (c *lruCache) GetWithExpiration(key string) (Value,time.Duration,bool){
	c.mu.RLock()
	defer c.mu.RUnlock()

	//1.获取缓存项
	elem,ok := c.items[key]
	if !ok{
		return nil,0,false
	}

	//2.检查过期时间
	now := time.Now()
	if expTime,hasExp := c.expires[key]; hasExp{
		if now.After(expTime){
			return nil,0,false
		}//已过期

		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return  elem.Value.(*lruEntry).value,ttl,true
	}

	//3.过期时间不存在则表明永不过期
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value,0,true
}

//绑定方法12：获取键的过期时间
func (c *lruCache) GetExpiration(key string) (time.Time,bool){
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime,ok := c.expires[key]
	return expTime,ok
}

//绑定方法13：更新过期时间
func (c *lruCache) UpdateExpiration(key string,expiration time.Duration) bool{
	c.mu.Lock()
	defer c.mu.Unlock()

	//1.缓存项是否存在
	if _,ok := c.items[key]; !ok{
		return false
	}

	//2.过期时间间隔是否为0
	if expiration > 0{
		c.expires[key] = time.Now().Add(expiration)
	}else{
		delete(c.expires,key)
	}

	return true
}

//绑定方法14：返回当前使用的字节数
func (c *lruCache) UsedBytes() int64{
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

//绑定方法15：返回最大允许字节数
func (c *lruCache) MaxBytes() int64{
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

//绑定方法16：设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64){
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0{
		c.evict()
	}
}