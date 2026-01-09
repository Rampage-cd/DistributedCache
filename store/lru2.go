package store

import(
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)
//lru-2算法是lru-k算法的特例：只有当某个数据被访问至少两次之后，才会被缓存
//优点是：比传统lru更能抵抗缓存污染，适用于访问模式中存在临时热点的情况

//实现方式：1.首次访问的数据



type lru2Store struct{
	locks []sync.Mutex						//每个桶的独立锁
	caches [][2]*cache						//每个桶包含两级缓存
	onEvicted func(key string,value Value)	//驱逐回调函数
	cleanupTick *time.Ticker				//定期清理定时器
	mask int32								//用于哈希取模的掩码（使桶的数量为2的幂，便于使用位运算快速定位）
}//实现了1.分桶并发控制2.两级缓存架构3.数据驱逐时回调

type cache struct{
	dlnk [][2]uint16		//dlnk模拟双向链表，dlnk[0]是哨兵节点，dlnk[][p]存储尾部索引，dlnk[][n]存储头部索引
	m []node				//表示单个节点信息的集合，m[0]对应dlnk[1]（注意下标关系）
	hmap map[string]uint16	//键到节点索引的映射
	last uint16				//最后一个节点元素的索引(用于添加新元素)
	//uint16范围是0-65535
}//实现了1.用二维数组模拟双向链表2.预分配内存池（m）3.快速查找映射4.内存管理（用last来管理已分配的节点）

type node struct{
	k string
	v Value
	expireAt int64//过期时间戳，0表示已删除
}

var clock,p,n = time.Now().UnixNano(),uint16(0),uint16(1)
//clock为当前时间的Unix纳秒级时间戳
//clock为内部时钟，减少time.Now()调用造成的GC压力
//time.Now()每次调用时会读取系统时间（外部）并产生临时内存对象，需要GC来清理

//与时钟有关的函数
//1.返回 clock 变量的当前值。
//atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64{
	return atomic.LoadInt64(&clock)
}

//2.保持时钟的更新
func init(){
	go func(){
		for{
			atomic.StoreInt64(&clock,time.Now().UnixNano())//每秒校准一次
			for i:=0;i<9;i++{
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock,int64(100*time.Millisecond))//每0.1秒增加一次
				//保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()//启动一个协程来并行更新时钟的值
}

//额外算法
//1.BKDR哈希算法，用于计算键的哈希值
func hashBKRD(s string) (hash int32){
	for i:=0;i<len(s);i++{
		hash = hash*131 + int32(s[i])
	}

	return hash
}

//2.计算大于或等于输入值的最近2的幂次方减一作为掩码值
func maskOfNextPowOf2(cap uint16) uint16{
	if cap>0 && cap&(cap-1)==0{
		return cap-1
	}//cap&(cap-1)是专门用来检验是否是2的整次幂

	//不是2的整次幂，则需要通过四次右移和按位或操作，将最高位1后面的位全变为1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
	//因为cap为16位，1+2+4+8=15，正好能覆盖所有位置
}

//实例化函数
//1.cache的实例化
func Create(cap uint16) *cache{
	return &cache{
		dlnk: make([][2]uint16,cap+1),
		m: make([]node,cap),
		hmap: make(map[string]uint16,cap),
		last: 0,
	}
}

//2.lry2Store的实例化
func newLRU2Cache(opts Options) *lru2Store{
	if opts.BucketCount == 0{
		opts.BucketCount = 16
	}
	if opts.CapPerBucket == 0{
		opts.CapPerBucket = 1024
	}//一级缓存的容量
	if opts.Level2Cap == 0{
		opts.Level2Cap = 1024
	}//二级缓存的容量
	if opts.CleanupInterval <= 0{
		opts.CleanupInterval = time.Minute
	}
	//设置一些默认值

	mask := maskOfNextPowOf2(opts.BucketCount)//确保桶的容量是2的幂
	s := &lru2Store{
		locks:			make([]sync.Mutex,mask+1),
		caches:			make([][2]*cache,mask+1),
		onEvicted:		opts.OnEvicted,
		cleanupTick:	time.NewTicker(opts.CleanupInterval),
		mask:			int32(mask),
	}
	for i := range s.caches{
		s.caches[i][0] = Create(opts.CapPerBucket)
		s.caches[i][1] = Create(opts.Level2Cap)
	}

	if opts.CleanupInterval > 0{
		go s.cleanupLoop()
	}
	return s
}

//cache的绑定方法
//1.向缓存中添加项，如果是新增则返回1，更新返回0
func (c *cache) put(key string,val Value,expireAt int64,onEvicted func(string,Value)) int{
	//1.如果存在，则修改
	if idx,ok := c.hmap[key]; ok{
		c.m[idx-1].v,c.m[idx-1].expireAt = val,expireAt
		c.adjust(idx,p,n)//刷新到链表头部
		return 0
	}

	//2.如果容量已满，则复用最后的索引节点（准备删除的节点）（减少了GC压力）
	if c.last == uint16(cap(c.m)){//cap表示切片的最大容量，len表示切片的当前容量
		tail := &c.m[c.dlnk[0][p]-1]
		if onEvicted != nil && (*tail).expireAt>0{
			onEvicted((*tail).k,(*tail).v)
		}//复用节点前，先调用回调函数

		delete(c.hmap,(*tail).k)
		c.hmap[key],(*tail).k,(*tail).v,(*tail).expireAt = c.dlnk[0][p],key,val,expireAt
		c.adjust(c.dlnk[0][p],p,n)//将服用的节点刷新到链表头部

		return 1
	}

	//3.容量未满
	c.last++
	if len(c.hmap)<=0{
		c.dlnk[0][p]= c.last
	}else{
		c.dlnk[c.dlnk[0][n]][p]= c.last
	}//是否是第一个节点决定了谁的上一个节点是新增节点(也可以不用写，默认刚开始哨兵节点的前后节点都是他自己)

	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt

	c.dlnk[c.last] = [2]uint16{0,c.dlnk[0][n]}
	c.hmap[key] = c.last
	c.dlnk[0][n]= c.last

	return 1
}

//2.从缓存中获取键对应的节点和状态
func (c *cache) get(key string) (*node,int){
	if idx,ok := c.hmap[key];ok{
		c.adjust(idx,p,n)
		return &c.m[idx-1],1
	}
	return nil,0
}

//3.从缓存中删除键对应的项(并返回删除的节点信息)
func (c *cache) del(key string) (*node,int,int64){
	if idx,ok := c.hmap[key]; ok&& c.m[idx-1].expireAt>0{
		e:=c.m[idx-1].expireAt
		c.m[idx-1].expireAt = 0
		c.adjust(idx,n,p)
		return &c.m[idx-1],1,e
	}//通过标记expireAt为0表示节点已删除

	return nil,0,0
}//删除的节点都在二维数组的末尾，等待容量达到最大后复用

//4.遍历缓存中的所有有效项
func (c *cache) walk(walker func(key string,value Value,expireAt int64) bool){
	for idx := c.dlnk[0][n]; idx!=0; idx = c.dlnk[idx][n]{
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k,c.m[idx-1].v,c.m[idx-1].expireAt){
			return 
		}//只有当函数返回false时才有可能return（另一个条件是数据未过期）
	}
}

//5.调整节点在链表中的位置（节点移到头部还是尾部）,方法1，2，3均调用了该方法
//（0，1表示移到头部，1，0表示移到尾部）
func (c *cache) adjust(idx,f,t uint16){
	if c.dlnk[idx][f]!=0{//总共涉及到六条边的移动
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]

		c.dlnk[idx][f] = 0
		c.dlnk[idx][t] = c.dlnk[0][t]

		c.dlnk[c.dlnk[0][t]][f] = idx
		c.dlnk[0][t] = idx
	}//f为0时，前面的节点不是哨兵节点则要移动
	//f为1时，后面的节点不是哨兵节点则要移动
}

//lru2Store的绑定方法
//1.查找缓存项
func (s *lru2Store) Get(key string) (Value,bool){
	idx := hashBKRD(key) & s.mask//与mask进行与操作，使得哈希值不会超过桶的最大下标
	//这也是mask为什么要等与2的n次幂减一的原因

	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	currentTime := Now()//读取当前的时间

	//1.检查一级缓存
	n1,status1,expireAt := s.caches[idx][0].del(key)
	//之所以使用del操作是因为，如果找到了，不管是否过期还是要移入二级缓存，都要删除该节点
	if status1 > 0{
		//1.1找到了缓存项，但是过期了
		if expireAt > 0 && currentTime>=expireAt{
			s.delete(key,idx)
			fmt.Println("找到项目已过期，删除它")
			return nil,false
		}

		//1.2没有过期，移入二级缓存
		s.caches[idx][1].put(key,n1.v,expireAt,s.onEvicted)
		fmt.Println("项目有效，将其移至二级缓存")
		return n1.v,true
	}

	//2.检查二级缓存
	n2,status2 := s._get(key,idx,1)
	if status2 > 0 && n2!=nil{
		//2.1找到了，但是过期了
		if n2.expireAt >0 && currentTime>= n2.expireAt{
			s.delete(key,idx)
			fmt.Println("找到项目已过期，删除它")
			return nil,false
		}

		//2.2没有过期，直接返回
		return n2.v,true
	}

	//3.一二级缓存均没有找到
	return nil,false
}
//1.在对应缓存组中查找缓存项(主要是防止缓存过期或已被删除)
func (s *lru2Store) _get(key string,idx,level int32) (*node,int){
	if n,st := s.caches[idx][level].get(key);st>0 && n!=nil{
		currentTime := Now()
		if n.expireAt <= 0 || currentTime>= n.expireAt{
			return nil,0
		}
		return n,st
	}

	return nil,0
}

//2.添加缓存项（过期时间默认）
func (s *lru2Store) Set(key string,value Value) error{
	return s.SetWithExpiration(key,value,9999999999999999)
}

//3.添加缓存项（需要传入过期时间）
func (s *lru2Store) SetWithExpiration(key string,value Value,expiration time.Duration) error{
	//计算过期时间
	expireAt := int64(0)
	if expiration > 0{
		expireAt = Now() + int64(expiration.Nanoseconds())//确保expiration单位为纳秒
	}

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	//放入对应桶的一级缓存
	s.caches[idx][0].put(key,value,expireAt,s.onEvicted)

	return nil
}

//4.删除操作1(只传入key即可)
func (s *lru2Store) Delete(key string) bool{
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	return s.delete(key,idx)
}
//4.删除操作2（需要传入key和桶的位置）
func (s *lru2Store) delete(key string,idx int32) bool{
	n1,s1,_ := s.caches[idx][0].del(key)
	n2,s2,_ := s.caches[idx][1].del(key)
	deleted := s1>0 || s2>0

	if deleted && s.onEvicted!=nil{
		if n1 != nil && n1.v!=nil{
			s.onEvicted(key,n1.v)
		}else if n2!=nil && n2.v!=nil{
			s.onEvicted(key,n2.v)
		}
	}

	return deleted
}

//5.清空所有缓存项
func (s *lru2Store) Clear(){
	var keys []string

	for i := range s.caches{
		s.locks[i].Lock()

		//每个桶的一级缓存遍历
		s.caches[i][0].walk(func(key string,value Value,expireAt int64) bool{
			keys = append(keys,key)
			return true
		})
		//每个桶的二级缓存遍历
		s.caches[i][1].walk(func(key string,value Value,expireAt int64) bool{
			for _,k := range keys{
				if key == k{
					return true
				}
			}//防止key的重复
			keys = append(keys,key)
			return true
		})

		s.locks[i].Unlock()
	}
	
	//根据键值删除所有缓存项
	for _,key := range keys{
		s.Delete(key)
	}
}

//6.统计缓存项总数
func (s *lru2Store) Len() int{
	count := 0
	for i:= range s.caches{
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string,value Value,expireAt int64) bool{
			count++
			return true
		})
		s.caches[i][1].walk(func(key string,value Value,expireAt int64) bool{
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

//7.关闭缓存相关资源
func (s *lru2Store) Close(){
	if s.cleanupTick != nil{
		s.cleanupTick.Stop()
	}//必须手动关闭计时器，否则会导致资源泄露
}

//8.定时器定时清理过期项
func (s *lru2Store) cleanupLoop(){
	//本质与遍历所有缓存项类似
	for range s.cleanupTick.C{
		currentTime := Now()

		for i := range s.caches{
			s.locks[i].Lock()

			//检查并清理过期项目
			var expiredKeys []string

			s.caches[i][0].walk(func(key string,value Value,expireAt int64) bool{
				if expireAt > 0 && currentTime >= expireAt{
					expiredKeys = append(expiredKeys,key)
				}
				return true
			})

			s.caches[i][1].walk(func(key string,value Value,expireAt int64) bool{
				if expireAt>0 && currentTime >= expireAt{
					for _,k := range expiredKeys{
						if key == k{
							return true
						}
					}//避免键值重复
					expiredKeys = append(expiredKeys,key)
				}
				return true
			})

			for _,key := range expiredKeys{
				s.delete(key,int32(i))
			}

			s.locks[i].Unlock()
		}
	}
}	

