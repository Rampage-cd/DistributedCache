package mycache

import(
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/Rampage-cd/DistributedCache/singleflight"
)

//定义常见的错误
//键不能为空错误
var ErrKeyRequired = errors.New("key is required")

//值不能为空错误
var ErrValueRequired = errors.New("value is required")

//组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")

//Getter:加载键值的回调函数接口
type Getter interface{
	Get(ctx context.Context,key string) ([]byte,error)
}

//GetterFunc函数类型实现Getter接口
type GetterFunc func(ctx context.Context,key string) ([]byte,error)

//Get方法实现Getter接口
func (f GetterFunc) Get(ctx context.Context,key string) ([]byte,error){
	return f(ctx,key)
}

var(
	groupsMu sync.RWMutex
	groups = make(map[string]*Group)
)

//缓存命名空间结构体
type Group struct{
	name		string
	getter 		Getter					//缓存未命中时的回调函数
	mainCache	*Cache					//本地缓存
	peers		PeerPicker				//选择合适的节点拿取想要的数据
	loader 		*singleflight.Group		//单飞机制防止缓存击穿
	expiration	time.Duration			//缓存过期时间，0表示永不过期
	closed		int32					//原子变量，标记组是否关闭
	stats 		groupStats				//统计信息
}

//统计信息结构体
type groupStats struct{
	loads			int64//缓存加载次数以及命中，未命中的次数
	localHits		int64
	localMisses		int64
	peerHits		int64//从节点获取的成功和失败次数
	peerMisses		int64
	loaderHits		int64//从加载器获取的成功和失败次数
	loaderErrors	int64
	loadDuration	int64//加载总耗时
}

//定义Group的配置选项
//为什么要使用函数类型，且后续有相关函数返回该函数类型？
//为了解决参数爆炸问题，可以在不修改构造函数参数的情况下，只添加一个对应的函数即可实现参数的增加
//为了解决默认值覆盖问题，在构造函数中已经设置好了默认值，如果传入了相关的函数，则会实现默认值被覆盖的效果
type GroupOption func(*Group)

//初始化操作
//1.设置缓存过期时间
func WithExpiration(d time.Duration) GroupOption{
	return func(g *Group){
		g.expiration = d
	}
}

//2.设置分布式节点
func WithPeers(peers PeerPicker) GroupOption{
	return func(g *Group){
		g.peers = peers
	}
}

//3.设置缓存选项
func WithCacheOptions(opts CacheOptions) GroupOption{
	return func(g *Group){
		g.mainCache = NewCache(opts)
	}
}

//4.创建一个新的Group实例
func NewGroup(name string,cacheBytes int64,getter Getter,opts ...GroupOption) *Group{
	if getter == nil{
		panic("nil Getter")
	}

	//创建默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:		name,
		getter:		getter,
		mainCache: 	NewCache(cacheOpts),
		loader:		&singleflight.Group{},
	}

	for _,opt := range opts{//opts实际上是多个匿名函数的切片
		opt(g)//调用匿名函数，闭包生效
	}

	//注册到全局映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _,exists := groups[name]; exists{
		logrus.Warnf("Group with name %s already exists, will be replaced",name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d,expiration=%v",name,cacheBytes,g.expiration)

	return g
}

//获取指定名称的组
func GetGroup(name string) *Group{
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

//绑定方法
//1.Get从缓存获取数据
func (g *Group) Get(ctx context.Context,key string) (ByteView,error){
	//检查组是否已经关闭，键是否为空
	if atomic.LoadInt32(&g.closed) == 1{
		return ByteView{}, ErrGroupClosed
	}

	if key == ""{
		return ByteView{}, ErrKeyRequired
	}

	//从本地缓存获取
	view, ok := g.mainCache.Get(ctx,key)
	if ok{
		atomic.AddInt64(&g.stats.localHits,1)
		return view,nil
	}

	atomic.AddInt64(&g.stats.localMisses,1)

	//尝试从其他节点获取和加载
	return g.load(ctx,key)
}

//2.load加载数据
func (g *Group) load(ctx context.Context,key string) (value ByteView,err error){
	//使用singleflight确保并发请求只加载一次
	startTime := time.Now()
	viewi,err := g.loader.Do(key, func() (interface{}, error) {
		return g.loadData(ctx,key)
	})

	//记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration,loadDuration)
	atomic.AddInt64(&g.stats.loads,1)

	if err != nil{
		atomic.AddInt64(&g.stats.loaderErrors,1)
		return ByteView{},err
	}

	view := viewi.(ByteView)

	//设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	return view, nil
}

//3.实际加载数据的方法
func (g *Group) loadData(ctx context.Context,key string) (value ByteView,err error){
	//尝试从远程节点获取
	if g.peers != nil{
		peer,ok,isSelf := g.peers.PickPeer(key)
		if ok && !isSelf{
			value,err := g.getFromPeer(ctx,peer,key)
			if err == nil{
				atomic.AddInt64(&g.stats.peerHits,1)
				return value,nil
			}

			atomic.AddInt64(&g.stats.peerMisses,1)
			logrus.Warnf("[mycache] failed to get from peer: %v",err)
		}
	}

	//从数据源加载
	bytes,err := g.getter.Get(ctx,key)
	if err != nil{
		return ByteView{},fmt.Errorf("failed to get data: %w",err)
	}

	atomic.AddInt64(&g.stats.loaderHits,1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

//4.getFromPeer从其他节点获取数据
func (g *Group) getFromPeer(ctx context.Context,peer Peer,key string) (ByteView,error){
	bytes,err := peer.Get(g.name,key)
	if err != nil{
		return ByteView{},fmt.Errorf("failed to get from peer: %w",err)
	}
	return ByteView{b: bytes},nil
}

//5.Set设置缓存值
func (g *Group) Set(ctx context.Context,key string,value []byte) error{
	//检查缓存组是否关闭，键是否为空，值是否为空
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	//检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	//创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	//设置到本地缓存
	if g.expiration > 0{
		g.mainCache.AddWithExpiration(key,view,time.Now().Add(g.expiration))
	}else{
		g.mainCache.Add(key,view)
	}

	//如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	if !isPeerRequest && g.peers != nil{
		go g.syncToPeers(ctx,"set",key,value)
	}

	return nil
}

//6.syncToPeers同步操作到其他节点
func (g *Group) syncToPeers(ctx context.Context,op string,key string,value []byte){
	if g.peers == nil{
		return 
	}

	//选择对等节点
	peer,ok,isSelf := g.peers.PickPeer(key)
	if !ok || isSelf{
		return 
	}

	//选择同步请求上下文
	syncCtx := context.WithValue(context.Background(),"from_peer",true)

	var err error
	switch op{
	case "set":
		err = peer.Set(syncCtx,g.name,key,value)
	case "delete":
		_,err = peer.Delete(g.name,key)
	}

	if err != nil{
		logrus.Errorf("[mycache] failed to sync %s to peer: %v",op,err)
	}
}

//7.Delete删除缓存值
func (g *Group) Delete(ctx context.Context,key string) error{
	//检查组是否已经关闭
	if atomic.LoadInt32(&g.closed) == 1{
		return ErrGroupClosed
	}

	if key == ""{
		return ErrKeyRequired
	}

	//从本地缓存删除
	g.mainCache.Delete(key)

	//检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	//如果不是从其他节点同步过来的请求，且开启了分布式模式，则同步到其他节点
	if !isPeerRequest && g.peers != nil{
		go g.syncToPeers(ctx,"delete",key,nil)
	}

	return nil
}

//Clear清空缓存
func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[KamaCache] cleared cache for group [%s]", g.name)
}

// Close 关闭组并释放资源
func (g *Group) Close() error {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 从全局组映射中移除
	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[KamaCache] closed cache group [%s]", g.name)
	return nil
}

// RegisterPeers 注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[MyCache] registered peers for group [%s]", g.name)
}

// Stats 返回缓存统计信息
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}//time.Duration的默认单位是纳秒，除以毫秒后，变成以毫秒为单位

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
	//stats中不仅有缓存组的信息，还有其内部缓存的信息
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 销毁指定名称的缓存组
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
	}
}