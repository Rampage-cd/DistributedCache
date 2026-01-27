package consistenthash

import(
	"errors"
	"sync"
	"sync/atomic"
	"fmt"
	"time"
	"sort"
	"math"
)

//Map 一致性哈希实现
type Map struct{
	mu sync.RWMutex

	config *Config //配置信息
	
	keys []int     //哈希环(存放哈希环上所有虚拟节点的哈希值)
	hashMap map[int]string//哈希环到节点的映射（记录虚拟节点属于哪个真实节点）
	nodeReplicas map[string]int//节点到虚拟节点数量的映射（记录真实节点有多少个虚拟节点）
	nodeCounts map[string]int64 //节点负载统计
	totalRequests int64			//总请求数
}

//New 创建一个Map实例
func New(opts ...Option) *Map{
	m := &Map{
		config:				DefaultConfig,//调用同一个包下，config.go中对Config的默认配置
		hashMap:			make(map[int]string),
		nodeReplicas:		make(map[string]int),
		nodeCounts:			make(map[string]int64),
	}

	for _,opt := range opts{
		opt(m)
	}

	m.startBalancer() //启动负载均衡器
	return m
}

//Option 配置选项
type Option func(*Map)

func WithConfig(config *Config) Option{
	return func(m *Map) {
		m.config = config
	}
}

//绑定方法
//1.Add添加节点
func (m *Map) Add(nodes ...string) error{
	if len(nodes) == 0{
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _,node := range nodes{
		if node == ""{
			continue
		}

		//如果该节点有意义的话，为节点添加虚拟节点
		m.addNode(node,m.config.DefaultReplicas)
	}

	//重新排序
	sort.Ints(m.keys)
	return nil
}

//2.添加节点的虚拟节点
func (m *Map) addNode(node string,replicas int){
	for i:=0; i<replicas; i++{
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d",node,i))))//计算虚拟节点对应的哈希值
		m.keys = append(m.keys,hash)
		m.hashMap[hash] = node
	}

	m.nodeReplicas[node] = replicas
}

//3.移除节点
func (m *Map) Remove(node string) error{
	if node == ""{
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	//获取该节点有多少个虚拟节点
	replicas := m.nodeReplicas[node]
	if replicas == 0{
		return fmt.Errorf("node %s not found",node)
	}

	//移除节点的所有虚拟节点
	for i:=0;i<replicas;i++{
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d",node,i))))
		delete(m.hashMap,hash)
		for j := 0;j<len(m.keys);j++{
			if m.keys[j]==hash{
				m.keys = append(m.keys[:j],m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas,node)
	delete(m.nodeCounts,node)
	return nil
}

//4.Get获取节点(根据key的哈希值找到对应虚拟节点的哈希值，再根据虚拟节点的哈希值找到真实节点)
func (m *Map) Get(key string) string{
	if key == ""{
		return ""
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys)==0{
		return ""
	}

	hash := int(m.config.HashFunc([]byte(key)))
	//二分查找
	idx := sort.Search(len(m.keys),func(i int) bool{
		return m.keys[i]>=hash//找到第一个满足该条件的元素索引
	})

	//处理边界情况
	if idx == len(m.keys){
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	count := m.nodeCounts[node]
	m.nodeCounts[node] = count + 1
	atomic.AddInt64(&m.totalRequests,1)

	return node
}

//5.检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance(){
	if atomic.LoadInt64(&m.totalRequests) < 1000{
		return //样本太少，不进行调整
	}

	//计算负载情况
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))//平均每个节点承受的负载
	var maxDiff float64

	for _,count := range m.nodeCounts{
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff{
			maxDiff = diff / avgLoad
		}
	}

	//如果最大的负载不均衡度超过阈值，调整虚拟节点
	if maxDiff > m.config.LoadBalanceThreshold{
		m.rebalanceNodes()
	}
}

//6.重新平衡节点
func (m *Map) rebalanceNodes(){
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	//调整每个节点的虚拟节点数量
	for node,count := range m.nodeCounts{
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(count) /avgLoad

		var newReplicas int
		if loadRatio > 1{
			//负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)//必须转化为int，因为虚拟节点个数必须为整数
		}else{
			//负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		//确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		//重新添加节点的虚拟节点
		if newReplicas != currentReplicas{
			if err := m.Remove(node); err != nil{
				continue//移除失败，则跳过
			}
			m.addNode(node,newReplicas)
		}
	}

	//重置计数器
	for node := range m.nodeCounts{
		m.nodeCounts[node]=0
	}
	atomic.StoreInt64(&m.totalRequests,0)

	//重新排序
	sort.Ints(m.keys)
}

//7. GetStats 获取负载统计信息（每个节点负载的百分比）
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

//8.将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func(){
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C{
			m.checkAndRebalance()
		}//无限循环可能会导致协程无法退出
	}()
}