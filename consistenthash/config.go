package consistenthash

import "hash/crc32"

//Config 一致性哈希的配置结构体
type Config struct{
	DefaultReplicas int			//每个真实节点对应的虚拟节点数
	MinReplicas int 			//最小虚拟节点数
	MaxReplicas int				//最大虚拟节点数
	HashFunc	func(data []byte) uint32//哈希函数
	LoadBalanceThreshold float64//负载均衡阈值，超过此值触发虚拟节点调整
}

//Config的默认配置
var DefaultConfig = &Config{
	DefaultReplicas:		50,
	MinReplicas:			10,
	MaxReplicas:			200,
	HashFunc:				crc32.ChecksumIEEE,
	LoadBalanceThreshold:	0.25,//25%的负载不均衡度触发调整
}