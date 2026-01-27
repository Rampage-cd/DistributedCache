package registry

import(
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

//Config 定义etcd客户端配置
type Config struct{
	Endpoints []string 		//集群地址
	DialTimeout time.Duration//连接超时时间
}

//DefaultConfig 提供默认配置
var DefaultConfig = &Config{
	Endpoints:		[]string{"localhost:2379"},
	DialTimeout:	5*time.Second,
}

//Register注册服务到etcd
func Register(svcName,addr string, stopCh <-chan error) error{
	// --- 第一阶段：初始化 etcd 客户端 ---
	// clientv3.New 会创建一个与 etcd 集群的 gRPC 连接池
	cli,err := clientv3.New(clientv3.Config{
		Endpoints:		DefaultConfig.Endpoints,
		DialTimeout:	DefaultConfig.DialTimeout,
	})//New函数返回一个Client结构体，内部有着各种各样的功能接口
	if err != nil{
		return fmt.Errorf("failed to create etcd clientf: %v",err)
	}

	// --- 第二阶段：地址自动补全 ---
	// 获取本机非回环 IP（如 192.168.1.50）
	localIP,err := getLocalIP()
	if err != nil{
		cli.Close()
		return fmt.Errorf("failed to get local IP: %v.",err)
	}
	// 如果传入的 addr 是 ":8081" 这种形式，自动补全为 "192.168.1.50:8081"
	if addr[0] == ':'{
		addr = fmt.Sprintf("%s%s",localIP,addr)
	}

	// --- 第三阶段：创建租约 (Lease) ---
	// Grant 申请一个生存时间 (TTL) 为 10 秒的租约
	// 术语解释：租约 (Lease) 是 etcd 维持心跳的机制。如果客户端在租约到期前不续约，
	// etcd 会自动删除所有绑定在该租约上的 Key。
	lease,err := cli.Grant(context.Background(),10)
	if err != nil{
		cli.Close()
		return fmt.Errorf("failed to create lease: %v",err)
	}

	// --- 第四阶段：写入 Key-Value 并绑定租约 ---
	// 设计 Key 的路径：/services/{服务名}/{地址}，方便服务发现端使用前缀查询
	key := fmt.Sprintf("/services/%s/%s",svcName,addr)
	// WithLease(lease.ID) 将这个 Key 的生命周期与上面创建的租约绑定
	_,err = cli.Put(context.Background(),key,addr,clientv3.WithLease(lease.ID))
	if err != nil{
		cli.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v",err)
	}

	// --- 第五阶段：开启自动续期 (KeepAlive) ---
	// KeepAlive 会返回一个通道，etcd SDK 会在后台自动发送心跳包给服务器续命
	// 只要程序不挂，租约就不会过期
	KeepAliveCh,err := cli.KeepAlive(context.Background(),lease.ID)
	//返回一个只读Channel，客户端通过它来确定续租成功
	if err != nil{
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v",err)
	}

	// --- 第六阶段：生命周期管理 (异步协程) ---
	go func(){
		defer cli.Close()
		for {
			select{
			case <-stopCh:
				// 情况 A：收到外部停止信号（如程序退出或手动注销）
				// 术语解释：Revoke (撤销) 会立即让租约失效并删除关联的 Key，实现“优雅下线”
				ctx,cancel := context.WithTimeout(context.Background(),3*time.Second)
				//超时时间到后，ctx会自动取消
				cli.Revoke(ctx,lease.ID)
				cancel()//可以尽早释放资源
				return 
			case resp,ok := <-KeepAliveCh:
				//情况 B：收到自动续期的应答
				if !ok{
					//如果通道关闭，说明与etcd的心跳连接断开了
					logrus.Warn("keep alive channel closed")
					return 
				}
				//记录每次成功续约的记录
				logrus.Debugf("successfully renewed lease: %d",resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s",svcName,addr)
	return nil
}

//获取本地IP（获取当前主机第一个非回环（Non-Loopback）的IPv4地址
func getLocalIP() (string,error){
	addrs,err := net.InterfaceAddrs()//向操作系统查询当前主机所有网络接口的单播地址列表，返回一个net.Addr切片
	if err != nil{
		return "",err
	}

	for _,addr := range addrs{
		if ipNet,ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback(){
			//net.IPNet是一个结构体，代表一个IP网络，内部数据类型为IP(net.IP)和Mask(net.IPMask)
			//使用类型断言的原因是addr为接口类型，需要通过类型断言才能访问到里面的IP字段
			//IsLoopback()函数用于检验该IP是否为回环地址（发往该地址的数据包不会离开主机网卡，而是会直接返回给本机）
			if ipNet.IP.To4() != nil{
				//To4函数将IP地址转换为4字节表示形式，如果是IPv6地址将返回nil（用于过滤IPv6地址）
				return ipNet.IP.String(),nil
				//net IP本质上是一个字节切片，通过String()将其转换成字符串
			}
		}
	}

	return "",fmt.Errorf("no valid local IP found")
}