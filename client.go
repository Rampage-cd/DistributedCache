package mycache

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Rampage-cd/DistributedCache/pb"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//客户端的实现
//通过gRPC与远端缓存节点通信，并可复用etcd客户端进行服务发现

//Client的作用：gRPC客户端（与缓存节点通信），etcd客户端持有者（用于服务发现/注册）
type Client struct{
	addr string
	svcName string
	etcdCli *clientv3.Client
	conn *grpc.ClientConn
	grpcCli pb.MyCacheClient
}//实现了Peer接口

//编译期接口断言（确保*Client实现了Peer接口）
var _ Peer = (*Client)(nil)

func NewClient(addr string,svcName string,etcdCli *clientv3.Client) (*Client,error){
	var err error

	//如果外部没有传入etcd客户端，则创建一个默认的
	if etcdCli == nil{
		etcdCli,err = clientv3.New(clientv3.Config{
			Endpoints: []string{"localhost:2379"},//etcd集群地址
			DialTimeout: 5*time.Second,//建立连接的超时时间
		})
		if err != nil{
			return nil,fmt.Errorf("failed to create etcd client: %v",err)
		}
	}

	//建立grpc连接
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),//不安全的传输（无TLS）
		grpc.WithBlock(),//Dial调用会阻塞，直到连接建立成功或失败
		grpc.WithTimeout(10*time.Second),//连接超时时间
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),//RPC调用时，如果连接尚未就绪，会等待而不是立刻失败
	)
	if err != nil{
		return nil,fmt.Errorf("failed to dial server: %v",err)
	}

	//基于gRPC连接创建业务客户端
	grpcClient := pb.NewMyCacheClient(conn)

	//构造Client实例
	client := &Client{
		addr: addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn: conn,
		grpcCli: grpcClient,
	}

	return client,nil
}

//1.Get 从远端MyCache获取缓存数据
func (c *Client) Get(group,key string) ([]byte,error){
	//为本次gRPC调用创建一个带超时的context
	ctx,cancel := context.WithTimeout(context.Background(),3*time.Second)
	defer cancel()

	//发起gRPC请求
	resp,err := c.grpcCli.Get(ctx,&pb.Request{
		Group: group,
		Key: key,
	})
	if err != nil{
		return nil,fmt.Errorf("failed to get value from mycache: %v",err)
	}

	//返回响应中的value字段
	return resp.GetValue(),nil	
}

//2.Delete从远端MyCache删除缓存数据
func (c *Client) Delete(group,key string) (bool,error){
	ctx,cancel := context.WithTimeout(context.Background(),3*time.Second)
	defer cancel()

	resp,err := c.grpcCli.Delete(ctx,&pb.Request{
		Group: group,
		Key:	key,
	})
	if err != nil{
		return false,fmt.Errorf("failed to delete value from mycache: %v",err)
	}

	return resp.GetValue(),nil
}

//3.Set向远端MyCache写入缓存数据
func (c *Client) Set(ctx context.Context,group,key string,value []byte) error{
	resp,err := c.grpcCli.Set(ctx,&pb.Request{
		Group: group,
		Key: key,
		Value:value,
	})
	if err != nil{
		return fmt.Errorf("failed to set value to mycache: %v",err)
	}

	//打印服务端返回结果(便于调试)
	logrus.Infof("grpc set request resp: %+v",resp)

	return nil
}

//4.关闭客户端资源(只负责关闭gRPC连接，etcd客户端是否关闭，通常由创建方决定)
func (c *Client) Close() error{
	if c.conn != nil{
		return c.conn.Close()
	}
	return nil
}