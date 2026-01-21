package singleflight

import(
	"sync"
)

//代表正在进行或已经结束的请求
type call struct{
	wg sync.WaitGroup
	val interface{}
	err error
}

type Group struct{
	m sync.Map//sync.Map是Go标准库sync包提供的一个并发安全的map实现
	//适用于读多写少，key生命周期较长，多个goroutine并发访问
}

//Do 针对相同的key，保证多次调用Do(),都只会调用一次fn
func (g *Group) Do(key string,fn func() (interface{},error)) (interface{},error){
	if existing,ok := g.m.Load(key); ok{
		c := existing.(*call)//使用Load操作得到值后，需要通过类型断言确定值的类型
		c.wg.Wait()//Done函数执行之后，等待的所有进程开始执行
		return c.val,c.err
	}

	//第一次调用Do，在m中保存（保存之后，再次调用Do会进入忙等待），然后调用fn()获取值
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key,c)

	c.val,c.err = fn()
	c.wg.Done()//获取完成后执行Done函数

	g.m.Delete(key)//每个key用完就删（因为c的作用是合并同一时刻，大量相同key的请求，下一时刻则需要重新调用fn函数获取数据）

	return c.val,c.err
}