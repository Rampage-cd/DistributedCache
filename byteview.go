package mycache

//ByteView为只读的字节视图，用于缓存数据
type ByteView struct{
	b []byte
}

func (b ByteView) Len() int{
	return len(b.b)
}//实现了Len()方法，意味着实现了Value接口

func (b ByteView) ByteSlice() []byte{
	return cloneBytes(b.b)
}

func (b ByteView) String() string{
	return string(b.b)
}

func cloneBytes(b []byte) []byte{
	c := make([]byte,len(b))
	copy(c,b)
	return c
}