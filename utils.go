package mycache

import "strings"

func ValidPeerAddr(addr string) bool{
	t1 := strings.Split(addr,":")
	if len(t1) != 2{
		return false
	}//addr必须有两部分组成（IP+端口）

	 t2 := strings.Split(t1[0],".")
	 if t1[0] != "localhost" && len(t2) != 4{
		return false
	 }//IP部分必须是localhost或者IPv4

	 return true
}