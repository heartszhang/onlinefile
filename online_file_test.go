package onlinefile

import (
	"fmt"
	"testing"
)

func TestNewHttpRangeFile(t *testing.T) {
	hrf := NewHttpRangeFile("http://www.sina.com.cn")
	buf := make([]byte, 8192)
	for {
		n, e := hrf.Read(buf)
		if n == 0 || e != nil {
			break
		}
	}
	t.Println(`done`)
}
