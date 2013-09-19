package main

import (
	"flag"
	"fmt"
	o "github.com/heartszhang/onlinefile"
	"net/http"
	"strconv"
	"strings"
)
import ()

var (
	uri    = flag.String("uri", "http://6pingm.com/v/127505.html", "url path")
	rstart = flag.Int("start", 0, "range start")
	rend   = flag.Int("end", 1024, "range end")
)

func main() {
	flag.Parse()
	if *uri == "" {
		flag.PrintDefaults()
		return
	}
	hrf := o.NewHttpRangeFile(*uri)
	buf := make([]byte, 512)
	for n, _ := hrf.Read(buf); n > 0; n, _ = hrf.Read(buf) {
		//		fmt.Println(e, `read 512 bytes`)
	}
	fmt.Println(`read one`)
	for n, _ := hrf.Read(buf); n > 0; n, _ = hrf.Read(buf) {
		//		fmt.Println(e, `read 512 bytes`)
	}
	fmt.Println(`read one`)
}

func main1() {
	flag.Parse()
	if *uri == "" {
		flag.PrintDefaults()
		return
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", *uri, nil)
	req.Header.Add(`Range`, print_byte_range(range_scope{*rstart, *rend}))
	req.Header.Add(`Connection`, `keep-alive`)
	resp, e := client.Do(req)
	if e == nil {
		defer resp.Body.Close()
	}
	fmt.Println(resp, e)
	req2, _ := http.NewRequest("GET", *uri, nil)
	req2.Header.Add(`Range`, print_byte_range_scope(1024, 2048))
	req2.Header.Add(`Connection`, `keep-alive`)
	resp2, e := client.Do(req2)
	fmt.Println(resp2, e)
}

type byte_range_spec interface {
	String() string
}

func print_byte_range_scope(start, end int) string {
	return print_byte_range(range_scope{start, end})
}
func print_byte_range(specs ...byte_range_spec) string {
	//	v := `bytes=`
	//	return v + strings.Join(specs, `,`)
	strs := make([]string, len(specs))
	for idx, spec := range specs {
		strs[idx] = spec.String()
	}
	rtn := `bytes=` + strings.Join(strs, `,`)
	fmt.Println(rtn)
	return rtn
}

type range_start_at struct {
	at int
}

func (this range_start_at) String() string {
	return strconv.Itoa(this.at) + `-`
}

type range_last_n struct {
	n int
}

func (this range_last_n) String() string {
	return `-` + strconv.Itoa(this.n)
}

type range_scope struct {
	first, last int
}

func (this range_scope) String() string {
	return strconv.Itoa(this.first) + `-` + strconv.Itoa(this.last)
}

type content_range struct {
	first, last     int
	instance_length int
}

func new_content_range(header string) content_range {
	var c content_range
	spec := strings.Fields(header)
	if len(spec) < 2 {
		return c
	}
	bspec := strings.Split(spec[1], `/`)
	if len(bspec) < 2 {
		return c
	}
	// rspec = strings.Split(bspec[0])
	return c
}
