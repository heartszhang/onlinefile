package onlinefile

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	_16K = 16384
	_1M  = 1024 * 1024
	_1K  = 1024
	_1B  = 256
)
const (
	status_recv_200 = 1 << iota
	status_recv_206
)
const (
	status_failed_0 = -iota
	status_closed
	status_failed_invalidrange
	status_failed_badrequest
	status_failed_statuscode
	status_recv_failed
	status_done

	slice8 = 8 //
)

type OnlineFile interface {
	io.Reader
	io.Seeker
	io.Closer
}

type HttpRangeFile struct {
	lock   sync.Locker
	update *sync.Cond

	slices       slice_table
	slice_size   int64 // const default is 1K
	length       int64
	read_pointer int64
	workers      int
	uri          string
	packets      chan *packet

	status int // < 0 : error-code, 0 : working, 1 : done, 2 : closed
	error  error

	fragments     []*os.File
	fragment_size int64
}

/*
fragments
write
read
resize
*/

func NewHttpRangeFile(uri string) OnlineFile {
	l := &sync.Mutex{}
	v := &HttpRangeFile{
		uri:           uri,
		packets:       make(chan *packet, _1B),
		slices:        new_file_desc(_16K, _1K),
		slice_size:    _1K,
		fragment_size: _16M,
		lock:          l,
		update:        sync.NewCond(l)}
	go v.work()
	return v
}

func (this *HttpRangeFile) Read(buf []byte) (n int, err error) {
	this.update.L.Lock()
	for !this.readable() {
		this.update.Wait()
	}
	c := min(this.avail(), int64(len(buf)))
	this.update.L.Unlock()

L:
	for c > 0 {
		idx := this.read_pointer / this.fragment_size
		in_frag := this.read_pointer % this.fragment_size
		l := min(c, this.fragment_size-in_frag)
		var led int
		led, err = this.read_from_fragment(int(idx), in_frag, buf[n:n+int(l)])
		this.read_pointer += int64(led)
		c -= int64(led)
		n += led

		switch err {
		default:
			break L
		case nil:
		case io.EOF:

			break L
		}
	}
	return
}

func (this *HttpRangeFile) Seek(offset int64, whence int) (ret int64, err error) {
	this.lock.Lock()
	switch whence {
	case 0:
		this.read_pointer = offset
	case 1:
		this.read_pointer += offset
	case 2:
		this.read_pointer = this.length - offset
	}
	ret = this.read_pointer
	c := this.avail()
	this.lock.Unlock()
	if c > slice8*this.slice_size {
		log.Println(ret, "seek triges another worker")
		go this.work()
	}
	return ret, nil
}

const (
	max_cocurrent_workers = 3 // enable 2 workers cocurrent
)

//http-server回应200ok，在下面一些情况不接受这些数据
// 已经收到了200
// 已经收到了206
// 已经发生错误
func (this *HttpRangeFile) unaccept_200() bool {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.status < 0 {
		return true
	}
	s200 := this.status & status_recv_200
	s206 := this.status & status_recv_206
	if s200 != 0 || s206 != 0 {
		return true
	}
	this.status |= status_recv_200
	return false
}

//已经收到200的时候，就不再接受206
//这种情况并不常见，一般认为发生了某种错误
func (this *HttpRangeFile) unaccept_206() bool {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.status < 0 {
		return true
	}
	s200 := this.status & status_recv_200
	s206 := this.status & status_recv_206
	if s200 != 0 {
		return true
	}
	if s206 == 0 {
		this.status |= status_recv_206
	}
	return false
}

//取出离read-pointer最近的尚未下载的slice索引
//发生错误或者，完整下载的情况下，不允许取出这些索引
func (this *HttpRangeFile) unready(slices int) (begin, end int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.status < 0 || (this.status&status_recv_200) != 0 {
		return -1, -1
	}
	rpidx := int(this.read_pointer / this.slice_size)
	begin, end = this.slices.unready_after(rpidx, slice8)

	//没有需要下载的内容, 从头开始搜索未下载的内容
	if begin == end && this.read_pointer != 0 {
		begin, end = this.slices.unready_after(0, slice8)
	}
	return
}

func min(lhs, rhs int64) int64 {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

//创建http range请求
//指定range范围，设置connection : keep-alive
//http.client关闭gzip，是通过disablecompression实现的，而不是直接设置Content-encoding
func (this *HttpRangeFile) new_request(begin, end int) (*http.Request, int64, int64, error) {
	first := int64(begin) * this.slice_size
	last := min(int64(end)*this.slice_size, this.length) - 1
	if this.length == 0 {
		last = int64(end)*this.slice_size - 1
	}
	if last <= first {
		return nil, first, last, fmt.Errorf(`%v : invalid range`, status_failed_invalidrange)
	}
	req, err := http.NewRequest(`GET`, this.uri, nil)
	if req != nil {
		req.Header.Add(`Connection`, `keep-alive`)
		req.Header.Add(`Range`, print_http_range(first, last))
	}
	return req, first, last, err
}

func print_http_range(first, last int64) string {
	return fmt.Sprintf(`bytes=%d-%d`, first, last)
}

func new_range_http_client() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableCompression: true}}
}

//发生错误后，保存错误信息，更新文件状态
func (this *HttpRangeFile) error_and_close(status int, err error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.status = status
	this.error = err
	if err != nil {
		this.Close()
	}
	// trig all reads to fail
	this.update.Broadcast()
}

//不需要等待writer和worker结束
func (this *HttpRangeFile) Close() error {
	this.status = status_closed
	this.packets <- nil
	fs := this.fragments
	this.fragments = make([]*os.File, 0)
	for _, f := range fs {
		f.Close()
	}
	return nil
}

const (
	_16M int64 = 16 * 1024 * 1024
	_1G  int64 = 1024 * 1024 * 1024

//	fragment_default_size       = _16M
)

//文件初始长度和真实长度不一致，下载过程中，如果服务器回送的文件长度发生变化
//都需要重新设定文件长度，但是文件数据不会被清理。这有可能导致文件内容错误
func (this *HttpRangeFile) try_reset_length(length int64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if length == this.length {
		return
	}
	if length < 0 {
		this.fragment_size = _1G
	}
	fragment_count := (length + this.fragment_size - 1) / this.fragment_size
	if fragment_count == 0 { // when transfer-encoding: chunked
		fragment_count = 1
	}
	log.Println(`frag-count:`, fragment_count, `total:`, length, `reset length`)

	switch {
	default:
	case this.fragments == nil:
		this.fragments = make([]*os.File, fragment_count)
		for i := 0; i < int(fragment_count); i++ {
			this.fragments[i], _ = ioutil.TempFile(``, ``)
		}
	case len(this.fragments) < int(fragment_count):
		nfs := make([]*os.File, fragment_count)
		copy(nfs, this.fragments)
		for i := len(this.fragments); i < int(fragment_count); i++ {
			nfs[i], _ = ioutil.TempFile(``, ``)
		}
		this.fragments = nfs
	case len(this.fragments) > int(fragment_count):
		for i := int(fragment_count); i < len(this.fragments); i++ {
			this.fragments[i].Close()
		}
		this.fragments = this.fragments[:fragment_count]
	}
	this.length = length
	this.slices.resize(length, this.slice_size)
}

func (this *HttpRangeFile) readable() bool {
	rpidx := int(this.read_pointer / this.slice_size)
	c := this.slices.avail_after(rpidx)
	return c > 0 || this.status < 0
}

// needn't lock
func (this *HttpRangeFile) avail() int64 {
	rpidx := this.read_pointer / this.slice_size
	c := this.slices.avail_after(int(rpidx))
	return int64(c) * this.slice_size
}

//分片下载过程
func (this *HttpRangeFile) work() {
	log.Println(`enter work`)
	var workers = this.register_work()
	defer this.unregister_work()
	if workers > max_cocurrent_workers { // too many workers
		return
	}
	//发生错误后，unread分配不出来需要下载的分片
	//begin, end是分片下标
	begin, end := this.unready(slice8)
	if begin == end {
		this.error_and_close(status_done, nil)
		return
	}
	log.Println(begin, end, `should some work be done`)
	req, _, _, err := this.new_request(begin, end)
	if err != nil {
		this.error_and_close(status_failed_badrequest, err)
		return
	}
	client := new_range_http_client() // disable gzip
	resp, err := client.Do(req)
	if err != nil {
		this.error_and_close(status_failed_badrequest, err)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		if this.unaccept_200() {
			return
		}
		//传输使用了Transfer-encoding:chunked或者长度未知的情况下content-length都不会有
		// 这种情况我们目前是不支持的
		//	te := resp.Header.Get(`Transfer-Encoding`)
		log.Println(`content-length`, resp.ContentLength)
		this.try_reset_length(resp.ContentLength)
		err = this.do_receive(resp, 0, this.length)
		if err == nil {
			this.error_and_close(status_done, nil)
		}
	case http.StatusPartialContent:
		if this.unaccept_206() {
			return
		}
		//如果first位置和我们请求的并不一致，没有按照分片进行对齐，程序处理不了这种情况
		first, last, total, _ := parse_content_range(resp.Header.Get(`Content-Range`))
		log.Println(first, last, total, `status 206`)
		this.try_reset_length(total)
		err = this.do_receive(resp, first, last+1)
		if err == nil {
			log.Println(`begin work`)
			go this.work()
		}
	default:
		err = fmt.Errorf(`%v: unsupported http response`, resp.StatusCode)
		this.error_and_close(status_failed_statuscode, err)
	}
}

//读满缓冲区或者EOF或者出现错误
func read_until_full(reader io.Reader, buf []byte) (n int, err error) {
	var x int
	for n < len(buf) && err == nil {
		x, err = reader.Read(buf[n:])
		n += x
	}
	return
}

func (this *HttpRangeFile) write_fragment_imp(buf []byte, begin, size int64) {
	//	log.Println(`begin:`, begin, `size:`, size, `write-fragment`, len(this.fragments))
	if begin%this.slice_size != 0 {
		// write a log
		return
	}
	var offset int64 = 0
	for offset < size {
		frag_idx := int((begin + offset) / this.fragment_size)
		in_frag := (begin + offset) % this.fragment_size
		l := min(size-offset, this.fragment_size-in_frag)
		n, _ := this.fragments[frag_idx].WriteAt(buf[int(offset):int(offset+l)], in_frag)
		offset += int64(n)
		log.Println(`bytes:`, n, `writed to fragment`, frag_idx)
	}
	idx := begin / this.slice_size
	end := (size+this.slice_size-1)/this.slice_size + idx

	this.lock.Lock()
	defer this.lock.Unlock()
	for i := idx; i < end; i++ {
		this.slices.update(int(i), slice_status_ready)
	}
	this.update.Broadcast()
}

func (this *HttpRangeFile) write_fragment(buf []byte, begin, size int64) {
	/*	p := &packet{offset: begin}
		p.data = make([]byte, count)
		copy(p.data, buf)
		this.packets <- p
	*/
	this.write_fragment_imp(buf, begin, size)
}

func (this *HttpRangeFile) do_receive(resp *http.Response, begin, end int64) error {
	buf := make([]byte, _1K)
L:
	for {
		n, e := read_until_full(resp.Body, buf)
		if n > 0 {
			this.write_fragment(buf, begin, int64(n))
			begin += int64(n)
		}
		switch {
		case n == 0 || e == io.EOF:
			//			this.error_and_close(status_done, nil)
			break L
		case e != nil:
			this.error_and_close(status_recv_failed, e)
			return e
		}
	}
	log.Println(`do-receive done`)
	return nil
}

//记录正在工作的下载器
func (this *HttpRangeFile) register_work() int {
	this.lock.Lock()
	defer this.lock.Unlock()
	v := this.workers
	this.workers++
	return v
}
func (this *HttpRangeFile) unregister_work() int {
	this.lock.Lock()
	defer this.lock.Unlock()
	v := this.workers
	this.workers--
	return v
}

/*
Content-Range = "Content-Range" ":" content-range-spec
       content-range-spec      = byte-content-range-spec
       byte-content-range-spec = bytes-unit SP
                                 byte-range-resp-spec "/"
                                 ( instance-length | "*" )
       byte-range-resp-spec = (first-byte-pos "-" last-byte-pos)
                                      | "*"
       instance-length           = 1*DIGIT
*/
func parse_content_range(header string) (first, last, total int64, err error) {
	spec := strings.Fields(header)
	if len(spec) < 2 { // bytes
		err = fmt.Errorf(`invalid header: %v`, header)
		return
	}
	bspec := strings.Split(spec[1], `/`) // xxxx-xxxx/xxxx
	if len(bspec) < 2 {
		err = fmt.Errorf(`invalid header: %v`, spec[1])
		return
	}
	rspec := strings.Split(bspec[0], `-`) // xxxx-xxxxx
	if len(rspec) != 2 {
		err = fmt.Errorf(`invalid header: %v`, bspec[0])
	}
	//total = bspec[1], first = rspec[0], last = rspec[1]
	first, err = strconv.ParseInt(rspec[0], 10, 0)
	if err == nil {
		last, err = strconv.ParseInt(rspec[1], 10, 0)
	}
	if err == nil {
		total, err = strconv.ParseInt(bspec[1], 10, 0)
	}
	return
}

func (this *HttpRangeFile) read_from_fragment(frag_idx int, offset int64, buf []byte) (n int, err error) {
	if frag_idx < 0 || frag_idx >= len(this.fragments) {
		err = fmt.Errorf(`read fragment out of range [%d]/%d`, frag_idx, offset)
		return
	}
	frag := this.fragments[frag_idx]
	frag.Seek(offset, 0)
	return frag.Read(buf)
}
