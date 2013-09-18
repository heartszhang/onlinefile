package onlinefile

import (
	"fmt"
	"io"
	//	"log"
	"net/http"
	"strconv"
	"sync"
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
}

const (
	_16K = 16384
	_1M  = 1024 * 1024
	_1K  = 1024
	_1B  = 256

	status_recv_200 = 1 << iota
	status_recv_206

	status_failed_0 = -iota
	status_closed
	status_failed_invalidrange
	status_failed_badrequest
	status_failed_statuscode
	status_recv_failed

	slice8 = 8 //
)

func NewHttpRangeFile(uri string) OnlineFile {
	l := &sync.Mutex{}
	v := &HttpRangeFile{
		uri:        uri,
		packets:    make(chan *packet, _1B),
		slices:     new_online_fd(_16K, _1K),
		slice_size: _1K,
		lock:       l,
		update:     sync.NewCond(l)}
	go v.work()
	return v
}

const (
	unsupported_status    = `unsupported status`
	max_cocurrent_workers = 2 // enable 2 workers cocurrent
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
	//没有需要下载的内容
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
	this.Close()
	this.update.Broadcast()
}

//不需要等待writer和worker结束
func (this *HttpRangeFile) Close() error {
	this.status = status_closed
	this.packets <- nil
	return nil
}

//文件初始长度和真实长度不一致，下载过程中，如果服务器回送的文件长度发生变化
//都需要重新设定文件长度，但是文件数据不会被清理。这有可能导致文件内容错误
func (this *HttpRangeFile) try_reset_length(len int64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len == this.length || len < 0 {
		return
	}
	this.length = len
	this.slices.resize(len, this.slice_size)
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

func (this *HttpRangeFile) Read([]byte) (n int, err error) {
	//	c := this.slices.avail_after(this.read_pointer)
	//	c := avail()
	this.update.L.Lock()
	for !this.readable() {
		this.update.Wait()
	}
	defer this.update.L.Unlock()
	// c := avail()	// file bytes
	// to be implemented
	return 0, nil
}

func (this *HttpRangeFile) Seek(offset int64, whence int) (ret int64, err error) {
	return 0, nil
}

func (this *HttpRangeFile) readable() bool {
	rpidx := int(this.read_pointer / this.slice_size)
	c := this.slices.avail_after(rpidx)
	return c > 0 || this.status < 0
}

//分片下载过程
func (this *HttpRangeFile) work() {
	var workers = this.register_work()
	defer this.unregister_work()
	if workers > max_cocurrent_workers { // too many workers
		return
	}
	//发生错误后，unread分配不出来需要下载的分片
	//begin, end是分片下标
	begin, end := this.unready(slice8)
	if begin == end {
		return
	}
	req, _, _, _ := this.new_request(begin, end)
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
		l, e := strconv.ParseInt(resp.Header.Get(`Content-Length`), 10, 0)
		if e == nil {
			this.try_reset_length(l)
		}
		this.do_receive(resp, 0, this.length)
	case http.StatusPartialContent:
		if this.unaccept_206() {
			return
		}
		//如果first位置和我们请求的并不一致，没有按照分片进行对齐，程序处理不了这种情况
		first, last, total := parse_content_range(resp.Header.Get(`Content-Range`))
		this.try_reset_length(total)
		err = this.do_receive(resp, first, last+1)
		if err == nil {
			go this.work()
		}
	default:
		err = fmt.Errorf(`%v: %v`, resp.StatusCode, unsupported_status)
		this.error_and_close(status_failed_statuscode, err)
	}
}

func parse_content_range(header string) (first, last, total int64) {
	return 0, 0, 0
}

//读满缓冲区或者EOF或者出现错误
func read_until_full(reader io.Reader, buf []byte) (int, error) {
	offset := 0
	for {
		x, e := reader.Read(buf[offset:])
		offset += x
		if offset >= len(buf) {
			return offset, e
		}
		if e != nil || x == 0 {
			return offset, e
		}
	}
}

func (this *HttpRangeFile) write_fragment_imp(buf []byte, begin, size int64) {
	if begin%this.slice_size != 0 {
		// write a log
		return
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

func (this *HttpRangeFile) write_fragment(buf []byte, begin, count int64) {
	p := &packet{offset: begin}
	p.data = make([]byte, count)
	copy(p.data, buf)
	this.packets <- p
}

const ()

func (this *HttpRangeFile) do_receive(resp *http.Response, begin, end int64) error {
	buf := make([]byte, _1K)
	for {
		n, e := read_until_full(resp.Body, buf) // resp.Body.Read(buf)
		if n > 0 {
			this.write_fragment(buf, begin, int64(n))
			begin += int64(n)
		}
		switch {
		case n == 0 || e == io.EOF:
			break
		case e != nil:
			this.error_and_close(status_recv_failed, e)
			return e
		}
	}
	return nil
}
