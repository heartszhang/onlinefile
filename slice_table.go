package onlinefile

const (
	slice_entry_bits byte = 2 // 2 bits
	slice_entry_mask byte = 1<<slice_entry_bits - 1

	slice_status_unready slice_entry = iota
	slice_status_pending
	slice_status_ready
)

type slice_entry byte
type slice_table interface {
	entry(index int) slice_entry
	update(index int, status slice_entry) slice_entry
	unready_after(startidx, max_count int) (beginidx, endidx int)
	rollback(beginidx, endidx int)
	mark_ready(beginidx, endidx int)
	avail_after(index int) (count int)
	resize(length, slice_size int64) // len is content-length not the slice-table length
}

type online_fd []byte

func new_online_fd(length, slice_size int64) slice_table {
	of := make(online_fd, 0)
	of.resize(length, slice_size)
	return &of
}

func (this *online_fd) resize(length, slice_size int64) {
	slice_count := (length + slice_size - 1) / slice_size
	data_length := int(slice_count+7) * int(slice_entry_bits) / 8

	if data_length == len(*this) {
		return
	}
	v := make(online_fd, data_length)
	copy(v, *this)
	*this = v
}

func (this *online_fd) entry(index int) slice_entry {
	c := 8 / int(slice_entry_bits)
	byte_idx := index / c
	in_byte := byte(index % int(c))
	v := ((*this)[byte_idx] >> (in_byte * slice_entry_bits)) & slice_entry_mask
	return slice_entry(v)
}

func (this *online_fd) avail_after(index int) (count int) {
	for end := len(*this); index < end; index++ {
		s := this.entry(index)
		if s != slice_status_ready {
			break
		}
		count++
	}
	return
}

func (this *online_fd) update(index int, status slice_entry) slice_entry {
	c := 8 / int(slice_entry_bits)
	bidx := index / c
	in_byte := index % c
	v := (*this)[bidx]
	l := status << (byte(in_byte) * slice_entry_bits)
	mask := ^(slice_entry_mask << (byte(in_byte) * slice_entry_bits))
	v = v&mask | byte(l)
	return slice_entry(v)
}

func (this *online_fd) unready_after(start, count int) (begin, end int) {
	begin = start + count     // bint
	c := 8 / slice_entry_bits // byte
	for i := start; i < start+count; i++ {
		in_byte := i % int(c)
		mask := slice_entry_mask << (slice_entry_bits * byte(in_byte))
		idx := i / int(c)
		if (*this)[idx]&mask == 0 {
			begin = i
			break
		}
	}
	end = start + count
	for i := begin; i < start+count; i++ {
		in_byte := i % int(c)
		mask := slice_entry_mask << (slice_entry_bits * byte(in_byte))
		idx := i / int(c)
		if (*this)[idx]&mask != 0 {
			end = i
			break
		}
	}
	for i := begin; i < end; i++ {
		this.update(i, slice_status_pending)
	}
	return
}

func (this *online_fd) rollback(begin, end int) {
	for i := begin; i < end; i++ {
		this.update(i, slice_status_unready)
	}
}

func (this *online_fd) mark_ready(begin, end int) {
	for i := begin; i < end; i++ {
		this.update(i, slice_status_ready)
	}
}
