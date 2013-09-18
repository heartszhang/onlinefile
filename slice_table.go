package onlinefile

import ()

import ()

const (
	slice_entry_bits byte = 2 // 2 bits
	slice_entry_mask byte = 1<<slice_entry_bits - 1
)
const (
	slice_status_unready slice_entry = iota
	slice_status_pending
	slice_status_ready

	byte_bits = 8
)

type slice_entry byte // 00 00 00 xx , only the lowest two bits be used

type slice_table interface {
	entry(index int) slice_entry
	update(index int, status slice_entry) slice_entry
	unready_after(startidx, max_count int) (beginidx, endidx int) // will update [begin,end) to pending
	reset_unready(beginidx, endidx int)                           // reset [begin,end) to unready
	mark_ready(beginidx, endidx int)
	avail_after(index int) (count int) //continuous slices from [index...
	resize(length, slice_size int64)   // len is content-length not the slice-table length
}

type file_desc []byte // implements interface slice_table

func new_file_desc(length, slice_size int64) slice_table {
	of := make(file_desc, 0)
	of.resize(length, slice_size)
	return &of
}

func (this *file_desc) resize(length, slice_size int64) {
	slice_count := (length + slice_size - 1) / slice_size
	data_length := int(slice_count+byte_bits-1) / byte_bits * int(slice_entry_bits)

	if data_length == len(*this) {
		return
	}
	v := make(file_desc, data_length)
	copy(v, *this)
	*this = v
}

func (this *file_desc) entry(index int) slice_entry {
	c := byte_bits / int(slice_entry_bits)
	byte_idx := index / c
	in_byte := byte(index%int(c)) * slice_entry_bits
	v := ((*this)[byte_idx] >> in_byte) & slice_entry_mask
	return slice_entry(v)
}

func (this *file_desc) avail_after(index int) (count int) {
	for end := len(*this); index < end; index++ {
		s := this.entry(index)
		if s != slice_status_ready {
			break
		}
		count++
	}
	return
}

func (this *file_desc) update(index int, status slice_entry) slice_entry {
	c := byte_bits / int(slice_entry_bits)
	bidx := index / c
	in_byte := index % c
	v := (*this)[bidx]
	l := status << (byte(in_byte) * slice_entry_bits)
	mask := ^(slice_entry_mask << (byte(in_byte) * slice_entry_bits))
	v = v&mask | byte(l)
	(*this)[bidx] = v
	return status
}
func mini(lhs, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}
func (this *file_desc) unready_after(start, count int) (begin, end int) {
	begin = start + count             // bint
	c := byte_bits / slice_entry_bits // byte
	for i := start; i < start+count; i++ {
		in_byte := i % int(c)
		mask := slice_entry_mask << (slice_entry_bits * byte(in_byte))
		idx := i / int(c)
		if (*this)[idx]&mask == 0 {
			begin = i
			break
		}
	}
	end = begin + count
	b := mini(begin+count, len(*this))
	for i := begin; i < b; i++ {
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

func (this *file_desc) reset_unready(begin, end int) {
	for i := begin; i < end; i++ {
		this.update(i, slice_status_unready)
	}
}

func (this *file_desc) mark_ready(begin, end int) {
	for i := begin; i < end; i++ {
		this.update(i, slice_status_ready)
	}
}
