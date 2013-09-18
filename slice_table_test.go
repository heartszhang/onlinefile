package onlinefile

import (
	"testing"
)

func TestSliceTable(t *testing.T) {
	fd := new_file_desc(123123, 1024)
	fd.update(1, slice_status_ready)
	if e2 := fd.entry(1); e2 != slice_status_ready {
		t.Errorf(`slice_table.update(%v) = %v`, 0, e2)
	}
	b, end := fd.unready_after(0, 8) // 0,1
	if b != 0 || end != 1 {
		t.Errorf(`slice_table.unready_after(0,8) = %v, %v`, b, end)
	}
	if ab := fd.avail_after(0); ab != 0 {
		t.Errorf(`slice_table.avail_after(0) = %v`, ab)
	}
}
