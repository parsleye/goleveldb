package table

import (
	"fmt"
)

type Stats struct {
	Open          int64
	OpenUse       float64
	WriteDataUse  float64
	WriteIndexUse float64
	ReadDataUse   float64
	ReadIndexUse  float64
}

// not atomic
func (s *Stats) String() string {
	return fmt.Sprintf("open table num %d, open table use %.2fs, write data use %.2fs, write index use %.2fs, read data use %.2fs, read index use %.2fs",
		s.Open, s.OpenUse, s.WriteDataUse, s.WriteIndexUse, s.ReadDataUse, s.ReadIndexUse)
}
