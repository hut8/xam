// Generated by: main
// TypeWriter: slice
// Directive: +gen on *FileData

package xam

// FileDataSlice is a slice of type *FileData. Use it where you would use []*FileData.
type FileDataSlice []*FileData

// Where returns a new FileDataSlice whose elements return true for func. See: http://clipperhouse.github.io/gen/#Where
func (rcv FileDataSlice) Where(fn func(*FileData) bool) (result FileDataSlice) {
	for _, v := range rcv {
		if fn(v) {
			result = append(result, v)
		}
	}
	return result
}
