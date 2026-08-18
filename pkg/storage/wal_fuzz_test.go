package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// decodeWALPrefix is the independent oracle for FuzzWALReplay. It parses the
// same wire format with its own bounds-checked decoder, applying every fully
// decodable record in order and stopping at the first malformed one. It shares
// no code with the production Replay path, so it cannot reproduce the bugs it
// is meant to detect.
func decodeWALPrefix(data []byte) map[string]string {
	state := make(map[string]string)
	off := 0
	for off < len(data) {
		op := data[off]
		off++
		switch opCode(op) {
		case opPut:
			if off+4 > len(data) {
				return state
			}
			klen := int(binary.BigEndian.Uint32(data[off : off+4]))
			off += 4
			if off+klen+4 > len(data) {
				return state
			}
			key := string(data[off : off+klen])
			off += klen
			vlen := int(binary.BigEndian.Uint32(data[off : off+4]))
			off += 4
			if off+vlen > len(data) {
				return state
			}
			state[key] = string(data[off : off+vlen])
			off += vlen
		case opDelete:
			if off+4 > len(data) {
				return state
			}
			klen := int(binary.BigEndian.Uint32(data[off : off+4]))
			off += 4
			if off+klen > len(data) {
				return state
			}
			delete(state, string(data[off:off+klen]))
			off += klen
		case opClear:
			clear(state)
		default:
			return state
		}
	}
	return state
}

// FuzzWALReplay feeds arbitrary bytes as a WAL file. Corrupt, truncated, or
// unknown-opcode input must fail cleanly (an error, never a panic or hang), and
// whatever prefix was decoded before the corruption must replay to exactly the
// state implied by the fully-decodable records.
func FuzzWALReplay(f *testing.F) {
	seeds := [][]byte{
		{},
		{byte(opClear)},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'v'},
		{byte(opDelete), 1, 0, 0, 0, 'k'},
		{byte(opClear), byte(opPut), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', byte(opDelete), 1, 0, 0, 0, 'a'},
		{0},                          // unknown opcode
		{0xFF},                       // unknown opcode
		{byte(opPut), 0, 0, 0, 0x10}, // truncated key
		{byte(opPut), 0xFF, 0xFF, 0xFF, 0xFF, 1, 0, 0, 0, 'v'}, // absurd key length
		{byte(opPut), 1, 0, 0, 0, 'k', 0xFF, 0xFF, 0xFF, 0xFF}, // absurd value length
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}

		wal, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		m := NewMemStore()
		replayErr := wal.Replay(m)
		wal.Close()

		if replayErr != nil {
			// Clean failure: the store refuses to open this WAL. The prefix
			// applied before the corruption must still match the oracle.
		}
		if want := decodeWALPrefix(data); !reflect.DeepEqual(m.data, want) {
			t.Fatalf("replayed state diverged from decoded prefix:\n got: %v\nwant: %v", m.data, want)
		}
	})
}
