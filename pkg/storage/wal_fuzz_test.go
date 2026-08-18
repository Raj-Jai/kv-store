package storage

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// decodeWALPrefix is the independent oracle for FuzzWALReplay. It parses the
// same wire format with its own bounds-checked decoder, applying every fully
// decodable record in order and stopping at the first malformed one. It shares
// no code with the production Replay path, so it cannot reproduce the bugs it
// is meant to detect.
//
// now is the clock the fuzz body pins on both sides, so the oracle and Replay
// agree on whether an entry carrying a deadline is expired.
func decodeWALPrefix(data []byte, now int64) map[string]entry {
	state := make(map[string]entry)
	off := 0
	for off < len(data) {
		op := data[off]
		off++
		switch opCode(op) {
		case opPut:
			key, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			value, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			state[key] = entry{Value: value}
		case opDelete:
			key, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			delete(state, key)
		case opClear:
			clear(state)
		case opIncr:
			key, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			e, exists := state[key]
			if exists && e.Exp != 0 && now >= e.Exp {
				delete(state, key)
				exists = false
			}
			if !exists {
				state[key] = entry{Value: "1"}
				continue
			}
			v, err := strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				continue // non-numeric: deterministic no-op, like lenient replay
			}
			if v == math.MaxInt64 {
				continue // overflow: failed at apply, replay is a no-op
			}
			state[key] = entry{Value: strconv.FormatInt(v+1, 10)}
		case opCAS:
			key, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			old, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			new, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			e, exists := state[key]
			if exists && e.Exp != 0 && now >= e.Exp {
				delete(state, key)
				exists = false
			}
			if !exists || e.Value != old {
				continue // absent or mismatch: deterministic no-op
			}
			state[key] = entry{Value: new}
		case opExpire:
			key, n, ok := readLenField(data, off)
			if !ok {
				return state
			}
			off += n
			if off+8 > len(data) {
				return state
			}
			ts := int64(binary.BigEndian.Uint64(data[off : off+8]))
			off += 8
			e, exists := state[key]
			if exists && e.Exp != 0 && now >= e.Exp {
				delete(state, key)
				exists = false
			}
			if !exists {
				continue // absent: deterministic no-op
			}
			state[key] = entry{Value: e.Value, Exp: ts}
		default:
			return state
		}
	}
	return state
}

// readLenField reads a BigEndian length-prefixed string from data at off,
// returning it plus the number of bytes consumed. ok is false when the header
// or payload runs past the end of data.
func readLenField(data []byte, off int) (string, int, bool) {
	if off+4 > len(data) {
		return "", 0, false
	}
	length := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if off+length > len(data) {
		return "", 0, false
	}
	return string(data[off : off+length]), 4 + length, true
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
		{byte(opIncr), 1, 0, 0, 0, 'k'},
		{byte(opCAS), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b'},
		{byte(opExpire), 1, 0, 0, 0, 'k', 0, 0, 0, 0, 0, 0, 0, 1},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, '5', byte(opIncr), 1, 0, 0, 0, 'k'},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'a', byte(opCAS), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b'},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'v', byte(opExpire), 1, 0, 0, 0, 'k', 0, 0, 0, 0, 0, 0, 0, 1},
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
		// Pin the clock so both sides agree on expired-ness for entries that
		// carry a deadline, making the prefix comparison exact.
		m := NewMemStore()
		m.now = func() time.Time { return time.Unix(0, 0) }
		replayErr := wal.Replay(m)
		wal.Close()

		if replayErr != nil {
			// Clean failure: the store refuses to open this WAL. The prefix
			// applied before the corruption must still match the oracle.
		}
		if want := decodeWALPrefix(data, 0); !reflect.DeepEqual(m.data, want) {
			t.Fatalf("replayed state diverged from decoded prefix:\n got: %v\nwant: %v", m.data, want)
		}
	})
}
