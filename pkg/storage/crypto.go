package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

const (
	// AtRestKeySize is the AES-256 key size in bytes. Keys come from the
	// ENCRYPTION_KEY or KEY_FILE environment variable, never from source.
	AtRestKeySize = 32
	// atRestNonceSize is the GCM nonce length prepended to each sealed blob.
	atRestNonceSize = 12

	// walEncryptedMagic marks a WAL file whose records are sealed. It is
	// deliberately outside the valid opCode range (1-6) so it can never be
	// confused with a plaintext log's first record.
	walEncryptedMagic byte = 0xF0

	// snapshotMagic prefixes an at-rest snapshot whose JSON payload is
	// sealed. Plaintext snapshots begin with '{'.
	snapshotMagic = "KVES"
)

// ErrDecrypt is returned when sealed data cannot be opened: a wrong key, a
// tampered blob, or corruption.
var ErrDecrypt = errors.New("at-rest decrypt failed: wrong key or corrupt data")

// Encryptor seals and opens byte blobs with a fixed key. It backs at-rest
// encryption for the WAL, local snapshots, and raft state so user data never
// sits on disk in plaintext.
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// NewAtRestCipher returns an AES-256-GCM Encryptor for a 32-byte key. Callers
// that persist secrets (raft state) use the same cipher instance as the store
// so one key covers every on-disk artifact.
func NewAtRestCipher(key []byte) (Encryptor, error) {
	return newAtRestCipher(key)
}

// atRestCipher encrypts and decrypts individual at-rest blobs with AES-256-GCM.
// Every sealed blob is self-describing: a fresh random 12-byte nonce is
// prepended to the ciphertext, so no replay state or stream position is needed.
type atRestCipher struct {
	aead cipher.AEAD
}

// newAtRestCipher builds the cipher from a 32-byte key.
func newAtRestCipher(key []byte) (*atRestCipher, error) {
	if len(key) != AtRestKeySize {
		return nil, fmt.Errorf("encryption key must be %d bytes, got %d", AtRestKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}
	return &atRestCipher{aead: aead}, nil
}

// Encrypt seals plain into nonce||ciphertext||tag.
func (c *atRestCipher) Encrypt(plain []byte) ([]byte, error) {
	nonce := make([]byte, atRestNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("random nonce: %w", err)
	}
	return c.aead.Seal(nonce, nonce, plain, nil), nil
}

// Decrypt opens a blob produced by Encrypt.
func (c *atRestCipher) Decrypt(sealed []byte) ([]byte, error) {
	if len(sealed) < atRestNonceSize {
		return nil, ErrDecrypt
	}
	plain, err := c.aead.Open(nil, sealed[:atRestNonceSize], sealed[atRestNonceSize:], nil)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}
