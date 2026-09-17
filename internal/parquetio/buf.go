package parquetio

import "bytes"

// cloneBuf copies buf's current contents out into a freshly-allocated slice. Every caller in this
// package reuses one bytes.Buffer as write scratch across many calls, so the bytes have to be
// copied out before returning: the next call resets and overwrites buf while a dispatch worker may
// still hold whatever this call returned.
func cloneBuf(buf *bytes.Buffer) []byte {
	doc := make([]byte, buf.Len())
	copy(doc, buf.Bytes())
	return doc
}
