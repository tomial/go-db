package btree

type leafCell struct {
	key  key
	data []byte
}

// leaf node
type LeafNode struct {
	nodeHeader
	Data []byte
}

func maxLeafNodeNumCell(ln *LeafNode) uint32 {
	return nodeBodySize() / ln.CellSize
}
