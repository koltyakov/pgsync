package server

import "testing"

func TestWebProgressHandlerKeepsPerTableIndexes(t *testing.T) {
	s := &Server{syncState: &SyncState{}}
	h := &webProgressHandler{server: s}

	h.OnStart([]string{"users", "orders"})
	h.OnTableStart("users", 0)
	h.OnTableStart("orders", 1)

	idx, total := h.indexForTable("users")
	if idx != 0 || total != 2 {
		t.Fatalf("expected users index 0/2, got %d/%d", idx, total)
	}

	idx, total = h.indexForTable("orders")
	if idx != 1 || total != 2 {
		t.Fatalf("expected orders index 1/2, got %d/%d", idx, total)
	}
}
