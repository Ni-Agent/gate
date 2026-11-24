package core

import "testing"

func TestTemporalMembershipAsOfUsesCurrentLabels(t *testing.T) {
	s := setup(t)
	tagm := tag(t, s, "gate:tg:temporal", "topic", "old label")
	m := create(t, s, "gate:tx:temporal", "content")
	call(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": tagm["id"], "expected_revision": 1, "expected_hash": m["token"]})
	call(t, s, "untag_resource", map[string]any{"resource_id": m["id"], "tag_id": tagm["id"], "expected_revision": 1, "expected_hash": m["token"]})
	if _, e := s.db.Exec("UPDATE associations SET created_at='2026-11-20T12:00:00.000000000Z',deleted_at='2026-11-21T12:00:00.000000000Z' WHERE target_id=?", m["id"]); e != nil {
		t.Fatal(e)
	}
	a := taskExpected(tagm)
	a["value"] = "current label"
	call(t, s, "update_tag", a)
	for _, c := range []struct {
		at string
		n  int
	}{{"2026-11-20T11:59:59Z", 0}, {"2026-11-20T12:00:00Z", 1}, {"2026-11-21T12:00:00Z", 0}} {
		out := call(t, s, "list_tags_for_resource", map[string]any{"id": m["id"], "revision": 1, "as_of": c.at})
		if len(items(out)) != c.n {
			t.Fatal(c, out)
		}
		if c.n == 1 && items(out)[0].(map[string]any)["value"] != "current label" {
			t.Fatal(out)
		}
	}
	bad(t, s, "list_tags_for_resource", map[string]any{"id": m["id"], "as_of": "yesterday"}, 400)
}
