package core

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// NotificationRetention is a diagnostic lookback, not an audit/delivery SLA.
const NotificationRetention = 24 * time.Hour
const historyTimeFormat = "2006-01-02T15:04:05.000000000Z"

func historyStamp(t time.Time) string { return t.UTC().Format(historyTimeFormat) }

func (s *Store) initNotificationHistory() error {
	if _, err := s.db.Exec("INSERT OR IGNORE INTO notification_history_meta VALUES(1,?)", now()); err != nil {
		return err
	}
	_, err := s.db.Exec("DELETE FROM notification_history WHERE emitted_at<?", historyStamp(time.Now().Add(-NotificationRetention)))
	return err
}

// Called with the writer mutex held, AFTER the resource transaction commits and
// immediately before live-channel handoff. Even with zero subscribers we record
// the invalidation. The separate transaction intentionally leaves a crash gap.
func (s *Store) recordNotification(e Event, at time.Time) error {
	if err := s.inject("notification_history"); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM notification_history WHERE emitted_at<?", historyStamp(at.Add(-NotificationRetention))); err != nil {
		return err
	}
	if _, err = tx.Exec("INSERT INTO notification_history(emitted_at,type,task_id,assignee,previous_assignee) VALUES(?,?,?,?,?)", historyStamp(at), e.Type, e.ID, e.Assignee, e.PreviousAssignee); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) notificationHistory(ctx context.Context, actor string, a map[string]any) (map[string]any, error) {
	if err := known(a, "identity task_id from to after limit"); err != nil {
		return nil, err
	}
	if err := validateStringFields(a, "identity task_id from to after"); err != nil {
		return nil, err
	}
	who := actor
	if v, ok := a["identity"]; ok {
		who = v.(string)
	}
	if err := registered(ctx, s.db, who); err != nil {
		return nil, err
	}
	task := text(a, "task_id")
	if _, ok := a["task_id"]; ok {
		if err := requireID(task, "tk"); err != nil {
			return nil, err
		}
	}
	after, limit, err := pageArgs(a)
	if err != nil {
		return nil, err
	}
	bound := int64(9223372036854775807)
	if after != "" {
		bound, err = strconv.ParseInt(after, 10, 64)
		if err != nil || bound < 1 || strconv.FormatInt(bound, 10) != after {
			return nil, fail(400, "invalid notification history cursor")
		}
	}
	// Serialize lazy maintenance with writes; the query and its bounds describe a
	// single snapshot. No scheduler, task polling or persistent client queue exists.
	s.mu.Lock()
	defer s.mu.Unlock()
	observed := time.Now().UTC()
	cutoff := observed.Add(-NotificationRetention)
	from, to := cutoff, observed
	for key, dest := range map[string]*time.Time{"from": &from, "to": &to} {
		if v, ok := a[key]; ok {
			*dest, err = time.Parse(time.RFC3339Nano, v.(string))
			if err != nil {
				return nil, fail(400, "valid RFC3339 interval required")
			}
		}
	}
	if !from.Before(to) {
		return nil, fail(400, "increasing RFC3339 interval required")
	}
	effectiveFrom := from
	if effectiveFrom.Before(cutoff) {
		effectiveFrom = cutoff
	}
	effectiveTo := to
	if effectiveTo.After(observed) {
		effectiveTo = observed
	}
	if _, err = s.db.ExecContext(ctx, "DELETE FROM notification_history WHERE emitted_at<?", historyStamp(cutoff)); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var started string
	if err = tx.QueryRowContext(ctx, "SELECT started_at FROM notification_history_meta WHERE singleton=1").Scan(&started); err != nil {
		return nil, err
	}
	var oldest, newest sql.NullString
	var count int64
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*),MIN(emitted_at),MAX(emitted_at) FROM notification_history WHERE (assignee=? OR previous_assignee=?)", who, who).Scan(&count, &oldest, &newest); err != nil {
		return nil, err
	}
	sqlq := "SELECT seq,emitted_at,type,task_id,assignee,previous_assignee FROM notification_history WHERE (assignee=? OR previous_assignee=?) AND emitted_at>=? AND emitted_at<? AND seq<?"
	args := []any{who, who, historyStamp(effectiveFrom), historyStamp(effectiveTo), bound}
	if task != "" {
		sqlq += " AND task_id=?"
		args = append(args, task)
	}
	args = append(args, limit+1)
	rows, err := tx.QueryContext(ctx, sqlq+" ORDER BY seq DESC LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []any{}
	for rows.Next() {
		var seq int64
		var at, typ, id, current, previous string
		if err = rows.Scan(&seq, &at, &typ, &id, &current, &previous); err != nil {
			return nil, err
		}
		interested := []string{current}
		if previous != "" && previous != current {
			interested = append(interested, previous)
		}
		entries = append(entries, map[string]any{"cursor": fmt.Sprint(seq), "emitted_at": at, "type": typ, "task_id": id, "assignee": current, "previous_assignee": previous, "interested_identities": interested})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	out := page(entries, limit, func(v any) string { return v.(map[string]any)["cursor"].(string) })
	var oldestAt, newestAt, lastFailure any
	if oldest.Valid {
		oldestAt = oldest.String
	}
	if newest.Valid {
		newestAt = newest.String
	}
	if s.historyLastFailure != "" {
		lastFailure = s.historyLastFailure
	}
	out["diagnostics"] = map[string]any{
		"identity": who, "observed_at": historyStamp(observed), "retention_seconds": int64(NotificationRetention / time.Second),
		"retained_from": historyStamp(cutoff), "history_started_at": started,
		"requested_from": historyStamp(from), "requested_to": historyStamp(to),
		"effective_from": historyStamp(effectiveFrom), "effective_to": historyStamp(effectiveTo),
		"oldest_event_at": oldestAt, "newest_event_at": newestAt, "retained_event_count": count,
		"bounds_scope":                           "identity, before task/time/cursor filters",
		"recording_failures_since_process_start": s.historyFailures, "last_recording_failure_at": lastFailure,
		"process_started_at": s.historyProcessStarted, "exhaustive": false, "delivery_receipt": false, "replay_supported": false,
		"meaning": "post-commit invalidation handoff attempt, not socket write or receipt; absence may reflect expiry, pre-history writes, crash or recording failure",
	}
	return out, nil
}
