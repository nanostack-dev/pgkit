package adminui

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
)

const advisoryLocksInDatabase = `l.locktype = 'advisory'
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())`

type advisoryLockSession struct {
	PID             int64   `json:"pid"`
	Mode            string  `json:"mode"`
	Granted         bool    `json:"granted"`
	Key             *string `json:"key"`
	ClassID         int64   `json:"classid"`
	ObjectID        int64   `json:"objid"`
	ObjectSubID     int64   `json:"objsubid"`
	ApplicationName string  `json:"application_name"`
	State           *string `json:"state"`
	XactStart       *string `json:"xact_start"`
	StateChange     *string `json:"state_change"`
	WaitEventType   *string `json:"wait_event_type"`
	WaitEvent       *string `json:"wait_event"`
}

type advisoryLocksResponse struct {
	Now   string                `json:"now"`
	Items []advisoryLockSession `json:"items"`
}

func (u *UI) handleLocks(w http.ResponseWriter, r *http.Request) {
	var response advisoryLocksResponse
	err := u.withSnapshot(r.Context(), func(q querier) error {
		now, err := queryNow(r.Context(), q)
		if err != nil {
			return err
		}
		items, err := queryAdvisoryLockSessions(r.Context(), q)
		if err != nil {
			return err
		}
		response = advisoryLocksResponse{Now: now, Items: items}
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func queryAdvisoryLockCount(ctx context.Context, q querier) (int, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks l WHERE `+advisoryLocksInDatabase).Scan(&count); err != nil {
		return 0, fmt.Errorf("adminui: advisory lock count: %w", err)
	}
	return count, nil
}

func queryAdvisoryLockSessions(ctx context.Context, q querier) ([]advisoryLockSession, error) {
	rows, err := q.QueryContext(ctx, `
SELECT COALESCE(l.pid, 0), l.mode, l.granted,
       CASE l.objsubid
           WHEN 1 THEN ((l.classid::bigint << 32) | l.objid::bigint)::text
           WHEN 2 THEN l.classid::text || ',' || l.objid::text
       END,
       l.classid::bigint, l.objid::bigint, l.objsubid,
       COALESCE(a.application_name, ''), a.state, a.xact_start, a.state_change, a.wait_event_type, a.wait_event
FROM pg_locks l
LEFT JOIN pg_stat_activity a ON a.pid = l.pid
WHERE `+advisoryLocksInDatabase+`
ORDER BY l.granted DESC, l.pid, l.classid, l.objid, l.objsubid`)
	if err != nil {
		return nil, fmt.Errorf("adminui: advisory locks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	items := []advisoryLockSession{}
	for rows.Next() {
		var (
			item                            advisoryLockSession
			key, state, waitType, waitEvent sql.NullString
			xactStart, stateChange          sql.NullTime
		)
		if err := rows.Scan(&item.PID, &item.Mode, &item.Granted, &key, &item.ClassID, &item.ObjectID, &item.ObjectSubID,
			&item.ApplicationName, &state, &xactStart, &stateChange, &waitType, &waitEvent); err != nil {
			return nil, fmt.Errorf("adminui: scan advisory lock: %w", err)
		}
		item.Key = nullableText(key)
		item.State = nullableText(state)
		item.XactStart = formatNullTime(xactStart)
		item.StateChange = formatNullTime(stateChange)
		item.WaitEventType = nullableText(waitType)
		item.WaitEvent = nullableText(waitEvent)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate advisory locks: %w", err)
	}
	return items, nil
}
