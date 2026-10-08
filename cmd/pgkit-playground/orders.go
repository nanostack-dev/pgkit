package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/nanostack-dev/pgkit/workflow"
)

type order struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Items    []item `json:"items"`
	Express  bool   `json:"express"`
}

type item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Cents    int    `json:"cents"`
}

type shipment struct {
	SKU     string `json:"sku"`
	Carrier string `json:"carrier"`
	Label   string `json:"label"`
}

type fulfilment struct {
	OrderID    string     `json:"order_id"`
	TotalCents int        `json:"total_cents"`
	Approver   string     `json:"approver,omitempty"`
	Shipments  []shipment `json:"shipments"`
}

type approval struct {
	By       string `json:"by"`
	Approved bool   `json:"approved"`
}

// managerApproval is sent to large orders, which wait for it before shipping.
var managerApproval = workflow.NewSignal[approval]("manager-approval")

const largeOrderCents = 50_000

// shipItem books one carrier per line item, as a child run of the order.
var shipItem = workflow.Define("ship-item", func(wf *workflow.Context, line item) (shipment, error) {
	carrier, err := wf.Step("choose-carrier", func(context.Context) (string, error) {
		return []string{"UPS", "DHL", "FedEx"}[rand.IntN(3)], nil
	})
	if err != nil {
		return shipment{}, err
	}
	label, err := wf.Step("book-label", func(ctx context.Context) (string, error) {
		if rand.IntN(4) == 0 {
			return "", errors.New("carrier API timed out")
		}
		return fmt.Sprintf("%s-%s", carrier, workflow.IdempotencyKey(ctx)[len(workflow.IdempotencyKey(ctx))-8:]), nil
	}, workflow.Retry{MaxAttempts: 5, Backoff: 200 * time.Millisecond})
	return shipment{SKU: line.SKU, Carrier: carrier, Label: label}, err
})

// fulfilOrder shows every durable operation: a transactional step, parallel
// lookups, a manager approval with a timeout, a cooling-off sleep and one child run
// per line item.
func fulfilOrder(db *sql.DB) *workflow.Workflow[order, fulfilment] {
	return workflow.Define("fulfil-order", func(wf *workflow.Context, in order) (fulfilment, error) {
		total, err := wf.TxStep("record-order", func(ctx context.Context, tx *sql.Tx) (int, error) {
			cents := 0
			for _, line := range in.Items {
				cents += line.Quantity * line.Cents
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO playground_orders (id, customer, total_cents, status) VALUES ($1, $2, $3, 'received')
				ON CONFLICT (id) DO NOTHING`, in.ID, in.Customer, cents)
			return cents, err
		})
		if err != nil {
			return fulfilment{}, err
		}

		fraud := wf.Async("fraud-check", func(context.Context) (bool, error) {
			time.Sleep(300 * time.Millisecond)
			return true, nil
		})
		stock := wf.Async("stock-check", func(context.Context) (bool, error) {
			time.Sleep(200 * time.Millisecond)
			return true, nil
		})
		if ok, err := fraud.Wait(); err != nil || !ok {
			return fulfilment{}, fmt.Errorf("fraud check: %w", err)
		}
		if ok, err := stock.Wait(); err != nil || !ok {
			return fulfilment{}, fmt.Errorf("stock check: %w", err)
		}

		result := fulfilment{OrderID: in.ID, TotalCents: total}
		if total >= largeOrderCents {
			decision, err := wf.Receive(managerApproval, 10*time.Minute)
			if errors.Is(err, workflow.ErrTimeout) {
				return fulfilment{}, fmt.Errorf("order %s was not approved in time", in.ID)
			}
			if err != nil {
				return fulfilment{}, err
			}
			if !decision.Approved {
				return fulfilment{}, fmt.Errorf("order %s rejected by %s", in.ID, decision.By)
			}
			result.Approver = decision.By
		}

		if !in.Express {
			if err := wf.Sleep("cooling-off", 3*time.Second); err != nil {
				return fulfilment{}, err
			}
		}

		var shipments []*workflow.Future[shipment]
		for _, line := range in.Items {
			shipments = append(shipments, wf.Start("ship", shipItem, line))
		}
		for _, pending := range shipments {
			shipped, err := pending.Wait()
			if err != nil {
				return fulfilment{}, err
			}
			result.Shipments = append(result.Shipments, shipped)
		}

		_, err = wf.TxStep("mark-shipped", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			_, err := tx.ExecContext(ctx, `UPDATE playground_orders SET status = 'shipped' WHERE id = $1`, in.ID)
			return true, err
		})
		return result, err
	})
}

func createPlaygroundTables(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS playground_orders (
		id TEXT PRIMARY KEY,
		customer TEXT NOT NULL,
		total_cents INTEGER NOT NULL,
		status TEXT NOT NULL
	)`)
	return err
}

func sampleOrder(id string, express bool, lines int, cents int) order {
	skus := []string{"desk", "chair", "lamp", "monitor", "keyboard"}
	in := order{ID: id, Customer: "customer-" + id, Express: express}
	for i := range lines {
		in.Items = append(in.Items, item{SKU: skus[i%len(skus)], Quantity: 1 + i%2, Cents: cents})
	}
	return in
}
