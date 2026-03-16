package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nanostack-dev/pgkit/adminui"
	pgkitfx "github.com/nanostack-dev/pgkit/fx"
	adminuifx "github.com/nanostack-dev/pgkit/fx/adminui"
	queuefx "github.com/nanostack-dev/pgkit/fx/queue"
	workflowfx "github.com/nanostack-dev/pgkit/fx/workflow"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/fx"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type playgroundOrderInput struct {
	OrderID         string                    `json:"order_id"`
	Currency        string                    `json:"currency"`
	Priority        string                    `json:"priority"`
	RequestedShipBy string                    `json:"requested_ship_by"`
	Customer        playgroundCustomer        `json:"customer"`
	Items           []playgroundOrderItem     `json:"items"`
	Webhooks        []playgroundWebhookTarget `json:"webhooks"`
}

type playgroundCustomer struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Tier   string `json:"tier"`
	Region string `json:"region"`
}

type playgroundOrderItem struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	Warehouse  string `json:"warehouse"`
	PriceCents int    `json:"price_cents"`
}

type playgroundShipmentPlan struct {
	ShipmentID string `json:"shipment_id"`
	Warehouse  string `json:"warehouse"`
	ItemCount  int    `json:"item_count"`
	Quantity   int    `json:"quantity"`
	Priority   string `json:"priority"`
	Mode       string `json:"mode"`
}

type playgroundCarrierBooking struct {
	ShipmentID string `json:"shipment_id"`
	Carrier    string `json:"carrier"`
	Service    string `json:"service"`
	ETA        string `json:"eta"`
}

type playgroundWebhookTarget struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	Event string `json:"event"`
}

type playgroundWebhookDispatch struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Event     string `json:"event"`
	Template  string `json:"template"`
	Audience  string `json:"audience"`
	BatchSize int    `json:"batch_size"`
}

type playgroundPricing struct {
	SubtotalCents int      `json:"subtotal_cents"`
	DiscountCents int      `json:"discount_cents"`
	NetCents      int      `json:"net_cents"`
	AppliedPromos []string `json:"applied_promos"`
}

type playgroundTax struct {
	TaxCents int    `json:"tax_cents"`
	Region   string `json:"region"`
}

type playgroundPayment struct {
	AuthorizationID string `json:"authorization_id"`
	CapturedCents   int    `json:"captured_cents"`
}

type playgroundShipmentArtifact struct {
	ShipmentID   string `json:"shipment_id"`
	PackageCount int    `json:"package_count"`
	Lane         string `json:"lane"`
}

type playgroundAllocation struct {
	ShipmentID   string `json:"shipment_id"`
	AllocationID string `json:"allocation_id"`
	ReservedQty  int    `json:"reserved_qty"`
}

type playgroundPicklist struct {
	ShipmentID  string `json:"shipment_id"`
	PicklistID  string `json:"picklist_id"`
	Destination string `json:"destination"`
}

type playgroundInvoice struct {
	InvoiceID  string `json:"invoice_id"`
	TotalCents int    `json:"total_cents"`
}

type playgroundCompletion struct {
	OrderID        string `json:"order_id"`
	FinalStatus    string `json:"final_status"`
	CompletedStage string `json:"completed_stage"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	token := strings.TrimSpace(os.Getenv("PGKIT_DASHBOARD_TOKEN"))
	if token == "" {
		token = "change-me"
	}
	addr := strings.TrimSpace(os.Getenv("PGKIT_PLAYGROUND_ADDR"))
	if addr == "" {
		addr = "127.0.0.1:18081"
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	app := fx.New(
		fx.Provide(func() (testcontainers.Container, string, error) {
			pg, err := postgres.Run(
				context.Background(),
				"postgres:16-alpine",
				postgres.WithDatabase("pgkit_test"),
				postgres.WithUsername("pgkit"),
				postgres.WithPassword("pgkit"),
			)
			if err != nil {
				return nil, "", err
			}
			connString, err := pg.ConnectionString(context.Background(), "sslmode=disable")
			if err != nil {
				_ = pg.Terminate(context.Background())
				return nil, "", err
			}
			return pg, connString, nil
		}),
		fx.Provide(func(container testcontainers.Container, dsn string) (*sql.DB, error) {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				return nil, err
			}
			if err := waitForPing(context.Background(), db, 20*time.Second); err != nil {
				_ = db.Close()
				return nil, err
			}
			_ = container
			return db, nil
		}),
		fx.Provide(fx.Annotate(func() *workflow.Definition {
			return mustDefinePlaygroundWorkflow()
		}, fx.ResultTags(`group:"pgkit.workflow.definitions"`))),
		pgkitfx.All(pgkitfx.Options{
			Queue: queuefx.Options{EnsureSchema: true},
			Workflow: workflowfx.Options{
				EnsureSchema: true,
				StartWorker:  true,
				WorkerConfig: workflow.WorkerConfig{PollInterval: 100 * time.Millisecond, ReapInterval: 5 * time.Second},
			},
			AdminUI: adminuifx.Options{
				StartServer: true,
				Addr:        addr,
				UIOptions:   adminui.Options{Token: token},
			},
		}),
		fx.Invoke(func(lc fx.Lifecycle, container testcontainers.Container, db *sql.DB, queue *qpkg.Client, module *workflow.Module) error {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					if err := ensurePlaygroundTables(ctx, db); err != nil {
						return err
					}
					if _, err := module.Publish(ctx, playgroundWorkflowName); err != nil {
						return err
					}
					if err := module.Activate(ctx, playgroundWorkflowName, 1); err != nil {
						return err
					}
					seedRuns, err := seedPlaygroundRuns(ctx, queue, module)
					if err != nil {
						return err
					}
					logger.Info("pgkit playground ready", "addr", addr, "workflow", playgroundWorkflowName, "run_ids", seedRuns)
					return nil
				},
				OnStop: func(context.Context) error {
					_ = db.Close()
					return container.Terminate(context.Background())
				},
			})
			return nil
		}),
	)

	startCtx, startCancel := context.WithTimeout(ctx, 60*time.Second)
	defer startCancel()
	if err := app.Start(startCtx); err != nil {
		panic(fmt.Errorf("start app: %w", err))
	}
	<-ctx.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	_ = app.Stop(stopCtx)
}

func waitForPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}

	return lastErr
}

const playgroundWorkflowName = "playground-order-orchestra"

func mustDefinePlaygroundWorkflow() *workflow.Definition {
	def, err := workflow.Define(playgroundWorkflowName, func(b *workflow.Builder) {
		b.Title("Global Order Orchestration")
		b.Description("A dense playground workflow that exercises standard steps, transactional steps, fan-out stages, transactional fan-out stages, retries, and wide dependency joins.")
		b.Metadata(map[string]any{"domain": "playground", "shape": "large-dag", "nodes": 32})

		b.Step("ingest-order", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"order_id":        input.OrderID,
				"line_count":      len(input.Items),
				"total_quantity":  totalQuantity(input.Items),
				"webhook_targets": len(input.Webhooks),
			}, nil
		}, workflow.StepOptions{Label: "Ingest Order", QueueName: "playground.intake", Metadata: map[string]any{"lane": "intake"}})

		b.Step("validate-request", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			if input.OrderID == "" {
				return nil, workflow.NonRetryable(fmt.Errorf("missing order_id"))
			}
			if len(input.Items) == 0 {
				return nil, workflow.NonRetryable(fmt.Errorf("order requires at least one item"))
			}
			if input.Customer.Email == "" {
				return nil, workflow.NonRetryable(fmt.Errorf("customer email is required"))
			}
			return map[string]any{"validated": true, "priority": strings.ToUpper(input.Priority)}, nil
		}, workflow.StepOptions{Label: "Validate Request", QueueName: "playground.intake", Metadata: map[string]any{"lane": "intake"}})

		b.Step("normalize-customer", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			segment := "standard"
			if input.Customer.Tier == "platinum" {
				segment = "vip"
			}
			return map[string]any{
				"customer_id": input.Customer.ID,
				"segment":     segment,
				"region":      input.Customer.Region,
				"tier":        input.Customer.Tier,
			}, nil
		}, workflow.StepOptions{Label: "Normalize Customer", QueueName: "playground.intake", DependsOn: []string{"validate-request"}, Metadata: map[string]any{"lane": "intake"}})

		b.TxStep("reserve-idempotency", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			if err := upsertPlaygroundOrder(ctx, tx, input.OrderID, step.RunID, input.Customer.Email, "accepted", input); err != nil {
				return nil, err
			}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "idempotency-reserved", map[string]any{"key": "idem-" + input.OrderID}); err != nil {
				return nil, err
			}
			return map[string]any{"reservation_key": "idem-" + input.OrderID}, nil
		}, workflow.StepOptions{Label: "Reserve Idempotency (TX)", QueueName: "playground.ledger", DependsOn: []string{"ingest-order"}, Metadata: map[string]any{"lane": "ledger"}})

		b.TxStep("snapshot-cart", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			if err := upsertPlaygroundOrder(ctx, tx, input.OrderID, step.RunID, input.Customer.Email, "snapshotted", input); err != nil {
				return nil, err
			}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "snapshot-written", map[string]any{"line_count": len(input.Items)}); err != nil {
				return nil, err
			}
			return map[string]any{"snapshot_version": 1, "line_count": len(input.Items)}, nil
		}, workflow.StepOptions{Label: "Snapshot Cart (TX)", QueueName: "playground.ledger", DependsOn: []string{"ingest-order"}, Metadata: map[string]any{"lane": "ledger"}})

		b.Step("hydrate-catalog", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			return map[string]any{"unique_skus": uniqueSKUCount(input.Items), "warehouses": uniqueWarehouses(input.Items)}, nil
		}, workflow.StepOptions{Label: "Hydrate Catalog", QueueName: "playground.catalog", DependsOn: []string{"validate-request"}, Metadata: map[string]any{"lane": "catalog"}})

		b.Step("check-inventory", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"reserved_quantity": totalQuantity(input.Items),
				"warehouse_count":   len(uniqueWarehouses(input.Items)),
				"allocation_mode":   "warehouse-split",
			}, nil
		}, workflow.StepOptions{Label: "Check Inventory", QueueName: "playground.inventory", DependsOn: []string{"hydrate-catalog", "ingest-order"}, Metadata: map[string]any{"lane": "inventory"}})

		b.Step("compute-promotions", func(_ context.Context, step workflow.StepContext) (any, error) {
			customer, err := stepOutput[map[string]any](step, "normalize-customer")
			if err != nil {
				return nil, err
			}
			promos := []string{"seasonal-bundle"}
			if customer["tier"] == "platinum" {
				promos = append(promos, "vip-credit")
			}
			return map[string]any{"applied_promos": promos, "promo_count": len(promos)}, nil
		}, workflow.StepOptions{Label: "Compute Promotions", QueueName: "playground.pricing", DependsOn: []string{"normalize-customer", "ingest-order"}, Metadata: map[string]any{"lane": "pricing"}})

		b.Step("compute-pricing", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			promotions, err := stepOutput[struct {
				AppliedPromos []string `json:"applied_promos"`
			}](step, "compute-promotions")
			if err != nil {
				return nil, err
			}
			subtotal := totalPriceCents(input.Items)
			discount := 1800 + len(promotions.AppliedPromos)*350
			return playgroundPricing{
				SubtotalCents: subtotal,
				DiscountCents: discount,
				NetCents:      subtotal - discount,
				AppliedPromos: promotions.AppliedPromos,
			}, nil
		}, workflow.StepOptions{Label: "Compute Pricing", QueueName: "playground.pricing", DependsOn: []string{"compute-promotions", "hydrate-catalog", "ingest-order"}, Metadata: map[string]any{"lane": "pricing"}})

		b.TxStep("compute-tax-ledger", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			pricing, err := stepOutput[playgroundPricing](step, "compute-pricing")
			if err != nil {
				return nil, err
			}
			customer, err := stepOutput[struct {
				Region string `json:"region"`
			}](step, "normalize-customer")
			if err != nil {
				return nil, err
			}
			tax := playgroundTax{TaxCents: pricing.NetCents / 12, Region: customer.Region}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "tax-ledger-written", tax); err != nil {
				return nil, err
			}
			return tax, nil
		}, workflow.StepOptions{Label: "Compute Tax Ledger (TX)", QueueName: "playground.ledger", DependsOn: []string{"compute-pricing", "normalize-customer"}, Metadata: map[string]any{"lane": "ledger"}})

		b.Step("risk-profile", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			score := 12 + len(input.Webhooks)*3 + totalQuantity(input.Items)
			if input.Customer.Region != "US" {
				score += 8
			}
			return map[string]any{"score": score, "review_band": "green"}, nil
		}, workflow.StepOptions{Label: "Risk Profile", QueueName: "playground.risk", DependsOn: []string{"normalize-customer", "ingest-order"}, Metadata: map[string]any{"lane": "risk"}})

		b.Step("fraud-screen", func(_ context.Context, step workflow.StepContext) (any, error) {
			if step.Attempt == 1 {
				return nil, fmt.Errorf("fraud gateway timeout on first pass")
			}
			risk, err := stepOutput[struct {
				Score int `json:"score"`
			}](step, "risk-profile")
			if err != nil {
				return nil, err
			}
			return map[string]any{"approved": true, "risk_score": risk.Score, "reviewed_on_attempt": step.Attempt}, nil
		}, workflow.StepOptions{Label: "Fraud Screen", QueueName: "playground.risk", DependsOn: []string{"risk-profile", "validate-request"}, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3, BackoffBase: 150 * time.Millisecond, BackoffMax: 500 * time.Millisecond}, Metadata: map[string]any{"lane": "risk"}})

		b.Step("authorize-payment", func(_ context.Context, step workflow.StepContext) (any, error) {
			pricing, err := stepOutput[playgroundPricing](step, "compute-pricing")
			if err != nil {
				return nil, err
			}
			tax, err := stepOutput[playgroundTax](step, "compute-tax-ledger")
			if err != nil {
				return nil, err
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			return playgroundPayment{
				AuthorizationID: "auth-" + input.OrderID,
				CapturedCents:   pricing.NetCents + tax.TaxCents,
			}, nil
		}, workflow.StepOptions{Label: "Authorize Payment", QueueName: "playground.payments", DependsOn: []string{"fraud-screen", "compute-pricing", "compute-tax-ledger"}, Metadata: map[string]any{"lane": "payments"}})

		b.Step("choose-fulfillment", func(_ context.Context, step workflow.StepContext) (any, error) {
			inventory, err := stepOutput[map[string]any](step, "check-inventory")
			if err != nil {
				return nil, err
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"strategy":        "split-by-warehouse",
				"warehouse_count": inventory["warehouse_count"],
				"priority":        input.Priority,
			}, nil
		}, workflow.StepOptions{Label: "Choose Fulfillment", QueueName: "playground.fulfillment", DependsOn: []string{"check-inventory", "authorize-payment"}, Metadata: map[string]any{"lane": "fulfillment"}})

		b.ForEach("shard-shipments", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			shipments := buildShipmentPlans(input)
			items := make([]any, 0, len(shipments))
			for _, shipment := range shipments {
				items = append(items, shipment)
			}
			return items, nil
		}, func(_ context.Context, step workflow.StepContext) (any, error) {
			shipment, err := decodeInput[playgroundShipmentPlan](step)
			if err != nil {
				return nil, err
			}
			return playgroundShipmentArtifact{ShipmentID: shipment.ShipmentID, PackageCount: shipment.ItemCount, Lane: shipment.Mode}, nil
		}, workflow.StepOptions{Label: "Shard Shipments", QueueName: "playground.fulfillment", DependsOn: []string{"choose-fulfillment", "ingest-order"}, Metadata: map[string]any{"lane": "fulfillment"}})

		b.TxForEach("allocate-inventory", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			shipments, err := stepItemOutputs[playgroundShipmentArtifact](step, "shard-shipments")
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(shipments))
			for _, shipment := range shipments {
				items = append(items, shipment)
			}
			return items, nil
		}, func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			artifact, err := decodeInput[playgroundShipmentArtifact](step)
			if err != nil {
				return nil, err
			}
			if step.ItemKey == "000001" && step.Attempt == 1 {
				return nil, fmt.Errorf("inventory reservation lock timeout")
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			allocation := playgroundAllocation{ShipmentID: artifact.ShipmentID, AllocationID: "alloc-" + artifact.ShipmentID, ReservedQty: artifact.PackageCount * 2}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "inventory-allocated", allocation); err != nil {
				return nil, err
			}
			return allocation, nil
		}, workflow.StepOptions{Label: "Allocate Inventory (TX Fan-Out)", QueueName: "playground.inventory", DependsOn: []string{"shard-shipments"}, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3, BackoffBase: 150 * time.Millisecond, BackoffMax: 500 * time.Millisecond}, Metadata: map[string]any{"lane": "inventory"}})

		b.TxForEach("create-picklists", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			shipments, err := stepItemOutputs[playgroundShipmentArtifact](step, "shard-shipments")
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(shipments))
			for _, shipment := range shipments {
				items = append(items, shipment)
			}
			return items, nil
		}, func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			shipment, err := decodeInput[playgroundShipmentArtifact](step)
			if err != nil {
				return nil, err
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			picklist := playgroundPicklist{ShipmentID: shipment.ShipmentID, PicklistID: "pick-" + shipment.ShipmentID, Destination: input.Customer.Region}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "picklist-created", picklist); err != nil {
				return nil, err
			}
			return picklist, nil
		}, workflow.StepOptions{Label: "Create Picklists (TX Fan-Out)", QueueName: "playground.warehouse", DependsOn: []string{"shard-shipments"}, Metadata: map[string]any{"lane": "warehouse"}})

		b.ForEach("arrange-carriers", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			shipments, err := stepItemOutputs[playgroundShipmentArtifact](step, "shard-shipments")
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(shipments))
			for _, shipment := range shipments {
				items = append(items, shipment)
			}
			return items, nil
		}, func(_ context.Context, step workflow.StepContext) (any, error) {
			shipment, err := decodeInput[playgroundShipmentArtifact](step)
			if err != nil {
				return nil, err
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			carrier := "DHL"
			service := "economy"
			if input.Priority == "express" {
				carrier = "UPS"
				service = "next-day"
			}
			return playgroundCarrierBooking{ShipmentID: shipment.ShipmentID, Carrier: carrier, Service: service, ETA: "2026-03-18T10:00:00Z"}, nil
		}, workflow.StepOptions{Label: "Arrange Carriers", QueueName: "playground.shipping", DependsOn: []string{"choose-fulfillment", "authorize-payment"}, Metadata: map[string]any{"lane": "shipping"}})

		b.Step("compile-customer-notice", func(_ context.Context, step workflow.StepContext) (any, error) {
			bookings := step.ItemOutputs("arrange-carriers")
			payment, err := stepOutput[playgroundPayment](step, "authorize-payment")
			if err != nil {
				return nil, err
			}
			return map[string]any{"template": "global-order-ready", "audience": "customer", "shipment_updates": len(bookings), "captured_cents": payment.CapturedCents}, nil
		}, workflow.StepOptions{Label: "Compile Customer Notice", QueueName: "playground.notifications", DependsOn: []string{"authorize-payment", "arrange-carriers"}, Metadata: map[string]any{"lane": "notifications"}})

		b.TxStep("emit-order-events", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			events := []string{"order.accepted", "payment.authorized", "fulfillment.selected"}
			for _, event := range events {
				if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, event, map[string]any{"order_id": input.OrderID}); err != nil {
					return nil, err
				}
			}
			if err := upsertPlaygroundOrder(ctx, tx, input.OrderID, step.RunID, input.Customer.Email, "processing", input); err != nil {
				return nil, err
			}
			return map[string]any{"event_count": len(events)}, nil
		}, workflow.StepOptions{Label: "Emit Order Events (TX)", QueueName: "playground.outbox", DependsOn: []string{"reserve-idempotency", "snapshot-cart", "authorize-payment"}, Metadata: map[string]any{"lane": "outbox"}})

		b.Step("package-wave-alpha", func(_ context.Context, step workflow.StepContext) (any, error) {
			allocations, err := stepItemOutputs[playgroundAllocation](step, "allocate-inventory")
			if err != nil {
				return nil, err
			}
			return map[string]any{"wave": "alpha", "packages": len(allocations), "stage": "boxing"}, nil
		}, workflow.StepOptions{Label: "Package Wave Alpha", QueueName: "playground.warehouse", DependsOn: []string{"allocate-inventory"}, Metadata: map[string]any{"lane": "warehouse"}})

		b.Step("package-wave-beta", func(_ context.Context, step workflow.StepContext) (any, error) {
			picklists, err := stepItemOutputs[playgroundPicklist](step, "create-picklists")
			if err != nil {
				return nil, err
			}
			return map[string]any{"wave": "beta", "picklists": len(picklists), "stage": "scan-pack"}, nil
		}, workflow.StepOptions{Label: "Package Wave Beta", QueueName: "playground.warehouse", DependsOn: []string{"create-picklists"}, Metadata: map[string]any{"lane": "warehouse"}})

		b.Step("quality-gate", func(_ context.Context, step workflow.StepContext) (any, error) {
			alpha, err := stepOutput[map[string]any](step, "package-wave-alpha")
			if err != nil {
				return nil, err
			}
			beta, err := stepOutput[map[string]any](step, "package-wave-beta")
			if err != nil {
				return nil, err
			}
			return map[string]any{"quality_score": 98, "alpha_stage": alpha["stage"], "beta_stage": beta["stage"]}, nil
		}, workflow.StepOptions{Label: "Quality Gate", QueueName: "playground.qa", DependsOn: []string{"package-wave-alpha", "package-wave-beta"}, Metadata: map[string]any{"lane": "qa"}})

		b.Step("customs-review", func(_ context.Context, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			lane := "domestic-fastlane"
			if input.Customer.Region != "US" {
				lane = "cross-border-review"
			}
			return map[string]any{"lane": lane, "documents_required": input.Customer.Region != "US"}, nil
		}, workflow.StepOptions{Label: "Customs Review", QueueName: "playground.compliance", DependsOn: []string{"quality-gate"}, Metadata: map[string]any{"lane": "compliance"}})

		b.TxStep("generate-trade-docs", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			payload := map[string]any{"bundle_id": "docs-" + input.OrderID, "document_count": 4}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "trade-docs-generated", payload); err != nil {
				return nil, err
			}
			return payload, nil
		}, workflow.StepOptions{Label: "Generate Trade Docs (TX)", QueueName: "playground.compliance", DependsOn: []string{"customs-review"}, Metadata: map[string]any{"lane": "compliance"}})

		b.Step("book-labels", func(_ context.Context, step workflow.StepContext) (any, error) {
			bookings, err := stepItemOutputs[playgroundCarrierBooking](step, "arrange-carriers")
			if err != nil {
				return nil, err
			}
			return map[string]any{"labels_booked": len(bookings), "carrier_mix": carrierMix(bookings)}, nil
		}, workflow.StepOptions{Label: "Book Labels", QueueName: "playground.shipping", DependsOn: []string{"generate-trade-docs", "arrange-carriers"}, Metadata: map[string]any{"lane": "shipping"}})

		b.TxStep("produce-invoice", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			pricing, err := stepOutput[playgroundPricing](step, "compute-pricing")
			if err != nil {
				return nil, err
			}
			tax, err := stepOutput[playgroundTax](step, "compute-tax-ledger")
			if err != nil {
				return nil, err
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			invoice := playgroundInvoice{InvoiceID: "inv-" + input.OrderID, TotalCents: pricing.NetCents + tax.TaxCents}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "invoice-produced", invoice); err != nil {
				return nil, err
			}
			return invoice, nil
		}, workflow.StepOptions{Label: "Produce Invoice (TX)", QueueName: "playground.billing", DependsOn: []string{"compute-tax-ledger", "authorize-payment", "compute-pricing"}, Metadata: map[string]any{"lane": "billing"}})

		b.ForEach("fanout-webhooks", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			notice, err := stepOutput[struct {
				Template string `json:"template"`
				Audience string `json:"audience"`
			}](step, "compile-customer-notice")
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(input.Webhooks))
			for _, target := range input.Webhooks {
				items = append(items, playgroundWebhookDispatch{Name: target.Name, URL: target.URL, Event: target.Event, Template: notice.Template, Audience: notice.Audience, BatchSize: len(input.Items)})
			}
			return items, nil
		}, func(_ context.Context, step workflow.StepContext) (any, error) {
			target, err := decodeInput[playgroundWebhookDispatch](step)
			if err != nil {
				return nil, err
			}
			return target, nil
		}, workflow.StepOptions{Label: "Fan-Out Webhooks", QueueName: "playground.outbox", DependsOn: []string{"emit-order-events", "compile-customer-notice"}, Metadata: map[string]any{"lane": "outbox"}})

		b.TxForEach("persist-webhooks", func(_ context.Context, step workflow.StepContext) ([]any, error) {
			targets, err := stepItemOutputs[playgroundWebhookDispatch](step, "fanout-webhooks")
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(targets))
			for _, target := range targets {
				items = append(items, target)
			}
			return items, nil
		}, func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			target, err := decodeInput[playgroundWebhookDispatch](step)
			if err != nil {
				return nil, err
			}
			if step.ItemKey == "000000" && step.Attempt == 1 {
				return nil, fmt.Errorf("webhook outbox unavailable")
			}
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			if err := insertWebhookOutbox(ctx, tx, step.RunID, input.OrderID, target); err != nil {
				return nil, err
			}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "webhook-persisted", target); err != nil {
				return nil, err
			}
			return map[string]any{"delivery_id": "wh-" + input.OrderID + "-" + step.ItemKey, "target": target.Name, "event": target.Event}, nil
		}, workflow.StepOptions{Label: "Persist Webhooks (TX Fan-Out)", QueueName: "playground.outbox", DependsOn: []string{"fanout-webhooks"}, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3, BackoffBase: 150 * time.Millisecond, BackoffMax: 500 * time.Millisecond}, Metadata: map[string]any{"lane": "outbox"}})

		b.Step("reconcile-audit", func(_ context.Context, step workflow.StepContext) (any, error) {
			deliveries := step.ItemOutputs("persist-webhooks")
			invoice, err := stepOutput[playgroundInvoice](step, "produce-invoice")
			if err != nil {
				return nil, err
			}
			return map[string]any{"invoice_id": invoice.InvoiceID, "webhook_deliveries": len(deliveries), "audit_balance": "clean"}, nil
		}, workflow.StepOptions{Label: "Reconcile Audit", QueueName: "playground.audit", DependsOn: []string{"produce-invoice", "persist-webhooks"}, Metadata: map[string]any{"lane": "audit"}})

		b.Step("final-approval", func(_ context.Context, step workflow.StepContext) (any, error) {
			notice, err := stepOutput[map[string]any](step, "compile-customer-notice")
			if err != nil {
				return nil, err
			}
			labels, err := stepOutput[map[string]any](step, "book-labels")
			if err != nil {
				return nil, err
			}
			reconcile, err := stepOutput[map[string]any](step, "reconcile-audit")
			if err != nil {
				return nil, err
			}
			return map[string]any{"approved": true, "notice_template": notice["template"], "carrier_mix": labels["carrier_mix"], "audit_balance": reconcile["audit_balance"]}, nil
		}, workflow.StepOptions{Label: "Final Approval", QueueName: "playground.audit", DependsOn: []string{"compile-customer-notice", "book-labels", "reconcile-audit"}, Metadata: map[string]any{"lane": "audit"}})

		b.TxStep("complete-order", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			input, err := decodeRunInput[playgroundOrderInput](step)
			if err != nil {
				return nil, err
			}
			completion := playgroundCompletion{OrderID: input.OrderID, FinalStatus: "completed", CompletedStage: "handoff"}
			if err := upsertPlaygroundOrder(ctx, tx, input.OrderID, step.RunID, input.Customer.Email, completion.FinalStatus, input); err != nil {
				return nil, err
			}
			if err := insertAuditEvent(ctx, tx, step.RunID, input.OrderID, step.StepName, step.ItemKey, "order-completed", completion); err != nil {
				return nil, err
			}
			return completion, nil
		}, workflow.StepOptions{Label: "Complete Order (TX)", QueueName: "playground.ledger", DependsOn: []string{"final-approval"}, Metadata: map[string]any{"lane": "ledger"}})
	})
	if err != nil {
		panic(err)
	}
	return def
}

func seedPlaygroundRuns(ctx context.Context, queue *qpkg.Client, module *workflow.Module) ([]string, error) {
	bootstrapJobs := []qpkg.EnqueueParams{
		{QueueName: "playground.audit", Payload: []byte(`{"event":"boot","component":"audit"}`), MaxAttempts: 3},
		{QueueName: "playground.notifications", Payload: []byte(`{"event":"warmup","channel":"email"}`), MaxAttempts: 3},
		{QueueName: "playground.shipping", Payload: []byte(`{"event":"carrier-cache-refresh"}`), MaxAttempts: 3},
	}
	for _, job := range bootstrapJobs {
		if _, err := queue.Enqueue(ctx, job); err != nil {
			return nil, err
		}
	}

	seedInputs := []playgroundOrderInput{
		sampleOrderInput("ORD-9001", "standard"),
		sampleOrderInput("ORD-9002", "express"),
	}

	runIDs := make([]string, 0, len(seedInputs))
	for idx, input := range seedInputs {
		run, err := module.Start(ctx, playgroundWorkflowName, input, &workflow.StartRunOptions{
			CreatedBy:      "playground",
			CorrelationKey: fmt.Sprintf("demo-order-%02d", idx+1),
		})
		if err != nil {
			return nil, err
		}
		runIDs = append(runIDs, run.ID)
	}
	return runIDs, nil
}

func ensurePlaygroundTables(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS playground_orders (
			order_id TEXT PRIMARY KEY,
			workflow_run_id TEXT NOT NULL,
			customer_email TEXT NOT NULL,
			status TEXT NOT NULL,
			snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS playground_audit_log (
			id BIGSERIAL PRIMARY KEY,
			workflow_run_id TEXT NOT NULL,
			order_id TEXT NOT NULL,
			step_name TEXT NOT NULL,
			item_key TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL,
			payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS playground_webhook_outbox (
			id BIGSERIAL PRIMARY KEY,
			workflow_run_id TEXT NOT NULL,
			order_id TEXT NOT NULL,
			target_name TEXT NOT NULL,
			event_name TEXT NOT NULL,
			status TEXT NOT NULL,
			payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func upsertPlaygroundOrder(ctx context.Context, tx *sql.Tx, orderID, runID, customerEmail, status string, snapshot any) error {
	payload, err := encodeJSON(snapshot)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO playground_orders (order_id, workflow_run_id, customer_email, status, snapshot_json)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (order_id) DO UPDATE SET
    workflow_run_id = EXCLUDED.workflow_run_id,
    customer_email = EXCLUDED.customer_email,
    status = EXCLUDED.status,
    snapshot_json = EXCLUDED.snapshot_json,
    updated_at = NOW()`, orderID, runID, customerEmail, status, string(payload))
	return err
}

func insertAuditEvent(ctx context.Context, tx *sql.Tx, runID, orderID, stepName, itemKey, eventType string, payload any) error {
	body, err := encodeJSON(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO playground_audit_log (workflow_run_id, order_id, step_name, item_key, event_type, payload_json)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)`, runID, orderID, stepName, itemKey, eventType, string(body))
	return err
}

func insertWebhookOutbox(ctx context.Context, tx *sql.Tx, runID, orderID string, target playgroundWebhookDispatch) error {
	body, err := encodeJSON(target)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO playground_webhook_outbox (workflow_run_id, order_id, target_name, event_name, status, payload_json)
VALUES ($1, $2, $3, $4, 'queued', $5::jsonb)`, runID, orderID, target.Name, target.Event, string(body))
	return err
}

func sampleOrderInput(orderID, priority string) playgroundOrderInput {
	customerTier := "gold"
	region := "US"
	if priority == "express" {
		customerTier = "platinum"
		region = "CA"
	}
	return playgroundOrderInput{
		OrderID:         orderID,
		Currency:        "USD",
		Priority:        priority,
		RequestedShipBy: "2026-03-18T16:00:00Z",
		Customer: playgroundCustomer{
			ID:     "cust-" + strings.ToLower(orderID),
			Email:  strings.ToLower(orderID) + "@playground.nanostack.dev",
			Tier:   customerTier,
			Region: region,
		},
		Items: []playgroundOrderItem{
			{SKU: "SKU-RED-01", Quantity: 2, Warehouse: "us-east", PriceCents: 4200},
			{SKU: "SKU-BLU-02", Quantity: 1, Warehouse: "us-east", PriceCents: 6800},
			{SKU: "SKU-GRN-03", Quantity: 3, Warehouse: "us-west", PriceCents: 2500},
			{SKU: "SKU-BLK-04", Quantity: 1, Warehouse: "eu-central", PriceCents: 11800},
			{SKU: "SKU-WHT-05", Quantity: 4, Warehouse: "eu-central", PriceCents: 1900},
			{SKU: "SKU-GLD-06", Quantity: 2, Warehouse: "us-west", PriceCents: 8600},
		},
		Webhooks: []playgroundWebhookTarget{
			{Name: "erp-sync", URL: "https://example.invalid/hooks/erp", Event: "order.updated"},
			{Name: "warehouse-bus", URL: "https://example.invalid/hooks/warehouse", Event: "shipment.ready"},
			{Name: "crm-journey", URL: "https://example.invalid/hooks/crm", Event: "customer.notified"},
			{Name: "analytics-stream", URL: "https://example.invalid/hooks/analytics", Event: "order.completed"},
		},
	}
}

func buildShipmentPlans(input playgroundOrderInput) []playgroundShipmentPlan {
	byWarehouse := make(map[string]*playgroundShipmentPlan)
	order := make([]string, 0)
	for _, item := range input.Items {
		plan, ok := byWarehouse[item.Warehouse]
		if !ok {
			plan = &playgroundShipmentPlan{
				ShipmentID: fmt.Sprintf("%s-%s", strings.ToLower(input.OrderID), strings.ReplaceAll(item.Warehouse, "_", "-")),
				Warehouse:  item.Warehouse,
				Priority:   input.Priority,
				Mode:       shipmentMode(input.Priority, item.Warehouse),
			}
			byWarehouse[item.Warehouse] = plan
			order = append(order, item.Warehouse)
		}
		plan.ItemCount++
		plan.Quantity += item.Quantity
	}
	sort.Strings(order)
	shipments := make([]playgroundShipmentPlan, 0, len(order))
	for _, warehouse := range order {
		shipments = append(shipments, *byWarehouse[warehouse])
	}
	return shipments
}

func shipmentMode(priority, warehouse string) string {
	if priority == "express" {
		return "priority-air"
	}
	if strings.HasPrefix(warehouse, "eu") {
		return "customs-consolidated"
	}
	return "ground-split"
}

func totalQuantity(items []playgroundOrderItem) int {
	total := 0
	for _, item := range items {
		total += item.Quantity
	}
	return total
}

func totalPriceCents(items []playgroundOrderItem) int {
	total := 0
	for _, item := range items {
		total += item.PriceCents * item.Quantity
	}
	return total
}

func uniqueSKUCount(items []playgroundOrderItem) int {
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		seen[item.SKU] = struct{}{}
	}
	return len(seen)
}

func uniqueWarehouses(items []playgroundOrderItem) []string {
	seen := make(map[string]struct{}, len(items))
	warehouses := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item.Warehouse]; ok {
			continue
		}
		seen[item.Warehouse] = struct{}{}
		warehouses = append(warehouses, item.Warehouse)
	}
	sort.Strings(warehouses)
	return warehouses
}

func carrierMix(bookings []playgroundCarrierBooking) []string {
	seen := make(map[string]struct{}, len(bookings))
	out := make([]string, 0, len(bookings))
	for _, booking := range bookings {
		key := booking.Carrier + ":" + booking.Service
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func encodeJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte(`{}`), nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []byte(`{}`), nil
	}
	return data, nil
}

func decodeRunInput[T any](step workflow.StepContext) (T, error) {
	var value T
	if err := step.DecodeRunInput(&value); err != nil {
		return value, err
	}
	return value, nil
}

func decodeInput[T any](step workflow.StepContext) (T, error) {
	var value T
	if err := step.DecodeInput(&value); err != nil {
		return value, err
	}
	return value, nil
}

func stepOutput[T any](step workflow.StepContext, stepName string) (T, error) {
	var value T
	if err := step.Output(stepName, &value); err != nil {
		return value, err
	}
	return value, nil
}

func stepItemOutputs[T any](step workflow.StepContext, stepName string) ([]T, error) {
	out := make([]T, 0)
	for _, item := range step.ItemOutputs(stepName) {
		var decoded T
		if err := json.Unmarshal(item.Payload, &decoded); err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}
