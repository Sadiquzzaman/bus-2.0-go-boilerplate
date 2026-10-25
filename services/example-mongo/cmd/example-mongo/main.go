// Command example-mongo is the single binary of the example-mongo service.
// The first argument selects the run mode (default "api"); run without a
// known mode to list them.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/app"
	platformconfig "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/config"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/bootstrap"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/config"
)

func main() { os.Exit(run()) }

func run() int {
	var cfg config.Config
	platformconfig.MustLoad(&cfg)

	a, err := app.New(context.Background(), cfg.App, cfg.Log, cfg.Observability)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	d := bootstrap.New(a, cfg)

	a.Mode("api", "serve the public HTTP API", d.RunAPI)
	a.Mode("relay", "publish outbox events to Kafka", d.RunRelay)
	a.Mode("consumer", "consume Kafka events", d.RunConsumer)
	a.Command("topics", "create this service's Kafka topics (local/dev only)", d.CreateTopics)
	a.Mode("all", "run every mode in one process (local development)", d.RunAll)
	return a.Run()
}
