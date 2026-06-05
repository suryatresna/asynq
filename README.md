<img src="https://user-images.githubusercontent.com/11155743/114697792-ffbfa580-9d26-11eb-8e5b-33bef69476dc.png" alt="Asynq logo" width="360px" />

# Simple, reliable & efficient distributed task queue in Go

[![GoDoc](https://godoc.org/github.com/suryatresna/asynq?status.svg)](https://godoc.org/github.com/suryatresna/asynq)
[![Go Report Card](https://goreportcard.com/badge/github.com/suryatresna/asynq)](https://goreportcard.com/report/github.com/suryatresna/asynq)
![Build Status](https://github.com/suryatresna/asynq/workflows/build/badge.svg)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)

Asynq is a Go library for queueing tasks and processing them asynchronously with workers. It's backed by [Redis](https://redis.io/) and is designed to be scalable yet easy to get started.

Highlevel overview of how Asynq works:

- Client puts tasks on a queue
- Server pulls tasks off queues and starts a worker goroutine for each task
- Tasks are processed concurrently by multiple workers

Task queues are used as a mechanism to distribute work across multiple machines. A system can consist of multiple worker servers and brokers, giving way to high availability and horizontal scaling.

**Example use case**

![Task Queue Diagram](https://user-images.githubusercontent.com/11155743/116358505-656f5f80-a806-11eb-9c16-94e49dab0f99.jpg)

## Features

- Guaranteed [at least one execution](https://www.cloudcomputingpatterns.org/at_least_once_delivery/) of a task
- Scheduling of tasks
- [Retries](https://github.com/suryatresna/asynq/wiki/Task-Retry) of failed tasks
- Automatic recovery of tasks in the event of a worker crash
- [Weighted priority queues](https://github.com/suryatresna/asynq/wiki/Queue-Priority#weighted-priority)
- [Strict priority queues](https://github.com/suryatresna/asynq/wiki/Queue-Priority#strict-priority)
- Low latency to add a task since writes are fast in Redis
- De-duplication of tasks using [unique option](https://github.com/suryatresna/asynq/wiki/Unique-Tasks)
- Allow [timeout and deadline per task](https://github.com/suryatresna/asynq/wiki/Task-Timeout-and-Cancelation)
- Allow [aggregating group of tasks](https://github.com/suryatresna/asynq/wiki/Task-aggregation) to batch multiple successive operations
- [Flexible handler interface with support for middlewares](https://github.com/suryatresna/asynq/wiki/Handler-Deep-Dive)
- [Ability to pause queue](/tools/asynq/README.md#pause) to stop processing tasks from the queue
- [Periodic Tasks](https://github.com/suryatresna/asynq/wiki/Periodic-Tasks)
- [Support Redis Sentinels](https://github.com/suryatresna/asynq/wiki/Automatic-Failover) for high availability
- Integration with [Prometheus](https://prometheus.io/) to collect and visualize queue metrics
- [Web UI](#web-ui) to inspect and remote-control queues and tasks
- [CLI](#command-line-tool) to inspect and remote-control queues and tasks

## Stability and Compatibility

**Status**: The library relatively stable and is currently undergoing **moderate development** with less frequent breaking API changes.

> ☝️ **Important Note**: Current major version is zero (`v0.x.x`) to accommodate rapid development and fast iteration while getting early feedback from users (_feedback on APIs are appreciated!_). The public API could change without a major version update before `v1.0.0` release.

### Redis Cluster Compatibility

Some of the lua scripts in this library may not be compatible with Redis Cluster.

## Sponsoring
If you are using this package in production, **please consider sponsoring the project to show your support!**

## Quickstart
Make sure you have Go installed ([download](https://golang.org/dl/)). The **last two** Go versions are supported (See https://go.dev/dl).

Initialize your project by creating a folder and then running `go mod init github.com/your/repo` ([learn more](https://blog.golang.org/using-go-modules)) inside the folder. Then install Asynq library with the [`go get`](https://golang.org/cmd/go/#hdr-Add_dependencies_to_current_module_and_install_them) command:

```sh
go get -u github.com/suryatresna/asynq
```

Make sure you're running a Redis server locally or from a [Docker](https://hub.docker.com/_/redis) container. Version `4.0` or higher is required.

Next, write a package that encapsulates task creation and task handling.

```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/suryatresna/asynq"
)

// A list of task types.
const (
    TypeEmailDelivery   = "email:deliver"
    TypeImageResize     = "image:resize"
)

type EmailDeliveryPayload struct {
    UserID     int
    TemplateID string
}

type ImageResizePayload struct {
    SourceURL string
}

//----------------------------------------------
// Write a function NewXXXTask to create a task.
// A task consists of a type and a payload.
//----------------------------------------------

func NewEmailDeliveryTask(userID int, tmplID string) (*asynq.Task, error) {
    payload, err := json.Marshal(EmailDeliveryPayload{UserID: userID, TemplateID: tmplID})
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeEmailDelivery, payload), nil
}

func NewImageResizeTask(src string) (*asynq.Task, error) {
    payload, err := json.Marshal(ImageResizePayload{SourceURL: src})
    if err != nil {
        return nil, err
    }
    // task options can be passed to NewTask, which can be overridden at enqueue time.
    return asynq.NewTask(TypeImageResize, payload, asynq.MaxRetry(5), asynq.Timeout(20 * time.Minute)), nil
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface. See examples below.
//---------------------------------------------------------------

func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
    var p EmailDeliveryPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Sending Email to User: user_id=%d, template_id=%s", p.UserID, p.TemplateID)
    // Email delivery code ...
    return nil
}

// ImageProcessor implements asynq.Handler interface.
type ImageProcessor struct {
    // ... fields for struct
}

func (processor *ImageProcessor) ProcessTask(ctx context.Context, t *asynq.Task) error {
    var p ImageResizePayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Resizing image: src=%s", p.SourceURL)
    // Image resizing code ...
    return nil
}

func NewImageProcessor() *ImageProcessor {
	return &ImageProcessor{}
}
```

In your application code, import the above package and use [`Client`](https://pkg.go.dev/github.com/suryatresna/asynq?tab=doc#Client) to put tasks on queues.

```go
package main

import (
    "log"
    "time"

    "github.com/suryatresna/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
    defer client.Close()

    // ------------------------------------------------------
    // Example 1: Enqueue task to be processed immediately.
    //            Use (*Client).Enqueue method.
    // ------------------------------------------------------

    task, err := tasks.NewEmailDeliveryTask(42, "some:template:id")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err := client.Enqueue(task)
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)


    // ------------------------------------------------------------
    // Example 2: Schedule task to be processed in the future.
    //            Use ProcessIn or ProcessAt option.
    // ------------------------------------------------------------

    info, err = client.Enqueue(task, asynq.ProcessIn(24*time.Hour))
    if err != nil {
        log.Fatalf("could not schedule task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)


    // ----------------------------------------------------------------------------
    // Example 3: Set other options to tune task processing behavior.
    //            Options include MaxRetry, Queue, Timeout, Deadline, Unique etc.
    // ----------------------------------------------------------------------------

    task, err = tasks.NewImageResizeTask("https://example.com/myassets/image.jpg")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err = client.Enqueue(task, asynq.MaxRetry(10), asynq.Timeout(3 * time.Minute))
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}
```

Next, start a worker server to process these tasks in the background. To start the background workers, use [`Server`](https://pkg.go.dev/github.com/suryatresna/asynq?tab=doc#Server) and provide your [`Handler`](https://pkg.go.dev/github.com/suryatresna/asynq?tab=doc#Handler) to process the tasks.

You can optionally use [`ServeMux`](https://pkg.go.dev/github.com/suryatresna/asynq?tab=doc#ServeMux) to create a handler, just as you would with [`net/http`](https://golang.org/pkg/net/http/) Handler.

```go
package main

import (
    "log"

    "github.com/suryatresna/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    srv := asynq.NewServer(
        asynq.RedisClientOpt{Addr: redisAddr},
        asynq.Config{
            // Specify how many concurrent workers to use
            Concurrency: 10,
            // Optionally specify multiple queues with different priority.
            Queues: map[string]int{
                "critical": 6,
                "default":  3,
                "low":      1,
            },
            // See the godoc for other configuration options
        },
    )

    // mux maps a type to a handler
    mux := asynq.NewServeMux()
    mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
    mux.Handle(tasks.TypeImageResize, tasks.NewImageProcessor())
    // ...register other handlers...

    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}
```

### Workflow: chaining tasks with parameter passing

Use `HandleStep` instead of `HandleFunc` when a handler needs to receive output from its upstream step or pass data to the next one. Steps are wired together by `RegisterFlow` + `SetFlows` and executed in topological order.

```go
package main

import (
    "context"
    "log"

    "github.com/suryatresna/asynq"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    srv := asynq.NewServer(
        asynq.RedisClientOpt{Addr: redisAddr},
        asynq.Config{Concurrency: 10},
    )

    mux := asynq.NewServeMux()

    // HandleStep — receives params from parent steps, returns params for child steps.
    mux.HandleStep("jobA", func(ctx context.Context, t *asynq.Task, in asynq.FlowParams) (asynq.FlowParams, error) {
        log.Printf("jobA running, input params: %v", in)
        // Produce output that jobB will receive.
        return asynq.FlowParams{"order_id": 42, "amount": 9.99}, nil
    })

    mux.HandleStep("jobB", func(ctx context.Context, t *asynq.Task, in asynq.FlowParams) (asynq.FlowParams, error) {
        // Receive params produced by jobA.
        orderID := in["order_id"]
        amount  := in["amount"]
        log.Printf("jobB processing order %v for amount %v", orderID, amount)
        // Override or extend params for the next step.
        return asynq.FlowParams{"order_id": orderID, "status": "processed"}, nil
    })

    // Define the flow: jobA → jobB
    flow1 := asynq.RegisterFlow("flowA")
    flow1.SetFlows("step1", "jobA", "jobB")

    mux.UseWorkflow(asynq.NewWorkflow(flow1))

    // Enqueue a "flowA" task to trigger the entire chain.
    // client.Enqueue(asynq.NewTask("flowA", payload))

    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}
```

Steps registered with `HandleFunc` (legacy) also work in a workflow — they just don't receive or produce `FlowParams`.

### Workflow: defining flows with Mermaid-like markdown

Instead of calling `SetFlows` manually, you can describe the DAG in a Mermaid-like markdown string and pass it to `RegisterFlowMarkdown`.

```go
workflowMarkdown := `
graph DataPipeline
    ingest[Start Data Ingestion] --> clean(Clean Datasets)
    ingest --> fetch(Fetch API Logs)
    clean --> validate{Run Validations}
    fetch --> validate
    validate -->|Pass| load[Load to Data Warehouse]
    validate -->|Fail| alert[Trigger Alert]
`

flow, err := asynq.RegisterFlowMarkdown(workflowMarkdown)
if err != nil {
    log.Fatal(err)
}

mux := asynq.NewServeMux()
mux.HandleStep("ingest",   handleIngest)
mux.HandleStep("clean",    handleClean)
mux.HandleStep("fetch",    handleFetch)
mux.HandleStep("validate", handleValidate)
mux.HandleStep("load",     handleLoad)
mux.HandleStep("alert",    handleAlert)

mux.UseWorkflow(asynq.NewWorkflow(flow))
srv.Run(mux)
```

**Supported syntax**

| Syntax | Meaning |
|---|---|
| `graph Name` | Flow group name (required header) |
| `A --> B` | Simple edge (label auto-generated as `A->B`) |
| `A -->|label| B` | Edge with an explicit label |
| `A[Display text]` | Rectangle node annotation (cosmetic only) |
| `A(Display text)` | Rounded node annotation (cosmetic only) |
| `A{Display text}` | Diamond node annotation (cosmetic only) |
| `%% comment` | Comment line (ignored) |

Node IDs (`ingest`, `clean`, …) must match the handler names passed to `mux.HandleStep` / `mux.HandleFunc` exactly. Display annotations are discarded during wiring.

For a more detailed walk-through of the library, see our [Getting Started](https://github.com/suryatresna/asynq/wiki/Getting-Started) guide.

To learn more about `asynq` features and APIs, see the package [godoc](https://godoc.org/github.com/suryatresna/asynq).

## Web UI

[Asynqmon](https://github.com/suryatresna/asynqmon) is a web based tool for monitoring and administrating Asynq queues and tasks.

Here's a few screenshots of the Web UI:

**Queues view**

![Web UI Queues View](https://user-images.githubusercontent.com/11155743/114697016-07327f00-9d26-11eb-808c-0ac841dc888e.png)

**Tasks view**

![Web UI TasksView](https://user-images.githubusercontent.com/11155743/114697070-1f0a0300-9d26-11eb-855c-d3ec263865b7.png)

**Metrics view**
<img width="1532" alt="Screen Shot 2021-12-19 at 4 37 19 PM" src="https://user-images.githubusercontent.com/10953044/146777420-cae6c476-bac6-469c-acce-b2f6584e8707.png">

**Settings and adaptive dark mode**

![Web UI Settings and adaptive dark mode](https://user-images.githubusercontent.com/11155743/114697149-3517c380-9d26-11eb-9f7a-ae2dd00aad5b.png)

For details on how to use the tool, refer to the tool's [README](https://github.com/suryatresna/asynqmon#readme).

## Command Line Tool

Asynq ships with a command line tool to inspect the state of queues and tasks.

To install the CLI tool, run the following command:

```sh
go install github.com/suryatresna/asynq/tools/asynq@latest
```

Here's an example of running the `asynq dash` command:

![Gif](/docs/assets/dash.gif)

For details on how to use the tool, refer to the tool's [README](/tools/asynq/README.md).

## Contributing

We are open to, and grateful for, any contributions (GitHub issues/PRs, feedback on [Gitter channel](https://gitter.im/go-asynq/community), etc) made by the community.

Please see the [Contribution Guide](/CONTRIBUTING.md) before contributing.

## License

Copyright (c) 2019-present [Ken Hibino](https://github.com/hibiken) and [Contributors](https://github.com/suryatresna/asynq/graphs/contributors). `Asynq` is free and open-source software licensed under the [MIT License](https://github.com/suryatresna/asynq/blob/master/LICENSE). Official logo was created by [Vic Shóstak](https://github.com/koddr) and distributed under [Creative Commons](https://creativecommons.org/publicdomain/zero/1.0/) license (CC0 1.0 Universal).
