# Relay

[![Go Reference](https://pkg.go.dev/badge/github.com/binozo/go-relay/relay.svg)](https://pkg.go.dev/github.com/binozo/go-relay/relay)

**Relay** is a type-safe, context-aware event broadcasting library for Go. It provides a simple `Bus` pattern for one-to-many communication using generics.

### Installation

```bash
go get -u github.com/binozo/go-relay
```

### Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/binozo/go-relay/relay"
)

func main() {
	bus := relay.NewChanBus[string]()
	defer bus.Close()

	sub := bus.Subscribe()
	defer sub.Unsubscribe()
	
	go func() {
		for {
			msg, err := sub.Listen(context.TODO())
			if err != nil {
				return
			}

			fmt.Println("Received:", msg)
		}
	}()

	bus.Broadcast("Hello, Relay!")
}
```

---

License: [Apache 2.0](LICENSE)