// Package transports provides the Transport interface, shared utilities,
// and a zero-dependency FakeTransport for testing.
// External transports (Redis, SQLite) live in separate submodules.
package transports

import (
	"fmt"

	"github.com/ieshan/nakusp/models"
)

// GetPayload serializes a job into a colon-separated string format.
// Format: "id:name:payload"
// This format is used by transports that store jobs as strings (e.g., Redis).
func GetPayload(job *models.Job) string {
	return fmt.Sprintf("%s:%s:%s", job.ID, job.Name, job.Payload)
}
