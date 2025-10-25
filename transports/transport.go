// Package transports provides various backend implementations for the Nakusp job queue system.
// Each transport implements the models.Transport interface and provides different storage
// mechanisms (in-memory, SQLite, Redis) for job queuing and processing.
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
