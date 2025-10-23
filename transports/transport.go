package transports

import (
	"fmt"

	"github.com/ieshan/nakusp/models"
)

func GetPayload(job *models.Job) string {
	return fmt.Sprintf("%s:%s:%d:%s", job.ID, job.Name, job.RetryCount, job.Payload)
}
