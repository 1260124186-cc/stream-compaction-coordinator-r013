package runtimecheck

import (
	"context"
	"testing"
	"time"
)

func TestWorkflowChecks(t *testing.T) {
	workflows := []string{
		"append-rollover",
		"compaction-recovery",
		"replica-watermark",
		"deadline-semantics",
	}
	for _, workflow := range workflows {
		t.Run(workflow, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result, err := Run(ctx, workflow)
			if err != nil {
				t.Fatalf("workflow %s failed: %v", workflow, err)
			}
			if result.Assertions == 0 {
				t.Fatalf("workflow %s recorded no assertions", workflow)
			}
		})
	}
}
