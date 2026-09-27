package application

import (
	"context"
	"sort"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func (s *Service) Recover(ctx context.Context) (RecoveryReport, error) {
	if err := contextError(ctx); err != nil {
		return RecoveryReport{}, err
	}
	if err := gateAcquire(ctx, &s.mutationGate); err != nil {
		return RecoveryReport{}, err
	}
	defer s.mutationGate.Unlock()

	if err := waitForLock(ctx, s.mu.TryLock); err != nil {
		return RecoveryReport{}, err
	}
	defer s.mu.Unlock()

	if err := contextError(ctx); err != nil {
		return RecoveryReport{}, err
	}
	next := domain.CloneState(s.state)
	report, err := recoverState(next, s.clock.Now())
	if err != nil {
		return RecoveryReport{}, err
	}
	if !report.Changed {
		return report, nil
	}
	if err := contextError(ctx); err != nil {
		return RecoveryReport{}, err
	}
	if err := s.repo.SaveContext(ctx, next); err != nil {
		return RecoveryReport{}, err
	}
	s.state = next
	s.logger.Info("state.recovered", map[string]any{
		"aborted_plans":      report.AbortedPlanIDs,
		"recomputed_streams": report.RecomputedStreamIDs,
	})
	return report, nil
}

func recoverState(state *domain.State, now time.Time) (RecoveryReport, error) {
	report := RecoveryReport{
		AbortedPlanIDs:      []string{},
		RecomputedStreamIDs: []string{},
	}
	if state == nil {
		return report, domain.NewError(domain.CodeInconsistentState, "state is nil")
	}

	for _, planID := range domain.FindRunningPlanIDs(state) {
		plan := state.Plans[planID]
		if err := domain.AbortRunningPlan(plan, state.Segments, now); err != nil {
			return report, err
		}
		report.AbortedPlanIDs = append(report.AbortedPlanIDs, planID)
		domain.RecordAudit(
			state,
			"compaction.plan.aborted_on_recovery",
			plan.StreamID,
			plan.ID,
			plan.Revision,
			now,
		)
		report.Changed = true
	}

	for streamID, stream := range state.Streams {
		before := stream.Watermark
		domain.RecomputeStreamWatermark(stream, streamSegments(state, streamID))
		if stream.Watermark != before {
			report.RecomputedStreamIDs = append(report.RecomputedStreamIDs, streamID)
			report.Changed = true
		}
	}
	sort.Strings(report.AbortedPlanIDs)
	sort.Strings(report.RecomputedStreamIDs)

	if err := domain.ValidateState(state); err != nil {
		return RecoveryReport{}, domain.WrapError(
			domain.CodeInconsistentState,
			"recovery produced an invalid state",
			err,
		)
	}
	return report, nil
}
