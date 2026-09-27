package application

import (
	"context"
	"sort"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

func (s *Service) CreateStream(
	ctx context.Context,
	command CreateStreamCommand,
) (CreateStreamResult, error) {
	var result CreateStreamResult
	err := s.mutate(ctx, "stream.create", func(state *domain.State, timeline domainTimeline) error {
		if existing := storage.FindStreamByName(state, command.Namespace, command.Name); existing != nil {
			return domain.NewError(domain.CodeDuplicate, "a stream with this namespace and name exists").
				WithDetail("namespace", command.Namespace).
				WithDetail("name", command.Name)
		}

		streamID := s.ids.New("str")
		if state.Streams[streamID] != nil {
			return domain.NewError(domain.CodeConflict, "generated stream identifier is already in use")
		}
		segmentID := s.ids.New("seg")
		if state.Segments[segmentID] != nil {
			return domain.NewError(domain.CodeConflict, "generated segment identifier is already in use")
		}

		stream, segment, err := domain.NewStream(
			streamID,
			segmentID,
			command.Namespace,
			command.Name,
			command.ThresholdBytes,
			timeline.Now(),
		)
		if err != nil {
			return err
		}
		state.Streams[stream.ID] = stream
		state.Segments[segment.ID] = segment
		domain.RecordAudit(
			state,
			"stream.created",
			stream.ID,
			stream.ID,
			stream.Revision,
			timeline.Now(),
		)
		result = CreateStreamResult{
			Stream:  cloneStream(stream),
			Segment: cloneSegment(segment),
		}
		return nil
	})
	return result, err
}

func (s *Service) AppendRecords(
	ctx context.Context,
	command AppendRecordsCommand,
) (AppendRecordsResult, error) {
	var result AppendRecordsResult
	err := s.mutate(ctx, "stream.append", func(state *domain.State, timeline domainTimeline) error {
		streamID := normalizedIdentifier(command.StreamID)
		stream := state.Streams[streamID]
		if stream == nil {
			return resourceNotFound("stream", streamID)
		}
		segment := state.Segments[stream.ActiveSegmentID]
		if segment == nil {
			return domain.NewError(domain.CodeInconsistentState, "active segment was not found")
		}

		newSegmentID := s.ids.New("seg")
		if state.Segments[newSegmentID] != nil {
			return domain.NewError(domain.CodeConflict, "generated segment identifier is already in use")
		}
		appended, err := domain.AppendRecords(
			stream,
			segment,
			domain.AppendInput{
				ExpectedRevision: command.ExpectedRevision,
				RecordCount:      command.RecordCount,
				PayloadBytes:     command.PayloadBytes,
			},
			newSegmentID,
			s.limits.MaxAppendRecords,
			s.limits.MaxAppendBytes,
			timeline.Now(),
		)
		if err != nil {
			return err
		}
		if appended.RolledOver {
			state.Segments[appended.Segment.ID] = appended.Segment
		}
		domain.RecomputeStreamWatermark(stream, streamSegments(state, stream.ID))
		domain.RecordAudit(
			state,
			"stream.records_appended",
			stream.ID,
			segment.ID,
			stream.Revision,
			timeline.Now(),
		)
		result = AppendRecordsResult{
			Stream:     cloneStream(stream),
			Segment:    cloneSegment(appended.Segment),
			RolledOver: appended.RolledOver,
		}
		return nil
	})
	return result, err
}

func (s *Service) CreateCompactionPlan(
	ctx context.Context,
	command CreateCompactionPlanCommand,
) (CompactionPlanResult, error) {
	var result CompactionPlanResult
	err := s.mutate(ctx, "compaction.plan.create", func(state *domain.State, timeline domainTimeline) error {
		streamID := normalizedIdentifier(command.StreamID)
		stream := state.Streams[streamID]
		if stream == nil {
			return resourceNotFound("stream", streamID)
		}

		// A retried create that committed before the caller observed the
		// deadline replays the existing active plan instead of producing a
		// second plan over the same sources.
		if replay, conflict, err := replayCompactionPlan(
			state,
			stream,
			command.SourceSegmentIDs,
			command.ExpectedStreamRevision,
		); err != nil {
			return err
		} else if replay != nil {
			result = CompactionPlanResult{Plan: clonePlan(replay)}
			return nil
		} else if conflict != nil {
			return conflict
		}

		planID := s.ids.New("cmp")
		if state.Plans[planID] != nil {
			return domain.NewError(domain.CodeConflict, "generated plan identifier is already in use")
		}
		plan, err := domain.NewCompactionPlan(
			planID,
			stream,
			state.Segments,
			append([]string(nil), command.SourceSegmentIDs...),
			command.ExpectedStreamRevision,
			timeline.Now(),
		)
		if err != nil {
			return err
		}
		state.Plans[plan.ID] = plan
		domain.RecordAudit(
			state,
			"compaction.plan.created",
			stream.ID,
			plan.ID,
			plan.Revision,
			timeline.Now(),
		)
		result = CompactionPlanResult{
			Plan: clonePlan(plan),
		}
		return nil
	})
	return result, err
}

func (s *Service) StartCompactionPlan(
	ctx context.Context,
	command StartCompactionPlanCommand,
) (CompactionPlanResult, error) {
	var result CompactionPlanResult
	err := s.mutate(ctx, "compaction.plan.start", func(state *domain.State, timeline domainTimeline) error {
		planID := normalizedIdentifier(command.PlanID)
		plan := state.Plans[planID]
		if plan == nil {
			return resourceNotFound("compaction plan", planID)
		}
		stream := state.Streams[plan.StreamID]
		if stream == nil {
			return domain.NewError(domain.CodeInconsistentState, "plan stream was not found")
		}
		// Replaying a start that committed before the deadline was observed
		// returns the running plan instead of rejecting the retry as invalid
		// state.
		if plan.State == domain.PlanStateRunning {
			result = CompactionPlanResult{Plan: clonePlan(plan)}
			return nil
		}
		if err := domain.StartCompactionPlan(plan, stream, state.Segments, timeline.Now()); err != nil {
			return err
		}
		domain.RecomputeStreamWatermark(stream, streamSegments(state, stream.ID))
		domain.RecordAudit(
			state,
			"compaction.plan.started",
			stream.ID,
			plan.ID,
			plan.Revision,
			timeline.Now(),
		)
		result = CompactionPlanResult{
			Plan: clonePlan(plan),
		}
		return nil
	})
	return result, err
}

func (s *Service) FinishCompactionPlan(
	ctx context.Context,
	command FinishCompactionPlanCommand,
) (CompactionPlanResult, error) {
	var result CompactionPlanResult
	err := s.mutate(ctx, "compaction.plan.finish", func(state *domain.State, timeline domainTimeline) error {
		planID := normalizedIdentifier(command.PlanID)
		plan := state.Plans[planID]
		if plan == nil {
			return resourceNotFound("compaction plan", planID)
		}
		stream := state.Streams[plan.StreamID]
		if stream == nil {
			return domain.NewError(domain.CodeInconsistentState, "plan stream was not found")
		}
		alreadyFinished := plan.State == domain.PlanStateCompleted
		resultSegmentID := s.ids.New("seg")
		if state.Segments[resultSegmentID] != nil {
			return domain.NewError(domain.CodeConflict, "generated segment identifier is already in use")
		}
		segment, err := domain.FinishCompactionPlan(
			plan,
			stream,
			state.Segments,
			resultSegmentID,
			domain.FinishCompactionInput{
				ResultChecksum:  command.ResultChecksum,
				ResultSizeBytes: command.ResultSizeBytes,
			},
			timeline.Now(),
		)
		if err != nil {
			return err
		}
		if _, exists := state.Segments[segment.ID]; !exists {
			state.Segments[segment.ID] = segment
		}
		domain.RecomputeStreamWatermark(stream, streamSegments(state, stream.ID))
		if !alreadyFinished {
			domain.RecordAudit(
				state,
				"compaction.plan.completed",
				stream.ID,
				plan.ID,
				plan.Revision,
				timeline.Now(),
			)
		}
		result = CompactionPlanResult{
			Plan:            clonePlan(plan),
			ResultSegment:   cloneSegment(segment),
			AlreadyFinished: alreadyFinished,
		}
		return nil
	})
	return result, err
}

func (s *Service) AcknowledgeReplica(
	ctx context.Context,
	command AcknowledgeReplicaCommand,
) (AcknowledgeReplicaResult, error) {
	var result AcknowledgeReplicaResult
	err := s.mutate(ctx, "replica.acknowledge", func(state *domain.State, timeline domainTimeline) error {
		segmentID := normalizedIdentifier(command.SegmentID)
		segment := state.Segments[segmentID]
		if segment == nil {
			return resourceNotFound("segment", segmentID)
		}
		stream := state.Streams[segment.StreamID]
		if stream == nil {
			return domain.NewError(domain.CodeInconsistentState, "segment stream was not found")
		}
		ack, changed, err := domain.ApplyReplicaAck(
			stream,
			segment,
			domain.ReplicaAckInput{
				NodeID:     command.NodeID,
				Generation: command.Generation,
				Offset:     command.Offset,
			},
			timeline.Now(),
		)
		if err != nil {
			return err
		}
		if changed {
			domain.RecomputeStreamWatermark(stream, streamSegments(state, stream.ID))
			domain.RecordAudit(
				state,
				"replica.acknowledged",
				stream.ID,
				segment.ID,
				stream.Revision,
				timeline.Now(),
			)
		}
		result = AcknowledgeReplicaResult{
			Ack:       ack,
			Changed:   changed,
			Watermark: stream.Watermark,
			StreamID:  stream.ID,
		}
		return nil
	})
	return result, err
}

// replayCompactionPlan handles a retried plan creation. An active
// (proposed or running) plan with the exact same source set and expected
// stream revision is treated as an idempotent replay and returned. A request
// overlapping an existing active plan in any other way is a genuine conflict.
// Plans already completed or aborted do not block: their sources are no
// longer sealed and are rejected by the normal validation path.
func replayCompactionPlan(
	state *domain.State,
	stream *domain.Stream,
	sourceSegmentIDs []string,
	expectedRevision uint64,
) (*domain.CompactionPlan, error, error) {
	planIDs := make([]string, 0, len(state.Plans))
	for id, plan := range state.Plans {
		if plan != nil && plan.StreamID == stream.ID &&
			(plan.State == domain.PlanStateProposed || plan.State == domain.PlanStateRunning) {
			planIDs = append(planIDs, id)
		}
	}
	sort.Strings(planIDs)

	for _, id := range planIDs {
		plan := state.Plans[id]
		if sameStringSet(plan.SourceSegmentIDs, sourceSegmentIDs) {
			if plan.ExpectedStreamRevision == expectedRevision {
				return plan, nil, nil
			}
			return nil, domain.NewError(
				domain.CodeConflict,
				"an active plan for these sources uses a different stream revision",
			).WithDetail("plan_id", plan.ID), nil
		}
		for _, segmentID := range sourceSegmentIDs {
			for _, existingID := range plan.SourceSegmentIDs {
				if segmentID == existingID {
					return nil, domain.NewError(
						domain.CodeConflict,
						"source segment is already part of an active compaction plan",
					).
						WithDetail("plan_id", plan.ID).
						WithDetail("segment_id", segmentID), nil
				}
			}
		}
	}
	return nil, nil, nil
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		seen[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func cloneStream(source *domain.Stream) *domain.Stream {
	state := domain.CloneState(&domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams: map[string]*domain.Stream{
			source.ID: source,
		},
		Segments: map[string]*domain.Segment{},
		Plans:    map[string]*domain.CompactionPlan{},
	})
	return state.Streams[source.ID]
}

func cloneSegment(source *domain.Segment) *domain.Segment {
	if source == nil {
		return nil
	}
	state := domain.CloneState(&domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments: map[string]*domain.Segment{
			source.ID: source,
		},
		Plans: map[string]*domain.CompactionPlan{},
	})
	return state.Segments[source.ID]
}

func clonePlan(source *domain.CompactionPlan) *domain.CompactionPlan {
	if source == nil {
		return nil
	}
	state := domain.CloneState(&domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments:      map[string]*domain.Segment{},
		Plans: map[string]*domain.CompactionPlan{
			source.ID: source,
		},
	})
	return state.Plans[source.ID]
}
