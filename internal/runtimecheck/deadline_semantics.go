package runtimecheck

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

// runDeadlineSemantics verifies the external cancellation/timeout contract
// and the single deadline boundary across processing start, serialization
// gate wait, and persistence. It is also exercised by the
// "deadline-semantics" verify command.
func runDeadlineSemantics(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newDeadlineHarness(3 * time.Second)
	if err != nil {
		return err
	}
	defer h.close()

	if err := deadlineStartBoundaries(ctx, h, assertions); err != nil {
		return err
	}
	if err := deadlinePersistenceTimeout(ctx, h, assertions); err != nil {
		return err
	}
	if err := deadlineGateWaitTimeout(ctx, h, assertions); err != nil {
		return err
	}
	if err := deadlineLateCommitIdempotency(ctx, h, assertions); err != nil {
		return err
	}
	if err := deadlinePlanCreateReplay(ctx, h, assertions); err != nil {
		return err
	}
	if err := deadlineErrorClassifications(ctx, h, assertions); err != nil {
		return err
	}
	return nil
}

// A request that is already canceled and a request whose server-side
// deadline has already passed must be rejected before processing with the
// stable cancellation/timeout codes rather than invalid_input.
func deadlineStartBoundaries(
	_ context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	createBody := map[string]any{
		"namespace":       "field-lab",
		"name":            "start-boundary",
		"threshold_bytes": 64,
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledResult, err := h.record(canceled, http.MethodPost, "/v1/streams", createBody)
	if err != nil {
		return err
	}
	if err := assertions.require(
		canceledResult.status == http.StatusRequestTimeout,
		"canceled request at start returned status %d: %s",
		canceledResult.status,
		canceledResult.body,
	); err != nil {
		return err
	}
	if err := assertions.require(
		canceledResult.Error.Code == string(domain.CodeRequestCanceled),
		"canceled request at start code is %q",
		canceledResult.Error.Code,
	); err != nil {
		return err
	}

	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()
	expiredResult, err := h.record(expired, http.MethodPost, "/v1/streams", createBody)
	if err != nil {
		return err
	}
	if err := assertions.require(
		expiredResult.status == http.StatusGatewayTimeout,
		"expired request at start returned status %d: %s",
		expiredResult.status,
		expiredResult.body,
	); err != nil {
		return err
	}
	if err := assertions.require(
		expiredResult.Error.Code == string(domain.CodeRequestTimeout),
		"expired request at start code is %q",
		expiredResult.Error.Code,
	); err != nil {
		return err
	}

	// Neither rejected request may have published any state.
	disk, err := h.loadedState()
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(disk.Streams) == 0,
		"pre-processing deadline rejections published state: %d streams",
		len(disk.Streams),
	); err != nil {
		return err
	}

	// A normal request through the same boundary still succeeds.
	created, err := h.do(
		context.Background(),
		http.MethodPost,
		"/v1/streams",
		map[string]any{
			"namespace":       "field-lab",
			"name":            "normal-flow",
			"threshold_bytes": 64,
		},
	)
	if err != nil {
		return err
	}
	if err := assertions.require(
		created.status == http.StatusCreated,
		"normal create returned status %d: %s",
		created.status,
		created.body,
	); err != nil {
		return err
	}
	return nil
}

// Slow storage that honors cancellation: the deadline lands during the
// persistence phase, the request fails with request_timeout, and no
// snapshot is published either in memory, on disk, or after restart.
func deadlinePersistenceTimeout(
	ctx context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	streamID, err := deadlineCreateStream(h, "persistence-timeout")
	if err != nil {
		return err
	}

	h.slow.configure(modeInterruptible, 400*time.Millisecond, nil)

	// Impose the short deadline on the server so the client stays connected
	// and can observe the 504 status instead of getting a transport abort.
	h.setRequestTimeout(80 * time.Millisecond)
	result, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 1,
			"record_count":      1,
			"payload_bytes":     10,
		},
	)
	if err != nil {
		return err
	}
	if err := assertions.require(
		result.status == http.StatusGatewayTimeout,
		"timed-out persistence returned status %d: %s",
		result.status,
		result.body,
	); err != nil {
		return err
	}
	if err := assertions.require(
		result.Error.Code == string(domain.CodeRequestTimeout),
		"timed-out persistence code is %q",
		result.Error.Code,
	); err != nil {
		return err
	}

	// The serialization gate must have been released: a subsequent normal
	// append runs without hanging.
	h.setRequestTimeout(3 * time.Second)
	h.slow.configure(modeNormal, 0, nil)
	if _, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 1,
			"record_count":      1,
			"payload_bytes":     10,
		},
	); err != nil {
		return err
	}

	if err := deadlineRequireStreamRevision(h, streamID, 2, assertions,
		"timed-out persistence leaked an append"); err != nil {
		return err
	}

	if err := h.restart(); err != nil {
		return err
	}
	if err := deadlineRequireStreamRevision(h, streamID, 2, assertions,
		"restart read after timed-out persistence disagrees"); err != nil {
		return err
	}
	return nil
}

// A request whose deadline elapses while queued behind an in-flight commit
// must fail at the serialization gate without running or publishing.
func deadlineGateWaitTimeout(
	ctx context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	streamID, err := deadlineCreateStream(h, "gate-wait")
	if err != nil {
		return err
	}

	release := make(chan struct{})
	h.slow.configure(modeHold, 0, release)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := h.do(ctx, http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": 1,
				"record_count":      1,
				"payload_bytes":     10,
			},
		)
		leaderDone <- err
	}()
	if err := waitForSaveStarted(h.slow, time.Second); err != nil {
		return err
	}

	// The queued request carries the short deadline. It is driven
	// in-process so its 504 response remains observable after the client
	// context expires, while the leader keeps the generous server
	// deadline and stays in-flight holding the gate.
	queuedCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	queued, err := h.record(queuedCtx, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 1,
			"record_count":      1,
			"payload_bytes":     20,
		},
	)
	cancel()
	if err != nil {
		return err
	}
	if err := assertions.require(
		queued.status == http.StatusGatewayTimeout,
		"gate-wait timeout returned status %d: %s",
		queued.status,
		queued.body,
	); err != nil {
		return err
	}
	if err := assertions.require(
		queued.Error.Code == string(domain.CodeRequestTimeout),
		"gate-wait timeout code is %q",
		queued.Error.Code,
	); err != nil {
		return err
	}

	close(release)
	h.slow.configure(modeNormal, 0, nil)
	if err := <-leaderDone; err != nil {
		return err
	}

	// Only the leader append landed (revision 1 -> 2).
	if err := deadlineRequireStreamRevision(h, streamID, 2, assertions,
		"gate-wait timeout leaked a queued append"); err != nil {
		return err
	}

	// Replaying the failed queued append now conflicts on the stale
	// revision, proving it never applied and keeping conflict semantics.
	replayed, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 1,
			"record_count":      1,
			"payload_bytes":     20,
		},
	)
	if err != nil {
		return err
	}
	if err := assertions.require(
		replayed.status == http.StatusConflict &&
			replayed.Error.Code == string(domain.CodeStaleRevision),
		"stale replay after gate wait returned %d %s",
		replayed.status,
		replayed.body,
	); err != nil {
		return err
	}
	return nil
}

// Slow storage that ignores cancellation: the commit lands just after the
// deadline. The caller sees no timeout because the committed result is
// published, and a retry replays the same result instead of duplicating it.
func deadlineLateCommitIdempotency(
	ctx context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	streamID, sources, err := deadlineTwoSealedSegments(h, "late-commit")
	if err != nil {
		return err
	}
	// Revision after create + four appends is 5.
	planCreated, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sources,
			"expected_stream_revision": 5,
		},
	)
	if err != nil {
		return err
	}
	var plan application.CompactionPlanResult
	if err := jsonDecode(planCreated.body, &plan); err != nil {
		return err
	}
	started, err := h.do(ctx, http.MethodPost,
		"/v1/compaction-plans/"+plan.Plan.ID+"/start", map[string]any{})
	if err != nil {
		return err
	}
	_ = started

	// Finish lands late: storage sleeps through the short deadline, commits
	// durably, and reports the context cancellation. The service must still
	// publish the committed result and never advertise a timeout for it.
	finishCommand := application.FinishCompactionPlanCommand{
		PlanID:          plan.Plan.ID,
		ResultChecksum:  "sha256:late",
		ResultSizeBytes: 80,
	}
	h.slow.configure(modeCommitThenReportCancel, 150*time.Millisecond, nil)
	finishCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	finished, finishErr := h.service.FinishCompactionPlan(finishCtx, finishCommand)
	cancel()
	if err := assertions.require(
		finishErr == nil,
		"late committed finish was reported as an error: %v",
		finishErr,
	); err != nil {
		return err
	}
	if err := assertions.require(
		finished.Plan != nil && finished.Plan.State == domain.PlanStateCompleted &&
			finished.ResultSegment != nil,
		"late committed finish result is incomplete: %+v",
		finished,
	); err != nil {
		return err
	}

	// Current read sees the completed plan with exactly one result segment.
	h.slow.configure(modeNormal, 0, nil)
	planRead, err := h.do(ctx, http.MethodGet,
		"/v1/compaction-plans/"+plan.Plan.ID, nil)
	if err != nil {
		return err
	}
	var currentPlan domain.CompactionPlan
	if err := jsonDecode(planRead.body, &currentPlan); err != nil {
		return err
	}
	if err := assertions.require(
		currentPlan.State == domain.PlanStateCompleted &&
			currentPlan.ResultChecksum == "sha256:late",
		"current read after late commit is %+v",
		currentPlan,
	); err != nil {
		return err
	}

	// Retrying the identical finish is idempotent and returns the same result.
	retry, err := h.do(ctx, http.MethodPost,
		"/v1/compaction-plans/"+plan.Plan.ID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:late",
			"result_size_bytes": 80,
		},
	)
	if err != nil {
		return err
	}
	var retried application.CompactionPlanResult
	if err := jsonDecode(retry.body, &retried); err != nil {
		return err
	}
	if err := assertions.require(
		retry.status == http.StatusOK &&
			retried.AlreadyFinished &&
			retried.ResultSegment != nil &&
			retried.ResultSegment.ID == currentPlan.ResultSegmentID,
		"finish retry was not idempotent: %s",
		retry.body,
	); err != nil {
		return err
	}

	// A retry with a different payload is a conflict, not a timeout.
	different, err := h.do(ctx, http.MethodPost,
		"/v1/compaction-plans/"+plan.Plan.ID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:different",
			"result_size_bytes": 81,
		},
	)
	if err != nil {
		return err
	}
	if err := assertions.require(
		different.status == http.StatusConflict &&
			different.Error.Code == string(domain.CodeConflict),
		"conflicting finish retry returned %d %s",
		different.status,
		different.body,
	); err != nil {
		return err
	}

	// Restart read still shows exactly one completed result.
	if err := h.restart(); err != nil {
		return err
	}
	restartedPlan, err := h.do(ctx, http.MethodGet,
		"/v1/compaction-plans/"+plan.Plan.ID, nil)
	if err != nil {
		return err
	}
	var afterRestart domain.CompactionPlan
	if err := jsonDecode(restartedPlan.body, &afterRestart); err != nil {
		return err
	}
	if err := assertions.require(
		afterRestart.State == domain.PlanStateCompleted &&
			afterRestart.ResultSegmentID == currentPlan.ResultSegmentID,
		"restart read after late commit is %+v",
		afterRestart,
	); err != nil {
		return err
	}
	return nil
}

// A plan create whose commit lands during the deadline is published; the
// retried create replays the same proposed plan instead of producing a
// second plan or a false validation/timeout error.
func deadlinePlanCreateReplay(
	ctx context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	streamID, sources, err := deadlineTwoSealedSegments(h, "create-replay")
	if err != nil {
		return err
	}

	command := application.CreateCompactionPlanCommand{
		StreamID:               streamID,
		SourceSegmentIDs:       sources,
		ExpectedStreamRevision: 5,
	}
	h.slow.configure(modeCommitThenReportCancel, 150*time.Millisecond, nil)
	createCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	first, firstErr := h.service.CreateCompactionPlan(createCtx, command)
	cancel()
	if err := assertions.require(
		firstErr == nil && first.Plan != nil &&
			first.Plan.State == domain.PlanStateProposed,
		"late committed plan create was not published: %+v err=%v",
		first,
		firstErr,
	); err != nil {
		return err
	}

	// Current read sees the one proposed plan.
	h.slow.configure(modeNormal, 0, nil)
	planRead, err := h.do(ctx, http.MethodGet,
		"/v1/compaction-plans/"+first.Plan.ID, nil)
	if err != nil {
		return err
	}
	if err := assertions.require(
		planRead.status == http.StatusOK,
		"current read of late committed plan returned %d: %s",
		planRead.status,
		planRead.body,
	); err != nil {
		return err
	}

	// The retried create returns the same plan rather than a second one.
	retry, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sources,
			"expected_stream_revision": 5,
		},
	)
	if err != nil {
		return err
	}
	var retried application.CompactionPlanResult
	if err := jsonDecode(retry.body, &retried); err != nil {
		return err
	}
	if err := assertions.require(
		retry.status == http.StatusCreated &&
			retried.Plan != nil && retried.Plan.ID == first.Plan.ID &&
			retried.Plan.State == domain.PlanStateProposed,
		"plan create retry was not idempotent: %s",
		retry.body,
	); err != nil {
		return err
	}

	// Exactly one plan for this stream exists after the retry.
	disk, err := h.loadedState()
	if err != nil {
		return err
	}
	streamPlans := 0
	for _, plan := range disk.Plans {
		if plan != nil && plan.StreamID == streamID {
			streamPlans++
		}
	}
	if err := assertions.require(
		streamPlans == 1,
		"plan create retry produced %d plans for the stream, want 1",
		streamPlans,
	); err != nil {
		return err
	}

	// Restart still exposes the single plan.
	if err := h.restart(); err != nil {
		return err
	}
	reloaded, err := h.do(ctx, http.MethodGet,
		"/v1/compaction-plans/"+first.Plan.ID, nil)
	if err != nil {
		return err
	}
	if err := assertions.require(
		reloaded.status == http.StatusOK,
		"restart read of replayed plan returned %d: %s",
		reloaded.status,
		reloaded.body,
	); err != nil {
		return err
	}
	return nil
}

// Ordinary validation and conflict classifications remain unchanged.
func deadlineErrorClassifications(
	ctx context.Context,
	h *deadlineHarness,
	assertions *counterAssertions,
) error {
	invalid, err := h.do(ctx, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "",
		"name":            "bad",
		"threshold_bytes": 64,
	})
	if err != nil {
		return err
	}
	if err := assertions.require(
		invalid.status == http.StatusBadRequest &&
			invalid.Error.Code == string(domain.CodeInvalidInput),
		"validation error classification changed: %d %s",
		invalid.status,
		invalid.body,
	); err != nil {
		return err
	}

	streamID, err := deadlineCreateStream(h, "conflict-flow")
	if err != nil {
		return err
	}
	stale, err := h.do(ctx, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 99,
			"record_count":      1,
			"payload_bytes":     10,
		},
	)
	if err != nil {
		return err
	}
	if err := assertions.require(
		stale.status == http.StatusConflict &&
			stale.Error.Code == string(domain.CodeStaleRevision),
		"conflict classification changed: %d %s",
		stale.status,
		stale.body,
	); err != nil {
		return err
	}

	missing, err := h.do(ctx, http.MethodGet, "/v1/streams/str_does_not_exist", nil)
	if err != nil {
		return err
	}
	if err := assertions.require(
		missing.status == http.StatusNotFound &&
			missing.Error.Code == string(domain.CodeNotFound),
		"not-found classification changed: %d %s",
		missing.status,
		missing.body,
	); err != nil {
		return err
	}
	return nil
}

func deadlineCreateStream(h *deadlineHarness, name string) (string, error) {
	result, err := h.do(context.Background(), http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "field-lab",
		"name":            name,
		"threshold_bytes": 32,
	})
	if err != nil {
		return "", err
	}
	if result.status != http.StatusCreated {
		return "", fmt.Errorf("create stream returned %d: %s", result.status, result.body)
	}
	var created application.CreateStreamResult
	if err := jsonDecode(result.body, &created); err != nil {
		return "", err
	}
	return created.Stream.ID, nil
}

func deadlineTwoSealedSegments(
	h *deadlineHarness,
	name string,
) (string, []string, error) {
	streamID, err := deadlineCreateStream(h, name)
	if err != nil {
		return "", nil, err
	}
	for index, payload := range []int64{20, 20, 20, 20} {
		result, err := h.do(context.Background(), http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": index + 1,
				"record_count":      2,
				"payload_bytes":     payload,
			},
		)
		if err != nil {
			return "", nil, err
		}
		if result.status != http.StatusOK {
			return "", nil, fmt.Errorf("append returned %d: %s", result.status, result.body)
		}
	}
	listed, err := h.do(context.Background(), http.MethodGet,
		"/v1/streams/"+streamID+"/segments?limit=100", nil)
	if err != nil {
		return "", nil, err
	}
	var segments struct {
		Items []*domain.Segment `json:"items"`
	}
	if err := jsonDecode(listed.body, &segments); err != nil {
		return "", nil, err
	}
	if len(segments.Items) < 3 {
		return "", nil, fmt.Errorf("expected sealed setup segments, got %d", len(segments.Items))
	}
	return streamID, []string{segments.Items[0].ID, segments.Items[1].ID}, nil
}

func deadlineRequireStreamRevision(
	h *deadlineHarness,
	streamID string,
	want uint64,
	assertions *counterAssertions,
	message string,
) error {
	disk, err := h.loadedState()
	if err != nil {
		return err
	}
	stream := disk.Streams[streamID]
	if stream == nil {
		return fmt.Errorf("stream %s missing from durable snapshot", streamID)
	}
	return assertions.require(stream.Revision == want,
		"%s: revision %d want %d", message, stream.Revision, want)
}

func waitForSaveStarted(repo *slowRepository, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		started, _ := repo.stats()
		if started >= 1 {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("slow repository save never started")
}
