/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
	"github.com/argoproj-labs/gitops-promoter/internal/settings"
	promoterConditions "github.com/argoproj-labs/gitops-promoter/internal/types/conditions"
	"github.com/argoproj-labs/gitops-promoter/internal/types/constants"
	"github.com/argoproj-labs/gitops-promoter/internal/utils"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// maxCommitStatusDescriptionLength bounds descriptions derived from Job conditions. 140 matches
// GitHub's commit status API description limit, the most common downstream consumer; other SCM
// providers tolerate longer values, so this is a conservative shared bound rather than a
// per-provider one.
const maxCommitStatusDescriptionLength = 140

// nonAlphanumeric matches runs of characters that are not ASCII letters or digits, used by
// jobName to sanitize the identity string into a DNS-safe stem.
var nonAlphanumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

// JobCommitStatusReconciler reconciles a JobCommitStatus object.
//
// For each environment the gate applies to (resolved from the referenced PromotionStrategy), the
// reconciler creates one Job from spec.jobTemplate for the environment's current SHA (selected by
// spec.reportOn: proposed or active hydrated commit), observes it, and publishes a CommitStatus.
// Jobs are never mutated once created, with one exception: a controller finalizer
// (JobCommitStatusJobFinalizer), added at creation and removed once the Job's terminal outcome has
// been durably recorded in a CommitStatus, so a short spec.jobTemplate.spec.ttlSecondsAfterFinished
// cannot delete the Job before the controller observes it. Jobs are never deleted by the
// controller itself — a superseded Job is left for native Job TTL/retry mechanics and owner
// garbage collection (on parent deletion) to reclaim. Phase is decided in two steps: the Job's own
// Complete/Failed conditions decide whether it has reached a terminal state at all (pending until
// then), and once terminal, spec.success.when.expression is evaluated against the finished Job to
// decide success vs. failure — this lets users distinguish a real failure from an infrastructure
// problem (e.g. ImagePullBackOff) the way Argo Workflows distinguishes Failed from Error. See
// issue #1597.
type JobCommitStatusReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    events.EventRecorder
	SettingsMgr *settings.Manager

	// EnqueueCTP is a function to enqueue CTP reconcile requests without modifying the CTP object.
	EnqueueCTP CTPEnqueueFunc

	// expressionCache caches compiled spec.success.when.expression programs to avoid recompiling on
	// every reconcile. Key: expression string, Value: compiled *vm.Program.
	expressionCache sync.Map
}

// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses,verbs=get;list;watch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/finalizers,verbs=update
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=commitstatuses,verbs=get;list;watch;patch;create;delete
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=promotionstrategies,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;patch

// Reconcile loads the JobCommitStatus and its referenced PromotionStrategy, then for each
// applicable environment: creates a Job for that environment's current SHA if one doesn't already
// exist (see resolveJobSha, ensureJobForEnvironment), observes its Complete/Failed conditions and
// spec.success.when.expression verdict, and publishes a CommitStatus correlated to that SHA. An
// existing Job for the same parent/environment/SHA identity is left untouched — Jobs are created
// at most once and never mutated.
func (r *JobCommitStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling JobCommitStatus", "name", req.Name)
	startTime := time.Now()

	var jcs promoterv1alpha1.JobCommitStatus
	// This function applies the resource status via Server-Side Apply at the end of the reconciliation. Don't write status manually.
	var previousReady *metav1.Condition
	defer utils.HandleReconciliationResult(ctx, startTime, &jcs, r.Client, r.Recorder, constants.JobCommitStatusControllerFieldOwner, &result, &err, &previousReady)

	err = r.Get(ctx, req.NamespacedName, &jcs, &client.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("JobCommitStatus not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to get JobCommitStatus")
		return ctrl.Result{}, fmt.Errorf("failed to get JobCommitStatus %q: %w", req.Name, err)
	}

	// Remove any existing Ready condition. We want to start fresh.
	previousReady = utils.RemoveReadyCondition(&jcs)

	if err := ensureControllerInstanceIDStable(ctx, r.SettingsMgr); err != nil {
		return ctrl.Result{}, err
	}

	// Fetch the referenced PromotionStrategy. HandleReconciliationResult records a Warning event
	// and sets Ready=False when this errors, so a missing reference is surfaced clearly without a
	// panic and without any child resource being created.
	var ps promoterv1alpha1.PromotionStrategy
	psKey := client.ObjectKey{
		Namespace: jcs.Namespace,
		Name:      jcs.Spec.PromotionStrategyRef.Name,
	}
	err = r.Get(ctx, psKey, &ps)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("referenced PromotionStrategy %q not found: %w", jcs.Spec.PromotionStrategyRef.Name, err)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get PromotionStrategy %q: %w", jcs.Spec.PromotionStrategyRef.Name, err)
	}

	// The parent-gate label key depends on the resolved Kind (promoter.argoproj.io/job-commit-status),
	// so resolve it once and reuse it for both the reserved-label check and the identity labels below.
	parentLabelKey := utils.CommitStatusGateLabelKeyForParent(&jcs)
	if err := validateNoReservedJobLabels(&jcs.Spec.JobTemplate, parentLabelKey); err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid spec.jobTemplate: %w", err)
	}

	applicableEnvs := utils.GetApplicableEnvironments(&ps, jcs.Spec.Key, jcs.Spec.ReportOn)
	logger.Info("Resolved applicable environments for JobCommitStatus",
		"promotionStrategy", ps.Name,
		"key", jcs.Spec.Key,
		"reportOn", jcs.Spec.ReportOn,
		"environmentCount", len(applicableEnvs))

	envStatusMap := make(map[string]*promoterv1alpha1.EnvironmentStatus, len(ps.Status.Environments))
	for i := range ps.Status.Environments {
		envStatusMap[ps.Status.Environments[i].Branch] = &ps.Status.Environments[i]
	}

	// Captured before newEnvStatuses overwrites jcs.Status.Environments below, so we can tell which
	// environments actually changed (phase or SHA) and only enqueue ChangeTransferPolicy for those.
	previousEnvByBranch := make(map[string]promoterv1alpha1.JobCommitStatusEnvironmentStatus, len(jcs.Status.Environments))
	for _, envStatus := range jcs.Status.Environments {
		previousEnvByBranch[envStatus.Branch] = envStatus
	}

	newEnvStatuses := make([]promoterv1alpha1.JobCommitStatusEnvironmentStatus, 0, len(applicableEnvs))
	commitStatuses := make([]*promoterv1alpha1.CommitStatus, 0, len(applicableEnvs))
	changedEnvironments := make([]string, 0, len(applicableEnvs))
	// Job-creation failures (API errors, including RBAC/forbidden) are transient-or-fixable: collect
	// them and, once every environment has otherwise been processed, return them so the standard
	// rate-limited retry applies and Ready=False surfaces the problem — without letting one
	// environment's failure stop the others from being processed in this same reconcile.
	var jobErrs []error
	for _, env := range applicableEnvs {
		envStatus, found := envStatusMap[env.Branch]
		if !found {
			return ctrl.Result{}, fmt.Errorf("environment %q not found in PromotionStrategy status", env.Branch)
		}

		sha := resolveJobSha(envStatus, jcs.Spec.ReportOn)
		if sha == "" {
			return ctrl.Result{}, fmt.Errorf("no SHA available for environment %q (reportOn: %q): PromotionStrategy may not be fully reconciled", env.Branch, jcs.Spec.ReportOn)
		}

		// Once a terminal outcome for this exact SHA has already been durably recorded, don't ask
		// ensureJobForEnvironment about it again: the Job's finalizer was removed specifically so it
		// could be cleaned up (ttlSecondsAfterFinished, or a manual delete), and re-deriving "no Job
		// exists" as "create one" here would recreate an identical Job forever, defeating that
		// cleanup entirely. Our own status is the durable answer once terminal.
		if prev, ok := previousEnvByBranch[env.Branch]; ok && prev.Sha == sha && isTerminalCommitPhase(prev.Phase) {
			newEnvStatuses = append(newEnvStatuses, prev)
			csName := utils.CommitStatusResourceName(ctx, &jcs, env.Branch)
			var existingCS promoterv1alpha1.CommitStatus
			if getErr := r.Get(ctx, client.ObjectKey{Name: csName, Namespace: jcs.Namespace}, &existingCS); getErr == nil {
				commitStatuses = append(commitStatuses, &existingCS)
			} else if !k8serrors.IsNotFound(getErr) {
				return ctrl.Result{}, fmt.Errorf("failed to get existing CommitStatus for environment %q: %w", env.Branch, getErr)
			}
			continue
		}

		envState := promoterv1alpha1.JobCommitStatusEnvironmentStatus{
			Branch: env.Branch,
			Sha:    sha,
		}

		var description string
		job, ensureErr := r.ensureJobForEnvironment(ctx, &jcs, &ps, parentLabelKey, env.Branch, sha)
		if ensureErr != nil {
			logger.Error(ensureErr, "failed to ensure Job for environment", "branch", env.Branch, "sha", sha)
			r.Recorder.Eventf(&jcs, nil, "Warning", "JobCreateFailed", "Reconciling",
				"failed to create Job for environment %q at sha %q: %v", env.Branch, sha, ensureErr)
			envState.Phase = promoterv1alpha1.CommitPhasePending
			envState.Reason = "JobCreateFailed"
			description = boundedDescription(fmt.Sprintf("failed to create Job: %v", ensureErr))
			jobErrs = append(jobErrs, fmt.Errorf("environment %q: %w", env.Branch, ensureErr))
		} else {
			envState.JobRef = &promoterv1alpha1.JobCommitStatusJobReference{Name: job.Name, Namespace: job.Namespace}
			if !job.CreationTimestamp.IsZero() {
				startedAt := job.CreationTimestamp
				envState.StartedAt = &startedAt
			}
			envState.Phase, envState.Reason, description, envState.FinishedAt = r.observeJob(ctx, &jcs, job, env.Branch)
		}

		previousPhase := previousEnvByBranch[env.Branch].Phase
		emitCommitStatusPhaseChangedEvent(r.Recorder, &jcs, jcs.Spec.Key, env.Branch, string(previousPhase), string(envState.Phase))
		if previousPhase != envState.Phase || previousEnvByBranch[env.Branch].Sha != sha {
			changedEnvironments = append(changedEnvironments, env.Branch)
		}

		cs, upsertErr := utils.UpsertCommitStatus(ctx, r.Client, utils.UpsertCommitStatusParams{
			Parent:      &jcs,
			RepoRefName: ps.Spec.RepositoryReference.Name,
			Branch:      env.Branch,
			Sha:         sha,
			Key:         jcs.Spec.Key,
			Description: description,
			Phase:       envState.Phase,
			FieldOwner:  constants.JobCommitStatusControllerFieldOwner,
		})
		if upsertErr != nil {
			return ctrl.Result{}, fmt.Errorf("failed to upsert CommitStatus for environment %q: %w", env.Branch, upsertErr)
		}
		commitStatuses = append(commitStatuses, cs)

		// The CommitStatus write above just durably recorded this outcome, so once it's terminal the
		// Job's finalizer can safely come off: nothing about this Job can change our answer for it
		// again, so there's no need to keep blocking ttlSecondsAfterFinished cleanup on our behalf.
		if job != nil && isTerminalCommitPhase(envState.Phase) {
			if err := r.removeJobFinalizer(ctx, job); err != nil {
				logger.Error(err, "failed to remove finalizer from terminal Job", "job", job.Name, "branch", env.Branch)
				r.Recorder.Eventf(&jcs, nil, "Warning", "JobFinalizerRemoveFailed", "Reconciling",
					"failed to remove finalizer from terminal Job %q for environment %q: %v", job.Name, env.Branch, err)
			}
		}

		newEnvStatuses = append(newEnvStatuses, envState)
	}
	jcs.Status.Environments = newEnvStatuses

	if err := utils.CleanupOrphanedCommitStatuses(ctx, r.Client, r.Recorder, &jcs, commitStatuses); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to cleanup orphaned CommitStatus resources: %w", err)
	}

	utils.InheritNotReadyConditionFromObjects(&jcs, promoterConditions.CommitStatusesNotReady, commitStatuses...)

	utils.EnqueueChangeTransferPolicies(ctx, r.EnqueueCTP, &ps, changedEnvironments, "job status observed")

	if len(jobErrs) > 0 {
		return ctrl.Result{}, errors.Join(jobErrs...)
	}

	return ctrl.Result{}, nil
}

// observeJob decides the CommitStatus phase, reason, description, and finish time for a Job.
//
// The Job's own Complete/Failed conditions decide terminality only: no terminal condition means
// pending. Once terminal, spec.success.when.expression is evaluated against the finished Job to
// decide success vs. failure (never inferred directly from which condition fired), so users can
// distinguish a real failure from an infrastructure problem in the expression itself. Conflicting
// Complete=True and Failed=True conditions are treated as a failure and reported with a warning
// event, since that combination should never happen and should not silently pass as success.
//
// finishedAt is the terminal condition's own LastTransitionTime, not the reconcile time, so it
// stays stable across reconciles as long as the Job's conditions don't change (see acceptance
// criteria: idempotent reconciles must not churn resource versions via fresh timestamps).
func (r *JobCommitStatusReconciler) observeJob(
	ctx context.Context,
	jcs *promoterv1alpha1.JobCommitStatus,
	job *batchv1.Job,
	branch string,
) (phase promoterv1alpha1.CommitStatusPhase, reason, description string, finishedAt *metav1.Time) {
	logger := log.FromContext(ctx)

	completeCond, failedCond := jobTerminalConditions(job)
	switch {
	case completeCond != nil && failedCond != nil:
		logger.Info("Job reports conflicting Complete and Failed conditions", "job", job.Name, "branch", branch)
		r.Recorder.Eventf(jcs, nil, "Warning", "JobConditionConflict", "Reconciling",
			"Job %q for environment %q reports both Complete=True and Failed=True conditions", job.Name, branch)
		return promoterv1alpha1.CommitPhaseFailure, "ConflictingJobConditions",
			boundedDescription(fmt.Sprintf("Job %s reported conflicting Complete and Failed conditions", job.Name)), nil

	case completeCond == nil && failedCond == nil:
		return promoterv1alpha1.CommitPhasePending, "JobRunning",
			boundedDescription(fmt.Sprintf("Job %s is running", job.Name)), nil
	}

	terminalCond := completeCond
	if terminalCond == nil {
		terminalCond = failedCond
	}
	if !terminalCond.LastTransitionTime.IsZero() {
		t := terminalCond.LastTransitionTime
		finishedAt = &t
	}

	success, evalErr := r.evaluateSuccessExpression(jcs.Spec.Success.When.Expression, job)
	if evalErr != nil {
		logger.Error(evalErr, "failed to evaluate success.when.expression", "job", job.Name, "branch", branch)
		r.Recorder.Eventf(jcs, nil, "Warning", "SuccessExpressionError", "Reconciling",
			"failed to evaluate success.when.expression for environment %q: %v", branch, evalErr)
		return promoterv1alpha1.CommitPhaseFailure, "SuccessExpressionError", boundedDescription(fmt.Sprintf("failed to evaluate success.when.expression: %v", evalErr)), finishedAt
	}

	phase = promoterv1alpha1.CommitPhaseFailure
	if success {
		phase = promoterv1alpha1.CommitPhaseSuccess
	}
	return phase, terminalCond.Reason, boundedDescription(jobConditionDescription(terminalCond)), finishedAt
}

// jobTerminalConditions returns the Job's Complete and Failed conditions when their Status is
// True, or nil for either that isn't present/true.
func jobTerminalConditions(job *batchv1.Job) (completeCond, failedCond *batchv1.JobCondition) {
	for i := range job.Status.Conditions {
		cond := &job.Status.Conditions[i]
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			completeCond = cond
		case batchv1.JobFailed:
			failedCond = cond
		default:
		}
	}
	return completeCond, failedCond
}

// jobConditionDescription formats a terminal Job condition's reason/message into human-readable text.
func jobConditionDescription(cond *batchv1.JobCondition) string {
	switch {
	case cond.Reason != "" && cond.Message != "":
		return fmt.Sprintf("%s: %s", cond.Reason, cond.Message)
	case cond.Reason != "":
		return cond.Reason
	case cond.Message != "":
		return cond.Message
	default:
		return string(cond.Type)
	}
}

// boundedDescription truncates a CommitStatus description to maxCommitStatusDescriptionLength.
func boundedDescription(s string) string {
	return utils.TruncateString(s, maxCommitStatusDescriptionLength)
}

// jobExprEnv is the type-hint environment used to compile spec.success.when.expression: a JSON
// round-trip of a batchv1.Job (so expressions use JSON/lowercase field paths matching the
// documented examples, e.g. "Job.status.succeeded", not Go field names).
type jobExprEnv struct {
	Job map[string]any `expr:"Job"`
}

// getCompiledSuccessExpression retrieves a compiled expression from the cache or compiles and
// caches it. This avoids recompiling the same expression on every reconciliation.
func (r *JobCommitStatusReconciler) getCompiledSuccessExpression(expression string) (*vm.Program, error) {
	if cached, ok := r.expressionCache.Load(expression); ok {
		program, ok := cached.(*vm.Program)
		if !ok {
			return nil, errors.New("cached value is not a *vm.Program")
		}
		return program, nil
	}

	program, err := expr.Compile(expression, expr.Env(jobExprEnv{}), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression: %w", err)
	}

	r.expressionCache.Store(expression, program)
	return program, nil
}

// jobToExprMap converts a Job into the JSON-shaped map passed to success.when.expression as Job.
//
// batchv1.JobStatus's Active/Succeeded/Failed counters are declared `json:",omitempty"`, so a
// plain json.Marshal drops them whenever they're zero (for example a freshly-Failed Job legitimately
// has Succeeded==0). Left alone, the documented default expression "Job.status.succeeded >= 1"
// would compare against a missing map key instead of 0 and error out rather than evaluate to
// false. Restore the true values after the JSON round-trip so those comparisons behave as
// documented regardless of whether the counters happen to be zero.
func jobToExprMap(job *batchv1.Job) (map[string]any, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Job for expression evaluation: %w", err)
	}
	var jobMap map[string]any
	if err := json.Unmarshal(data, &jobMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Job for expression evaluation: %w", err)
	}

	status, _ := jobMap["status"].(map[string]any)
	if status == nil {
		status = map[string]any{}
		jobMap["status"] = status
	}
	status["active"] = job.Status.Active
	status["succeeded"] = job.Status.Succeeded
	status["failed"] = job.Status.Failed

	return jobMap, nil
}

// evaluateSuccessExpression evaluates spec.success.when.expression against the finished Job,
// converted to a JSON-shaped map so expressions can use the documented JSON/lowercase field paths
// (e.g. "Job.status.succeeded") instead of Go field names.
func (r *JobCommitStatusReconciler) evaluateSuccessExpression(expression string, job *batchv1.Job) (bool, error) {
	program, err := r.getCompiledSuccessExpression(expression)
	if err != nil {
		return false, err
	}

	jobMap, err := jobToExprMap(job)
	if err != nil {
		return false, err
	}

	output, err := expr.Run(program, jobExprEnv{Job: jobMap})
	if err != nil {
		return false, fmt.Errorf("failed to evaluate expression: %w", err)
	}

	result, ok := output.(bool)
	if !ok {
		return false, fmt.Errorf("expression must return boolean, got %T", output)
	}
	return result, nil
}

// isTerminalCommitPhase reports whether phase is a terminal outcome (success or failure) that,
// once durably recorded for a given SHA, will never be recomputed for that same SHA again.
func isTerminalCommitPhase(phase promoterv1alpha1.CommitStatusPhase) bool {
	return phase == promoterv1alpha1.CommitPhaseSuccess || phase == promoterv1alpha1.CommitPhaseFailure
}

// resolveJobSha returns the SHA to create the Job against, based on spec.reportOn: "active" uses
// the active hydrated commit (post-promotion re-check); anything else (including the "proposed"
// default) uses the proposed hydrated commit (pre-promotion gate). Mirrors
// internal/webrequest.resolveReportedSha.
func resolveJobSha(envStatus *promoterv1alpha1.EnvironmentStatus, reportOn string) string {
	if reportOn == constants.CommitRefActive {
		return envStatus.Active.Hydrated.Sha
	}
	return envStatus.Proposed.Hydrated.Sha
}

// reservedJobLabelKeys returns the label keys the controller manages on every Job it creates:
// the parent-gate label, the environment label, and the hydrated-SHA label. Together they form
// the identity used to detect an already-created Job for a given parent/environment/SHA tuple.
func reservedJobLabelKeys(parentLabelKey string) [3]string {
	return [3]string{parentLabelKey, promoterv1alpha1.EnvironmentLabel, promoterv1alpha1.JobCommitStatusShaLabel}
}

// validateNoReservedJobLabels rejects a jobTemplate that sets any of the controller-reserved
// labels itself. These labels are how the controller detects an already-created Job (see
// ensureJobForEnvironment), so a user-supplied value would either be silently overwritten or,
// worse, corrupt the identity used to decide whether a Job already exists.
func validateNoReservedJobLabels(jobTemplate *batchv1.JobTemplateSpec, parentLabelKey string) error {
	for _, key := range reservedJobLabelKeys(parentLabelKey) {
		if _, exists := jobTemplate.Labels[key]; exists {
			return fmt.Errorf("jobTemplate.metadata.labels sets reserved label %q, which is managed by the controller", key)
		}
	}
	return nil
}

// ensureJobForEnvironment returns the Job for the given parent/branch/sha identity, creating it
// from spec.jobTemplate if it does not already exist. Existence is decided by listing Jobs in the
// JobCommitStatus's namespace that carry all three identity labels (not by computed name), because
// the computed name is truncated to fit Kubernetes' label-value limit and so cannot be trusted
// alone to prove non-existence for a different SHA. Label-matching candidates are further filtered
// to those actually owned (metav1.IsControlledBy) by this JobCommitStatus: labels alone are
// attacker- or accident-controlled (anyone who can create a Job in this namespace can set them),
// so trusting a label match without an ownership check would let an unrelated or spoofed Job
// masquerade as this environment's result. An already-created, owned Job is returned as-is and
// never mutated except for JobCommitStatusJobFinalizer (see removeJobFinalizer).
func (r *JobCommitStatusReconciler) ensureJobForEnvironment(
	ctx context.Context,
	jcs *promoterv1alpha1.JobCommitStatus,
	ps *promoterv1alpha1.PromotionStrategy,
	parentLabelKey, branch, sha string,
) (*batchv1.Job, error) {
	identityLabels := map[string]string{
		parentLabelKey:                           utils.KubeSafeLabel(jcs.Name),
		promoterv1alpha1.EnvironmentLabel:        utils.KubeSafeLabel(branch),
		promoterv1alpha1.JobCommitStatusShaLabel: sha,
	}

	var existing batchv1.JobList
	if err := r.List(ctx, &existing, client.InNamespace(jcs.Namespace), client.MatchingLabels(identityLabels)); err != nil {
		return nil, fmt.Errorf("failed to list existing Jobs: %w", err)
	}
	owned := make([]batchv1.Job, 0, len(existing.Items))
	for _, job := range existing.Items {
		if metav1.IsControlledBy(&job, jcs) {
			owned = append(owned, job)
		} else {
			log.FromContext(ctx).Info("ignoring Job matching identity labels but not owned by this JobCommitStatus",
				"job", job.Name, "branch", branch, "sha", sha)
		}
	}
	if len(owned) > 0 {
		if len(owned) > 1 {
			log.FromContext(ctx).Info("multiple owned Jobs found for the same parent/environment/sha identity; using the oldest",
				"branch", branch, "sha", sha, "count", len(owned))
			slices.SortFunc(owned, func(a, b batchv1.Job) int {
				return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
			})
		}
		return &owned[0], nil
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName(jcs.Name, branch, sha),
			Namespace:   jcs.Namespace,
			Labels:      mergeStringMaps(jcs.Spec.JobTemplate.Labels, identityLabels),
			Annotations: jcs.Spec.JobTemplate.Annotations,
			Finalizers:  []string{promoterv1alpha1.JobCommitStatusJobFinalizer},
		},
		Spec: *jcs.Spec.JobTemplate.Spec.DeepCopy(),
	}
	injectPromoterJobEnv(&job.Spec.Template.Spec, sha, branch, ps.Name, ps.Spec.RepositoryReference.Name)

	if err := controllerutil.SetControllerReference(jcs, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference: %w", err)
	}

	if err := r.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to create Job: %w", err)
	}
	return job, nil
}

// removeJobFinalizer removes JobCommitStatusJobFinalizer from job via a merge patch (not a full
// Update, so this only ever touches metadata.finalizers) once its terminal outcome has been
// durably recorded. A no-op, with no API call, if the finalizer is already absent — keeping
// repeated reconciles of an already-terminal Job from churning its resourceVersion.
func (r *JobCommitStatusReconciler) removeJobFinalizer(ctx context.Context, job *batchv1.Job) error {
	if !controllerutil.ContainsFinalizer(job, promoterv1alpha1.JobCommitStatusJobFinalizer) {
		return nil
	}
	original := job.DeepCopy()
	controllerutil.RemoveFinalizer(job, promoterv1alpha1.JobCommitStatusJobFinalizer)
	if err := r.Patch(ctx, job, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("failed to remove finalizer from Job %q: %w", job.Name, err)
	}
	return nil
}

// promoterJobEnvVars returns the documented context environment variables added to every
// container and init container of a Job created for a proposed (or active) commit. These are
// plain env vars, not a template language: the values are the exact strings the controller
// resolved, with no further substitution performed by the running container.
func promoterJobEnvVars(sha, branch, promotionStrategyName, repositoryRefName string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "PROMOTER_JOB_SHA", Value: sha},
		{Name: "PROMOTER_JOB_BRANCH", Value: branch},
		{Name: "PROMOTER_JOB_PROMOTION_STRATEGY", Value: promotionStrategyName},
		{Name: "PROMOTER_JOB_REPOSITORY", Value: repositoryRefName},
	}
}

// injectPromoterJobEnv appends the documented context environment variables to every container
// and init container in podSpec.
func injectPromoterJobEnv(podSpec *corev1.PodSpec, sha, branch, promotionStrategyName, repositoryRefName string) {
	envVars := promoterJobEnvVars(sha, branch, promotionStrategyName, repositoryRefName)
	for i := range podSpec.Containers {
		podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, envVars...)
	}
	for i := range podSpec.InitContainers {
		podSpec.InitContainers[i].Env = append(podSpec.InitContainers[i].Env, envVars...)
	}
}

// mergeStringMaps returns a new map containing user's entries overlaid by reserved's entries.
// Reserved keys win on conflict, though validateNoReservedJobLabels rejects such conflicts before
// this is ever called.
func mergeStringMaps(user, reserved map[string]string) map[string]string {
	out := make(map[string]string, len(user)+len(reserved))
	for k, v := range user {
		out[k] = v
	}
	for k, v := range reserved {
		out[k] = v
	}
	return out
}

// jobName returns a deterministic, DNS-safe Job name for a parent/branch/sha identity.
//
// Job names are capped to the Kubernetes label-value limit (63 characters, not the 253-character
// DNS subdomain limit that Job names would otherwise allow): Kubernetes stamps each Job's Pods
// with a job-name label set to the Job's name, and label values longer than 63 characters fail
// validation. To stay well under that limit while remaining collision-resistant, the name is a
// truncated, sanitized stem followed by a hyphen and an FNV-32a hash of the full (untruncated)
// identity string, so the same inputs always produce the same name (idempotent) and a truncated
// stem cannot by itself cause two different identities to collide. This is a defense in depth
// alongside the label-based existence check in ensureJobForEnvironment, which is authoritative.
func jobName(parentName, branch, sha string) string {
	identity := strings.ToLower(nonAlphanumeric.ReplaceAllString(parentName+"-"+branch+"-"+sha, "-"))

	h := fnv.New32a()
	if _, err := h.Write([]byte(identity)); err != nil {
		// hash.Hash.Write is documented to never return an error; this panic should never be reached.
		panic(fmt.Sprintf("jobName: unexpected error writing to FNV hash: %v", err))
	}
	hash := strconv.FormatUint(uint64(h.Sum32()), 16)

	limit := validation.DNS1123LabelMaxLength
	if len(hash)+1 > limit {
		return utils.TruncateString(hash, limit)
	}
	budget := limit - len(hash) - 1 // runes for stem before the final "-<hash>"
	stem := strings.Trim(identity, "-")
	if stem == "" {
		stem = "x"
	}
	stem = utils.TruncateString(stem, budget)
	stem = strings.TrimRight(stem, "-")
	if stem == "" {
		stem = "x"
	}
	return stem + "-" + hash
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:dupl // Gate controllers share the same SetupWithManager skeleton by design.
func (r *JobCommitStatusReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	// Use Direct methods to read configuration from the API server without cache during setup.
	// The cache is not started during SetupWithManager, so we must use the non-cached API reader.
	rateLimiter, err := settings.GetRateLimiterDirect[promoterv1alpha1.JobCommitStatusConfiguration, ctrl.Request](ctx, r.SettingsMgr)
	if err != nil {
		return fmt.Errorf("failed to get JobCommitStatus rate limiter: %w", err)
	}

	maxConcurrentReconciles, err := settings.GetMaxConcurrentReconcilesDirect[promoterv1alpha1.JobCommitStatusConfiguration](ctx, r.SettingsMgr)
	if err != nil {
		return fmt.Errorf("failed to get JobCommitStatus max concurrent reconciles: %w", err)
	}

	err = ctrl.NewControllerManagedBy(mgr).
		For(&promoterv1alpha1.JobCommitStatus{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&promoterv1alpha1.PromotionStrategy{}, r.enqueueJobCommitStatusForPromotionStrategy()).
		Owns(&batchv1.Job{}).
		Named("jobcommitstatus").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
			RateLimiter:             rateLimiter,
		}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}
	return nil
}

// enqueueJobCommitStatusForPromotionStrategy returns a handler that enqueues all JobCommitStatus
// resources that reference a PromotionStrategy when that PromotionStrategy changes. Lookups use
// PromotionStrategyRefField, the shared field index registered once for all gate CRDs by
// RegisterGatePromotionStrategyRefFieldIndexes (see fieldindex.go).
func (r *JobCommitStatusReconciler) enqueueJobCommitStatusForPromotionStrategy() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		ps, ok := obj.(*promoterv1alpha1.PromotionStrategy)
		if !ok {
			return nil
		}

		var jcsList promoterv1alpha1.JobCommitStatusList
		if err := r.List(ctx, &jcsList,
			client.InNamespace(ps.Namespace),
			client.MatchingFields{PromotionStrategyRefField: ps.Name},
		); err != nil {
			log.FromContext(ctx).Error(err, "failed to list JobCommitStatus resources")
			return nil
		}

		requests := make([]ctrl.Request, 0, len(jcsList.Items))
		for i := range jcsList.Items {
			requests = append(requests, ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(&jcsList.Items[i]),
			})
		}

		return requests
	})
}
