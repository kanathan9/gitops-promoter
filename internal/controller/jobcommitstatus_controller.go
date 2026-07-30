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
	"fmt"
	"hash/fnv"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
	"github.com/argoproj-labs/gitops-promoter/internal/settings"
	"github.com/argoproj-labs/gitops-promoter/internal/types/constants"
	"github.com/argoproj-labs/gitops-promoter/internal/utils"
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

// nonAlphanumeric matches runs of characters that are not ASCII letters or digits, used by
// jobName to sanitize the identity string into a DNS-safe stem.
var nonAlphanumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

// JobCommitStatusReconciler reconciles a JobCommitStatus object.
//
// For each environment the gate applies to (resolved from the referenced PromotionStrategy), the
// reconciler creates one Job from spec.jobTemplate for the environment's current SHA (selected by
// spec.reportOn: proposed or active hydrated commit), and leaves it alone once created — Jobs are
// never mutated or deleted here. Job observation and CommitStatus management are added in a later
// subtask; see issue #1597.
type JobCommitStatusReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    events.EventRecorder
	SettingsMgr *settings.Manager

	// EnqueueCTP is a function to enqueue CTP reconcile requests without modifying the CTP object.
	// Unused until a later subtask starts transitioning CommitStatus phases, but plumbed through now
	// so every gate controller is constructed identically.
	EnqueueCTP CTPEnqueueFunc
}

// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses,verbs=get;list;watch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/finalizers,verbs=update
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=promotionstrategies,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create

// Reconcile loads the JobCommitStatus and its referenced PromotionStrategy, then creates one Job
// per applicable environment for that environment's current SHA (see resolveJobSha). An existing
// Job for the same parent/environment/SHA identity is left untouched — Jobs are created at most
// once and never mutated. Observing Job completion and managing CommitStatus resources from the
// result is added in a later subtask.
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

	newEnvStatuses := make([]promoterv1alpha1.JobCommitStatusEnvironmentStatus, 0, len(applicableEnvs))
	for _, env := range applicableEnvs {
		envStatus, found := envStatusMap[env.Branch]
		if !found {
			return ctrl.Result{}, fmt.Errorf("environment %q not found in PromotionStrategy status", env.Branch)
		}

		sha := resolveJobSha(envStatus, jcs.Spec.ReportOn)
		if sha == "" {
			return ctrl.Result{}, fmt.Errorf("no SHA available for environment %q (reportOn: %q): PromotionStrategy may not be fully reconciled", env.Branch, jcs.Spec.ReportOn)
		}

		envState := promoterv1alpha1.JobCommitStatusEnvironmentStatus{
			Branch: env.Branch,
			Sha:    sha,
			Phase:  promoterv1alpha1.CommitPhasePending,
		}

		job, ensureErr := r.ensureJobForEnvironment(ctx, &jcs, &ps, parentLabelKey, env.Branch, sha)
		if ensureErr != nil {
			logger.Error(ensureErr, "failed to ensure Job for environment", "branch", env.Branch, "sha", sha)
			r.Recorder.Eventf(&jcs, nil, "Warning", "JobCreateFailed", "Reconciling",
				"failed to create Job for environment %q at sha %q: %v", env.Branch, sha, ensureErr)
		} else {
			envState.JobRef = &promoterv1alpha1.JobCommitStatusJobReference{Name: job.Name, Namespace: job.Namespace}
			if !job.CreationTimestamp.IsZero() {
				startedAt := job.CreationTimestamp
				envState.StartedAt = &startedAt
			}
		}

		newEnvStatuses = append(newEnvStatuses, envState)
	}
	jcs.Status.Environments = newEnvStatuses

	return ctrl.Result{}, nil
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
// alone to prove non-existence for a different SHA. An already-created Job is returned as-is and
// never mutated.
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
	if len(existing.Items) > 0 {
		if len(existing.Items) > 1 {
			log.FromContext(ctx).Info("multiple Jobs found for the same parent/environment/sha identity; using the oldest",
				"branch", branch, "sha", sha, "count", len(existing.Items))
			slices.SortFunc(existing.Items, func(a, b batchv1.Job) int {
				return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
			})
		}
		return &existing.Items[0], nil
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName(jcs.Name, branch, sha),
			Namespace:   jcs.Namespace,
			Labels:      mergeStringMaps(jcs.Spec.JobTemplate.Labels, identityLabels),
			Annotations: jcs.Spec.JobTemplate.Annotations,
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
