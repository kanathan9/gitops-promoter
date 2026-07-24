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

package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// JobCommitStatusSpec defines the desired state of JobCommitStatus.
//
// Applicable environments are NOT enumerated on this resource. Instead, the controller resolves
// them from the referenced PromotionStrategy the same way GitCommitStatus and WebRequestCommitStatus
// do: an environment is applicable when Key matches an entry in PromotionStrategy.Spec.ProposedCommitStatuses
// or Environment.ProposedCommitStatuses (reportOn=proposed), or PromotionStrategy.Spec.ActiveCommitStatuses
// or Environment.ActiveCommitStatuses (reportOn=active). The PromotionStrategy remains the single
// authoritative source for "which environments does this gate apply to".
type JobCommitStatusSpec struct {
	// PromotionStrategyRef is a reference to the promotion strategy that this commit status applies to.
	// +required
	PromotionStrategyRef ObjectReference `json:"promotionStrategyRef"`

	// Key is the gate name referenced in the PromotionStrategy's proposedCommitStatuses or activeCommitStatuses.
	// It is used as the commit status key and in status messages.
	// Must be lowercase alphanumeric with hyphens, 1–63 characters (pattern: ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$).
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Key string `json:"key"`

	// ReportOn specifies which commit SHA the Job is run against and the CommitStatus is reported on.
	//   - "proposed" (default): the controller (re)creates the Job whenever the proposed hydrated SHA
	//     changes for an applicable environment. Use this for pre-promotion gates (tests, evals, migrations
	//     dry-runs) that must pass before the change is promoted.
	//   - "active": the controller (re)creates the Job whenever the active hydrated SHA changes. Use this
	//     for post-promotion checks that re-validate the environment after a promotion has landed.
	// +optional
	// +kubebuilder:default="proposed"
	// +kubebuilder:validation:Enum=proposed;active
	ReportOn string `json:"reportOn,omitempty"`

	// DescriptionTemplate is the human-readable commit status description shown in the SCM provider
	// (GitHub, GitLab, etc.). Uses Go templates.
	//
	// Template variables (mirrors WebRequestCommitStatus's promotionstrategy-context variable set):
	//   - {{ .Branch }}: the environment branch the Job was created for
	//   - {{ .PromotionStrategy }}: the full PromotionStrategy spec and status
	//   - {{ .JobCommitStatus }}: the full JobCommitStatus spec and status (snapshot from the previous reconcile)
	//   - {{ .NamespaceMetadata.Labels }} / {{ .NamespaceMetadata.Annotations }}: labels/annotations of the namespace
	//   - {{ .Job }}: the full child Job object once it has finished (nil while the Job is still running),
	//     so the description can surface details such as {{ .Job.status.conditions }} or a container's
	//     termination message.
	//
	// If not specified, defaults to empty string.
	// +optional
	DescriptionTemplate string `json:"descriptionTemplate,omitempty"`

	// Success defines how to decide success/failure from the finished child Job.
	// +required
	Success JobCommitStatusSuccessSpec `json:"success"`

	// JobTemplate is a standard batch/v1 Job template, embedded verbatim. The controller does not inject
	// defaults into the Job spec itself — native Job mechanics carry their own weight:
	//   backoffLimit            -> retry semantics
	//   activeDeadlineSeconds   -> timeout
	//   ttlSecondsAfterFinished -> cleanup (a reasonable suggestion is 24h; not defaulted by the controller)
	//
	// Templating (Go templates, same variables as DescriptionTemplate) is supported ONLY in
	// jobTemplate.metadata.labels and jobTemplate.metadata.annotations; the rest of the Job spec is used
	// as-is. The controller also stamps promoter.argoproj.io/branch, promoter.argoproj.io/dry-sha, and
	// promoter.argoproj.io/hydrated-sha labels onto the created Job (and, transitively, its Pods), so
	// they're available to the running container via the downward API.
	// +required
	JobTemplate batchv1.JobTemplateSpec `json:"jobTemplate"`
}

// JobCommitStatusSuccessSpec defines when the commit status phase is success, based on the finished child Job.
type JobCommitStatusSuccessSpec struct {
	// When configures the expression evaluated against the finished Job to determine success.
	// +required
	When JobCommitStatusWhenSpec `json:"when"`
}

// JobCommitStatusWhenSpec holds the expression used to evaluate a finished Job.
type JobCommitStatusWhenSpec struct {
	// Expression is an expr expression (github.com/expr-lang/expr) evaluated against the finished Job.
	// It must return a boolean: true means success, false means failure.
	//
	// Available variables:
	//   - Job (batchv1.Job): the finished child Job object
	//
	// A reasonable documented default for most use cases is a zero exit code on the main container,
	// expressed as:
	//
	//   "Job.status.succeeded >= 1"
	//
	// Users running multi-completion Jobs, or who need to distinguish a nonzero exit code (Failed) from
	// an infrastructure problem such as ImagePullBackOff (Error, the way Argo Workflows does), can inspect
	// Job.status (and, in the future, pod/container statuses) in the expression.
	// +required
	// +kubebuilder:validation:MinLength=1
	Expression string `json:"expression"`
}

// JobCommitStatusStatus defines the observed state of JobCommitStatus.
type JobCommitStatusStatus struct {
	// ObservedGeneration is the .metadata.generation that this status was reconciled from.
	// Because status is written via Server-Side Apply with ForceOwnership (which has no
	// optimistic-concurrency check), this field is the canonical way to detect stale
	// status writes: compare status.observedGeneration with metadata.generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Environments holds the observed Job/gate status for each applicable environment (as resolved from
	// the referenced PromotionStrategy; see JobCommitStatusSpec).
	// +listType=map
	// +listMapKey=branch
	// +optional
	Environments []JobCommitStatusEnvironmentStatus `json:"environments,omitempty"`

	// Conditions represent the latest available observations of the JobCommitStatus's state.
	// Standard condition types include "Ready" which aggregates the status of all environments.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// InstanceID mirrors metadata.labels[promoter.argoproj.io/instance-id] stamped on each
	// reconcile attempt by this install's controller, including when Ready=False; omitted
	// when the resource has no instance-id label (default install).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`
	InstanceID *string `json:"instanceID,omitempty"`
}

// JobCommitStatusEnvironmentStatus defines the observed Job/gate status for a specific environment.
type JobCommitStatusEnvironmentStatus struct {
	// Branch is the environment branch name being gated.
	// +required
	// +kubebuilder:validation:MinLength=1
	Branch string `json:"branch"`

	// Sha is the hydrated commit SHA (proposed or active, per spec.reportOn) that the current or most
	// recent Job was created for.
	// Supports both SHA-1 (40 chars) and SHA-256 (64 chars) Git hash formats.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^([a-f0-9]{40}|[a-f0-9]{64})?$`
	Sha string `json:"sha,omitempty"`

	// JobRef references the child Job created for Sha. The Job always lives in the JobCommitStatus's own
	// namespace.
	// +optional
	JobRef *JobCommitStatusJobReference `json:"jobRef,omitempty"`

	// Phase represents the current phase of the gate for this environment.
	//   - "pending": the Job has been created but has not finished, or no Job has been created yet for Sha
	//   - "success": the Job finished and success.when.expression evaluated to true
	//   - "failure": the Job finished and success.when.expression evaluated to false, or the Job itself
	//     failed (for example activeDeadlineSeconds was exceeded or backoffLimit was reached)
	// +kubebuilder:validation:Enum=pending;success;failure
	// +required
	Phase CommitStatusPhase `json:"phase"`

	// StartedAt is when the current/most recent Job was created.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// FinishedAt is when the current/most recent Job reached a terminal state (Complete or Failed).
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

// JobCommitStatusJobReference references the child Job created for a given environment/SHA.
type JobCommitStatusJobReference struct {
	// Name is the name of the child Job.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace is the namespace of the child Job. This always matches the JobCommitStatus's own namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`
}

// GetConditions returns the conditions of the JobCommitStatus.
func (jcs *JobCommitStatus) GetConditions() *[]metav1.Condition {
	return &jcs.Status.Conditions
}

// SetObservedGeneration records the object generation that produced the current status.
func (jcs *JobCommitStatus) SetObservedGeneration(generation int64) {
	jcs.Status.ObservedGeneration = generation
}

// SetStatusInstanceID records the instance-id label mirrored into status on each reconcile attempt.
func (jcs *JobCommitStatus) SetStatusInstanceID(v *string) {
	jcs.Status.InstanceID = v
}

// +kubebuilder:ac:generate=true
// +kubebuilder:externalDocs:url="https://gitops-promoter.readthedocs.io/en/stable/crd-specs/#jobcommitstatus",description="CRD reference (examples and behavior)"
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// JobCommitStatus is the Schema for the jobcommitstatuses API.
//
// It gates promotions on in-cluster batch work: for each applicable environment (resolved from the
// referenced PromotionStrategy), the controller creates a Job from jobTemplate for the relevant hydrated
// SHA (per reportOn), observes it, and reflects the result into a CommitStatus.
// +kubebuilder:printcolumn:name="Key",type=string,JSONPath=`.spec.key`
// +kubebuilder:printcolumn:name="PromotionStrategy",type=string,JSONPath=`.spec.promotionStrategyRef.name`
// +kubebuilder:printcolumn:name="ReportOn",type=string,JSONPath=`.spec.reportOn`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type JobCommitStatus struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of JobCommitStatus
	// +required
	Spec JobCommitStatusSpec `json:"spec"`

	// status defines the observed state of JobCommitStatus
	// +optional
	Status JobCommitStatusStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// JobCommitStatusList contains a list of JobCommitStatus.
type JobCommitStatusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []JobCommitStatus `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &JobCommitStatus{}, &JobCommitStatusList{})
		return nil
	})
}
