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
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
	"github.com/argoproj-labs/gitops-promoter/internal/types/constants"
	"github.com/argoproj-labs/gitops-promoter/internal/utils"
)

// setJobCondition fetches the latest version of the Job named jobKey, sets its status to have
// exactly one terminal condition (condType=True with the given reason/message) plus the given
// succeeded count, and writes it via the status subresource — simulating what a real Job
// controller would eventually report, since envtest runs no kubelet/job-controller to produce
// this itself.
func setJobCondition(ctx context.Context, jobKey types.NamespacedName, condType batchv1.JobConditionType, reason, message string, succeeded int32) {
	var job batchv1.Job
	Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
	job.Status.Succeeded = succeeded

	now := metav1.NewTime(time.Now())
	if job.Status.StartTime == nil {
		// The API server requires status.startTime to be set on a finished Job.
		job.Status.StartTime = &now
	}

	var conditions []batchv1.JobCondition
	switch condType {
	case batchv1.JobFailed:
		// The API server requires a FailureTarget=True condition before a Failed=True condition
		// can be set (Kubernetes' job-tracking-with-finalizers ordering).
		conditions = append(conditions, batchv1.JobCondition{
			Type:               batchv1.JobFailureTarget,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             reason,
			Message:            message,
		})
	case batchv1.JobComplete:
		// The API server requires a SuccessCriteriaMet=True condition and status.completionTime
		// before a Complete=True condition can be set.
		conditions = append(conditions, batchv1.JobCondition{
			Type:               batchv1.JobSuccessCriteriaMet,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
		job.Status.CompletionTime = &now
	default:
	}
	conditions = append(conditions, batchv1.JobCondition{
		Type:               condType,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	job.Status.Conditions = conditions

	Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
}

// jobCommitStatusWithValidTemplate returns a minimal, schema-valid JobCommitStatus so tests can
// Create() it against a real API server (envtest) without tripping CRD validation.
func jobCommitStatusWithValidTemplate(name, namespace, psName string) *promoterv1alpha1.JobCommitStatus {
	return &promoterv1alpha1.JobCommitStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: promoterv1alpha1.JobCommitStatusSpec{
			PromotionStrategyRef: promoterv1alpha1.ObjectReference{Name: psName},
			Key:                  "eval-gate",
			Success: promoterv1alpha1.JobCommitStatusSuccessSpec{
				When: promoterv1alpha1.JobCommitStatusWhenSpec{Expression: "Job.status.succeeded >= 1"},
			},
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{Name: "check", Image: "busybox"},
							},
						},
					},
				},
			},
		},
	}
}

var _ = Describe("JobCommitStatus Controller - Missing PromotionStrategy", Ordered, func() {
	var (
		ctx context.Context
		jcs *promoterv1alpha1.JobCommitStatus
	)

	BeforeEach(func() {
		ctx = context.Background()
		jcs = jobCommitStatusWithValidTemplate("jcs-missing-ps", "default", "nonexistent-ps")
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, jcs)
	})

	It("should record a readable warning without panicking, and create no child resources", func() {
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      "jcs-missing-ps",
				Namespace: "default",
			}, &current)
			g.Expect(err).NotTo(HaveOccurred())

			readyCondition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
			g.Expect(readyCondition).ToNot(BeNil())
			g.Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(readyCondition.Message).To(ContainSubstring("nonexistent-ps"))
		}, constants.EventuallyTimeout).Should(Succeed())

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace("default"))).To(Succeed())
		Expect(ownedJobs(jobs.Items, jcs)).To(BeEmpty())
	})
})

// jobCommitStatusForGate builds a schema-valid JobCommitStatus for the "Job Creation" suite below:
// one container and one init container (so tests can verify env-var injection on both), and
// caller-supplied labels/annotations on jobTemplate.metadata so tests can verify user fields
// survive alongside the controller-reserved ones.
func jobCommitStatusForGate(name, psName, key string, labels, annotations map[string]string) *promoterv1alpha1.JobCommitStatus {
	jcs := jobCommitStatusWithValidTemplate(name, "default", psName)
	jcs.Spec.Key = key
	jcs.Spec.JobTemplate.Labels = labels
	jcs.Spec.JobTemplate.Annotations = annotations
	jcs.Spec.JobTemplate.Spec.Template.Spec.InitContainers = []corev1.Container{
		{Name: "prep", Image: "busybox", Command: []string{"true"}},
	}
	jcs.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command = []string{"true"}
	return jcs
}

// envVarMap converts a container's Env slice to a map for easy lookup in assertions.
func envVarMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

var _ = Describe("JobCommitStatus Controller - Job Creation", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
	)

	const gateKey = "job-eval-gate"

	BeforeAll(func() {
		ctx = context.Background()

		By("Setting up test git repository and a PromotionStrategy gating only development and staging")
		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment || promotionStrategy.Spec.Environments[i].Branch == testBranchStaging {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		By("Waiting for the PromotionStrategy to populate proposed hydrated SHAs")
		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			g.Expect(ps.Status.Environments).To(HaveLen(3))
			for _, env := range ps.Status.Environments {
				g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty(), "branch %s", env.Branch)
			}
		}, constants.EventuallyTimeout).Should(Succeed())
	})

	AfterAll(func() {
		By("Cleaning up test resources")
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("creates one owned Job per applicable environment, with identity labels, an owner reference, preserved user fields, and injected context env vars", func() {
		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey,
			map[string]string{"team": "platform"},
			map[string]string{"example.com/note": "hello"},
		)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())

		var devSha, stagingSha string
		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for _, env := range ps.Status.Environments {
				switch env.Branch {
				case testBranchDevelopment:
					devSha = env.Proposed.Hydrated.Sha
				case testBranchStaging:
					stagingSha = env.Proposed.Hydrated.Sha
				default:
				}
			}
			g.Expect(devSha).ToNot(BeEmpty())
			g.Expect(stagingSha).ToNot(BeEmpty())
		}, constants.EventuallyTimeout).Should(Succeed())

		var current promoterv1alpha1.JobCommitStatus
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(2))
			for _, env := range current.Status.Environments {
				g.Expect(env.JobRef).ToNot(BeNil(), "branch %s should have a JobRef", env.Branch)
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace("default"))).To(Succeed())
		owned := ownedJobs(jobs.Items, &current)
		Expect(owned).To(HaveLen(2), "expected exactly one Job per applicable environment")

		parentLabelKey := utils.CommitStatusGateLabelKeyForParent(&current)
		shaByBranch := map[string]string{testBranchDevelopment: devSha, testBranchStaging: stagingSha}
		// The environment label value is KubeSafeLabel-sanitized (e.g. "environment-development"),
		// while the injected PROMOTER_JOB_BRANCH env var carries the raw branch name (e.g.
		// "environment/development"); map back from the sanitized label to the raw branch so both
		// can be checked against the same shaByBranch map.
		rawBranchBySanitized := map[string]string{
			utils.KubeSafeLabel(testBranchDevelopment): testBranchDevelopment,
			utils.KubeSafeLabel(testBranchStaging):     testBranchStaging,
		}
		seenBranches := map[string]bool{}
		for _, job := range owned {
			sanitizedBranch := job.Labels[promoterv1alpha1.EnvironmentLabel]
			branch, ok := rawBranchBySanitized[sanitizedBranch]
			Expect(ok).To(BeTrue(), "unexpected "+promoterv1alpha1.EnvironmentLabel+" label value %q", sanitizedBranch)
			seenBranches[branch] = true

			By("Verifying identity labels for branch " + branch)
			Expect(job.Labels[parentLabelKey]).To(Equal(utils.KubeSafeLabel(current.Name)))
			Expect(job.Labels[promoterv1alpha1.JobCommitStatusShaLabel]).To(Equal(shaByBranch[branch]))

			By("Verifying user labels/annotations are preserved on " + job.Name)
			Expect(job.Labels["team"]).To(Equal("platform"))
			Expect(job.Annotations["example.com/note"]).To(Equal("hello"))

			By("Verifying the owner reference on " + job.Name)
			ownerRef := metav1.GetControllerOf(&job)
			Expect(ownerRef).ToNot(BeNil())
			Expect(ownerRef.Name).To(Equal(current.Name))
			Expect(ownerRef.Kind).To(Equal("JobCommitStatus"))

			By("Verifying user container/init-container fields and injected env vars on " + job.Name)
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers[0].Name).To(Equal("check"))
			Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal("busybox"))
			Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.InitContainers[0].Name).To(Equal("prep"))

			for _, c := range append(append([]corev1.Container{}, job.Spec.Template.Spec.Containers...), job.Spec.Template.Spec.InitContainers...) {
				env := envVarMap(c.Env)
				Expect(env["PROMOTER_JOB_SHA"]).To(Equal(shaByBranch[branch]))
				Expect(env["PROMOTER_JOB_BRANCH"]).To(Equal(branch))
				Expect(env["PROMOTER_JOB_PROMOTION_STRATEGY"]).To(Equal(name))
				Expect(env["PROMOTER_JOB_REPOSITORY"]).To(Equal(gitRepo.Name))
			}
		}
		Expect(seenBranches).To(HaveKey(testBranchDevelopment))
		Expect(seenBranches).To(HaveKey(testBranchStaging))
	})

	It("does not create duplicate Jobs on repeated reconciliation", func() {
		var before batchv1.JobList
		Expect(k8sClient.List(ctx, &before, client.InNamespace("default"))).To(Succeed())
		var current promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
		beforeCount := len(ownedJobs(before.Items, &current))
		Expect(beforeCount).To(Equal(2))

		By("Forcing another reconcile by changing an unrelated spec field")
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			current.Spec.DescriptionTemplate = "reconciled again"
			g.Expect(k8sClient.Update(ctx, &current)).To(Succeed())
		}, constants.EventuallyTimeout).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		}, constants.EventuallyTimeout).Should(Succeed())

		Consistently(func(g Gomega) {
			var after batchv1.JobList
			g.Expect(k8sClient.List(ctx, &after, client.InNamespace("default"))).To(Succeed())
			g.Expect(ownedJobs(after.Items, &current)).To(HaveLen(beforeCount))
		}, "3s", "500ms").Should(Succeed())
	})
})

var _ = Describe("JobCommitStatus Controller - New Proposed SHA", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
	)

	const gateKey = "job-eval-gate-sha-advance"

	BeforeAll(func() {
		ctx = context.Background()

		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-sha-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		// Gate only the development environment, to isolate the SHA-advance assertion below from
		// the other environments (makeChangeAndHydrateRepo advances every environment's proposed
		// commit in one push).
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for _, env := range ps.Status.Environments {
				if env.Branch == testBranchDevelopment {
					g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty())
				}
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey, nil, nil)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterAll(func() {
		if jcs != nil {
			_ = k8sClient.Delete(ctx, jcs)
		}
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("creates a new Job when the proposed SHA advances, leaving the previous Job unchanged", func() {
		var firstSha string
		var firstJob batchv1.Job
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].JobRef).ToNot(BeNil())
			firstSha = current.Status.Environments[0].Sha

			var job batchv1.Job
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      current.Status.Environments[0].JobRef.Name,
				Namespace: current.Status.Environments[0].JobRef.Namespace,
			}, &job)).To(Succeed())
			firstJob = job
		}, constants.EventuallyTimeout).Should(Succeed())

		By("Advancing the proposed commit")
		gitPath, err := os.MkdirTemp("", "jobcommitstatus-sha-advance-*")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = os.RemoveAll(gitPath) }()
		makeChangeAndHydrateRepo(gitPath, gitRepo, "advance for job gate test", "advance for job gate test")

		var secondSha string
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].Sha).ToNot(Equal(firstSha))
			g.Expect(current.Status.Environments[0].JobRef).ToNot(BeNil())
			secondSha = current.Status.Environments[0].Sha
		}, constants.EventuallyTimeout).Should(Succeed())
		Expect(secondSha).ToNot(Equal(firstSha))

		By("Verifying the original Job for the first SHA is untouched")
		var stillFirstJob batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: firstJob.Name, Namespace: firstJob.Namespace}, &stillFirstJob)).To(Succeed())
		Expect(stillFirstJob.ResourceVersion).To(Equal(firstJob.ResourceVersion))
		Expect(stillFirstJob.Labels[promoterv1alpha1.JobCommitStatusShaLabel]).To(Equal(firstSha))

		By("Verifying exactly two Jobs now exist for this gate: one per SHA")
		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace("default"))).To(Succeed())
		var current promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
		owned := ownedJobs(jobs.Items, &current)
		Expect(owned).To(HaveLen(2))
	})

	It("does not let the older SHA's Job alter the newer SHA's status once it completes", func() {
		var current promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
		Expect(current.Status.Environments).To(HaveLen(1))
		trackedSha := current.Status.Environments[0].Sha
		trackedJobRef := current.Status.Environments[0].JobRef
		Expect(trackedJobRef).ToNot(BeNil())

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace("default"))).To(Succeed())
		owned := ownedJobs(jobs.Items, &current)
		Expect(owned).To(HaveLen(2), "expected both the older and newer SHA's Jobs to still exist")

		var olderJob *batchv1.Job
		for i := range owned {
			if owned[i].Name != trackedJobRef.Name {
				olderJob = &owned[i]
			}
		}
		Expect(olderJob).ToNot(BeNil(), "expected to find the Job for the older, untracked SHA")
		Expect(olderJob.Labels[promoterv1alpha1.JobCommitStatusShaLabel]).ToNot(Equal(trackedSha))

		By("Completing the older SHA's Job")
		setJobCondition(ctx, types.NamespacedName{Name: olderJob.Name, Namespace: olderJob.Namespace},
			batchv1.JobComplete, "", "", 1)

		By("Verifying the tracked (newer) environment status and CommitStatus are unaffected")
		Consistently(func(g Gomega) {
			var after promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &after)).To(Succeed())
			g.Expect(after.Status.Environments).To(HaveLen(1))
			g.Expect(after.Status.Environments[0].Sha).To(Equal(trackedSha))
			g.Expect(after.Status.Environments[0].JobRef.Name).To(Equal(trackedJobRef.Name))
			g.Expect(after.Status.Environments[0].Phase).To(Equal(promoterv1alpha1.CommitPhasePending))

			var cs promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      utils.CommitStatusResourceName(ctx, &after, testBranchDevelopment),
				Namespace: "default",
			}, &cs)).To(Succeed())
			g.Expect(cs.Spec.Sha).To(Equal(trackedSha))
			g.Expect(cs.Spec.Phase).To(Equal(promoterv1alpha1.CommitPhasePending))
		}, "3s", "500ms").Should(Succeed())
	})
})

var _ = Describe("JobCommitStatus Controller - Job Observation", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
		jobKey            types.NamespacedName
		sha               string
	)

	const gateKey = "job-eval-gate-observation"

	BeforeAll(func() {
		ctx = context.Background()

		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-observe-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for _, env := range ps.Status.Environments {
				if env.Branch == testBranchDevelopment {
					g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty())
				}
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey, nil, nil)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterAll(func() {
		if jcs != nil {
			_ = k8sClient.Delete(ctx, jcs)
		}
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("reports pending, correlated to the proposed SHA, while the Job has no terminal condition", func() {
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			env := current.Status.Environments[0]
			g.Expect(env.JobRef).ToNot(BeNil())
			g.Expect(env.Phase).To(Equal(promoterv1alpha1.CommitPhasePending))
			g.Expect(env.Reason).To(Equal("JobRunning"))
			sha = env.Sha
			jobKey = types.NamespacedName{Name: env.JobRef.Name, Namespace: env.JobRef.Namespace}

			var cs promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      utils.CommitStatusResourceName(ctx, &current, testBranchDevelopment),
				Namespace: "default",
			}, &cs)).To(Succeed())
			g.Expect(cs.Spec.Phase).To(Equal(promoterv1alpha1.CommitPhasePending))
			g.Expect(cs.Spec.Sha).To(Equal(sha))
		}, constants.EventuallyTimeout).Should(Succeed())
	})

	It("promptly reports success once the Job completes and success.when.expression passes", func() {
		setJobCondition(ctx, jobKey, batchv1.JobComplete, "", "", 1)

		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			env := current.Status.Environments[0]
			g.Expect(env.Sha).To(Equal(sha))
			g.Expect(env.Phase).To(Equal(promoterv1alpha1.CommitPhaseSuccess))
			g.Expect(env.FinishedAt).ToNot(BeNil())

			var cs promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      utils.CommitStatusResourceName(ctx, &current, testBranchDevelopment),
				Namespace: "default",
			}, &cs)).To(Succeed())
			g.Expect(cs.Spec.Phase).To(Equal(promoterv1alpha1.CommitPhaseSuccess))
			g.Expect(cs.Spec.Sha).To(Equal(sha))
		}, constants.EventuallyTimeout).Should(Succeed())
	})

	It("does not churn resource versions when reconciling an unchanged terminal Job repeatedly", func() {
		var before promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &before)).To(Succeed())
		Expect(before.Status.Environments[0].Phase).To(Equal(promoterv1alpha1.CommitStatusPhase("success")))
		beforeFinishedAt := before.Status.Environments[0].FinishedAt
		Expect(beforeFinishedAt).ToNot(BeNil())

		var beforeCS promoterv1alpha1.CommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      utils.CommitStatusResourceName(ctx, &before, testBranchDevelopment),
			Namespace: "default",
		}, &beforeCS)).To(Succeed())

		var beforeJob batchv1.Job
		Expect(k8sClient.Get(ctx, jobKey, &beforeJob)).To(Succeed())

		By("Forcing several more reconciles by touching an unrelated field repeatedly")
		for i := 0; i < 3; i++ {
			Eventually(func(g Gomega) {
				var current promoterv1alpha1.JobCommitStatus
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
				if current.Annotations == nil {
					current.Annotations = map[string]string{}
				}
				current.Annotations["test.gitops-promoter.io/touch"] = strconv.Itoa(i)
				g.Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			}, constants.EventuallyTimeout).Should(Succeed())

			Eventually(func(g Gomega) {
				var current promoterv1alpha1.JobCommitStatus
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
				g.Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
			}, constants.EventuallyTimeout).Should(Succeed())
		}

		var after promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &after)).To(Succeed())
		Expect(after.Status.Environments[0].Phase).To(Equal(promoterv1alpha1.CommitPhaseSuccess))
		Expect(after.Status.Environments[0].FinishedAt).To(Equal(beforeFinishedAt))

		var afterCS promoterv1alpha1.CommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      utils.CommitStatusResourceName(ctx, &after, testBranchDevelopment),
			Namespace: "default",
		}, &afterCS)).To(Succeed())
		Expect(afterCS.ResourceVersion).To(Equal(beforeCS.ResourceVersion), "CommitStatus should not be re-applied when nothing changed")

		var afterJob batchv1.Job
		Expect(k8sClient.Get(ctx, jobKey, &afterJob)).To(Succeed())
		Expect(afterJob.ResourceVersion).To(Equal(beforeJob.ResourceVersion), "Job must never be mutated by the controller")
	})
})

var _ = Describe("JobCommitStatus Controller - Job Failure", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
	)

	const gateKey = "job-eval-gate-failure"

	BeforeAll(func() {
		ctx = context.Background()

		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-failure-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for _, env := range ps.Status.Environments {
				if env.Branch == testBranchDevelopment {
					g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty())
				}
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		// backoffLimit: 0 matches the human test plan's "non-zero Job with backoffLimit: 0" scenario;
		// envtest runs no kubelet/job-controller, so the terminal Failed condition is simulated via
		// setJobCondition rather than a real container exit.
		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey, nil, nil)
		jcs.Spec.JobTemplate.Spec.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterAll(func() {
		if jcs != nil {
			_ = k8sClient.Delete(ctx, jcs)
		}
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("reports failure with a useful reason for a Failed Job", func() {
		var jobKey types.NamespacedName
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].JobRef).ToNot(BeNil())
			jobKey = types.NamespacedName{
				Name:      current.Status.Environments[0].JobRef.Name,
				Namespace: current.Status.Environments[0].JobRef.Namespace,
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		setJobCondition(ctx, jobKey, batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit", 0)

		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			env := current.Status.Environments[0]
			g.Expect(env.Phase).To(Equal(promoterv1alpha1.CommitPhaseFailure))
			g.Expect(env.Reason).To(Equal("BackoffLimitExceeded"))

			var cs promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      utils.CommitStatusResourceName(ctx, &current, testBranchDevelopment),
				Namespace: "default",
			}, &cs)).To(Succeed())
			g.Expect(cs.Spec.Phase).To(Equal(promoterv1alpha1.CommitPhaseFailure))
			g.Expect(cs.Spec.Description).To(ContainSubstring("BackoffLimitExceeded"))
		}, constants.EventuallyTimeout).Should(Succeed())
	})
})

var _ = Describe("JobCommitStatus Controller - Job Finalizer Lifecycle", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
		jobKey            types.NamespacedName
	)

	const gateKey = "job-eval-gate-finalizer"

	BeforeAll(func() {
		ctx = context.Background()

		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-finalizer-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for _, env := range ps.Status.Environments {
				if env.Branch == testBranchDevelopment {
					g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty())
				}
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey, nil, nil)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterAll(func() {
		if jcs != nil {
			_ = k8sClient.Delete(ctx, jcs)
		}
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("adds a finalizer to the Job at creation", func() {
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].JobRef).ToNot(BeNil())
			jobKey = types.NamespacedName{
				Name:      current.Status.Environments[0].JobRef.Name,
				Namespace: current.Status.Environments[0].JobRef.Namespace,
			}

			var job batchv1.Job
			g.Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
			g.Expect(job.Finalizers).To(ContainElement(promoterv1alpha1.JobCommitStatusJobFinalizer))
		}, constants.EventuallyTimeout).Should(Succeed())
	})

	It("removes the finalizer once the CommitStatus reflects a terminal phase, unblocking deletion", func() {
		setJobCondition(ctx, jobKey, batchv1.JobComplete, "", "", 1)

		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].Phase).To(Equal(promoterv1alpha1.CommitPhaseSuccess))

			var job batchv1.Job
			g.Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
			g.Expect(job.Finalizers).ToNot(ContainElement(promoterv1alpha1.JobCommitStatusJobFinalizer))
		}, constants.EventuallyTimeout).Should(Succeed())

		By("Verifying deletion is no longer blocked (what a real ttlSecondsAfterFinished cleanup relies on)")
		// Background propagation is requested explicitly: envtest runs no garbage-collector controller
		// to process the API server's legacy "orphan" finalizer, which it otherwise attaches by default
		// when a Delete request leaves PropagationPolicy unset — that finalizer has nothing to do with
		// JobCommitStatusJobFinalizer (already confirmed removed above) and would leave the Job stuck
		// Terminating forever in this test environment.
		Expect(k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobKey.Name, Namespace: jobKey.Namespace}}, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func(g Gomega) {
			var job batchv1.Job
			err := k8sClient.Get(ctx, jobKey, &job)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, constants.EventuallyTimeout).Should(Succeed())
	})
})

var _ = Describe("JobCommitStatus Controller - Environment Removal and Parent Deletion", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
	)

	const gateKey = "job-eval-gate-env-removal"

	BeforeAll(func() {
		ctx = context.Background()

		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-env-removal-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		for i := range promotionStrategy.Spec.Environments {
			if promotionStrategy.Spec.Environments[i].Branch == testBranchDevelopment || promotionStrategy.Spec.Environments[i].Branch == testBranchStaging {
				promotionStrategy.Spec.Environments[i].ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: gateKey}}
			}
		}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())

		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			g.Expect(ps.Status.Environments).To(HaveLen(3))
			for _, env := range ps.Status.Environments {
				g.Expect(env.Proposed.Hydrated.Sha).ToNot(BeEmpty())
			}
		}, constants.EventuallyTimeout).Should(Succeed())

		jcs = jobCommitStatusForGate(name+"-gate", name, gateKey, nil, nil)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())
	})

	AfterAll(func() {
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("cleans up the CommitStatus for a removed environment, and garbage-collects owned children on parent deletion", func() {
		var developmentCSName, stagingCSName string
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(2))
			for _, env := range current.Status.Environments {
				g.Expect(env.JobRef).ToNot(BeNil())
			}
			developmentCSName = utils.CommitStatusResourceName(ctx, &current, testBranchDevelopment)
			stagingCSName = utils.CommitStatusResourceName(ctx, &current, testBranchStaging)

			var cs promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: stagingCSName, Namespace: "default"}, &cs)).To(Succeed())
		}, constants.EventuallyTimeout).Should(Succeed())

		By("Removing the staging environment from this gate")
		Eventually(func(g Gomega) {
			var ps promoterv1alpha1.PromotionStrategy
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &ps)).To(Succeed())
			for i := range ps.Spec.Environments {
				if ps.Spec.Environments[i].Branch == testBranchStaging {
					ps.Spec.Environments[i].ProposedCommitStatuses = nil
				}
			}
			g.Expect(k8sClient.Update(ctx, &ps)).To(Succeed())
		}, constants.EventuallyTimeout).Should(Succeed())

		By("Verifying the removed environment's CommitStatus is cleaned up, and the remaining one is untouched")
		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			g.Expect(current.Status.Environments).To(HaveLen(1))
			g.Expect(current.Status.Environments[0].Branch).To(Equal(testBranchDevelopment))

			var stagingCS promoterv1alpha1.CommitStatus
			err := k8sClient.Get(ctx, types.NamespacedName{Name: stagingCSName, Namespace: "default"}, &stagingCS)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())

			var devCS promoterv1alpha1.CommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: developmentCSName, Namespace: "default"}, &devCS)).To(Succeed())
		}, constants.EventuallyTimeout).Should(Succeed())

		By("Capturing the remaining Job's owner reference before deleting the parent")
		var current promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
		var job batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      current.Status.Environments[0].JobRef.Name,
			Namespace: current.Status.Environments[0].JobRef.Namespace,
		}, &job)).To(Succeed())
		jobOwnerRef := metav1.GetControllerOf(&job)
		Expect(jobOwnerRef).ToNot(BeNil())
		Expect(jobOwnerRef.UID).To(Equal(current.UID))
		Expect(jobOwnerRef.Kind).To(Equal("JobCommitStatus"))

		By("Deleting the parent JobCommitStatus")
		Expect(k8sClient.Delete(ctx, &current)).To(Succeed())
		Eventually(func(g Gomega) {
			var deleted promoterv1alpha1.JobCommitStatus
			err := k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &deleted)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, constants.EventuallyTimeout).Should(Succeed())

		// envtest runs no garbage-collector controller, so the owned Job is not actually cascade-deleted
		// here. What we can and do verify is the contract a real cluster's GC relies on: the Job's
		// controller owner reference still points at the now-deleted parent's UID.
		var stillJob batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &stillJob)).To(Succeed())
		stillOwnerRef := metav1.GetControllerOf(&stillJob)
		Expect(stillOwnerRef).ToNot(BeNil())
		Expect(stillOwnerRef.UID).To(Equal(current.UID))
	})
})

var _ = Describe("JobCommitStatus Controller - Reserved Labels", Ordered, func() {
	var (
		ctx               context.Context
		name              string
		scmSecret         *corev1.Secret
		scmProvider       *promoterv1alpha1.ScmProvider
		gitRepo           *promoterv1alpha1.GitRepository
		promotionStrategy *promoterv1alpha1.PromotionStrategy
		jcs               *promoterv1alpha1.JobCommitStatus
	)

	BeforeAll(func() {
		ctx = context.Background()
		name, scmSecret, scmProvider, gitRepo, _, _, promotionStrategy = promotionStrategyResource(ctx, "job-commit-status-reserved-label-test", "default")
		setupInitialTestGitRepoOnServer(ctx, gitRepo)

		Expect(k8sClient.Create(ctx, scmSecret)).To(Succeed())
		Expect(k8sClient.Create(ctx, scmProvider)).To(Succeed())
		Expect(k8sClient.Create(ctx, gitRepo)).To(Succeed())
		promotionStrategy.Spec.ProposedCommitStatuses = []promoterv1alpha1.CommitStatusSelector{{Key: "reserved-label-gate"}}
		Expect(k8sClient.Create(ctx, promotionStrategy)).To(Succeed())
	})

	AfterAll(func() {
		if jcs != nil {
			_ = k8sClient.Delete(ctx, jcs)
		}
		if promotionStrategy != nil {
			_ = k8sClient.Delete(ctx, promotionStrategy)
		}
		if gitRepo != nil {
			_ = k8sClient.Delete(ctx, gitRepo)
		}
		if scmProvider != nil {
			_ = k8sClient.Delete(ctx, scmProvider)
		}
		if scmSecret != nil {
			_ = k8sClient.Delete(ctx, scmSecret)
		}
	})

	It("rejects a jobTemplate that sets a reserved label, and creates no Jobs", func() {
		jcs = jobCommitStatusForGate("reserved-label-gate", name, "reserved-label-gate",
			map[string]string{promoterv1alpha1.EnvironmentLabel: "not-allowed"},
			nil,
		)
		Expect(k8sClient.Create(ctx, jcs)).To(Succeed())

		Eventually(func(g Gomega) {
			var current promoterv1alpha1.JobCommitStatus
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
			readyCondition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
			g.Expect(readyCondition).ToNot(BeNil())
			g.Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(readyCondition.Message).To(ContainSubstring("reserved label"))
		}, constants.EventuallyTimeout).Should(Succeed())

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace("default"))).To(Succeed())
		var current promoterv1alpha1.JobCommitStatus
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jcs.Name, Namespace: "default"}, &current)).To(Succeed())
		Expect(ownedJobs(jobs.Items, &current)).To(BeEmpty())
	})
})

// ownedJobs filters jobs to those controller-owned by parent.
func ownedJobs(jobs []batchv1.Job, parent *promoterv1alpha1.JobCommitStatus) []batchv1.Job {
	var out []batchv1.Job
	for _, job := range jobs {
		ownerRef := metav1.GetControllerOf(&job)
		if ownerRef != nil && ownerRef.UID == parent.UID {
			out = append(out, job)
		}
	}
	return out
}

// TestEnsureJobForEnvironment_IgnoresSpoofedJob verifies that a Job carrying the right identity
// labels (parent, environment, sha) but NOT actually owned by this JobCommitStatus is never
// treated as "the" Job for that environment: labels alone are attacker- or accident-controlled
// (anyone who can create a Job in the namespace can set them), so only an ownership check
// (metav1.IsControlledBy) can be trusted. ensureJobForEnvironment must create its own, properly
// owned Job rather than reuse the spoofed one.
func TestEnsureJobForEnvironment_IgnoresSpoofedJob(t *testing.T) {
	t.Parallel()

	scheme := utils.GetScheme()
	ctx := t.Context()

	jcs := jobCommitStatusWithValidTemplate("gate", "default", "ps-a")
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(jcs).Build()
	if err := cl.Get(ctx, client.ObjectKeyFromObject(jcs), jcs); err != nil {
		t.Fatalf("failed to get created JobCommitStatus: %v", err)
	}

	r := &JobCommitStatusReconciler{Client: cl, Scheme: scheme}
	parentLabelKey := utils.CommitStatusGateLabelKeyForParent(jcs)
	sha := "abc123def456789012345678901234567890abcd"

	spoofed := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spoofed-job",
			Namespace: "default",
			Labels: map[string]string{
				parentLabelKey:                           utils.KubeSafeLabel(jcs.Name),
				promoterv1alpha1.EnvironmentLabel:        utils.KubeSafeLabel(testBranchDevelopment),
				promoterv1alpha1.JobCommitStatusShaLabel: sha,
			},
			// No OwnerReference to jcs: this Job is not actually owned by it.
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "spoof", Image: "busybox"}},
				},
			},
		},
	}
	if err := cl.Create(ctx, spoofed); err != nil {
		t.Fatalf("failed to create spoofed Job: %v", err)
	}

	ps := &promoterv1alpha1.PromotionStrategy{
		ObjectMeta: metav1.ObjectMeta{Name: "ps-a", Namespace: "default"},
		Spec:       promoterv1alpha1.PromotionStrategySpec{RepositoryReference: promoterv1alpha1.ObjectReference{Name: "repo"}},
	}

	got, err := r.ensureJobForEnvironment(ctx, jcs, ps, parentLabelKey, testBranchDevelopment, sha)
	if err != nil {
		t.Fatalf("ensureJobForEnvironment returned an error: %v", err)
	}
	if got.Name == spoofed.Name {
		t.Fatalf("ensureJobForEnvironment returned the spoofed Job %q instead of creating its own", spoofed.Name)
	}
	if !metav1.IsControlledBy(got, jcs) {
		t.Fatal("ensureJobForEnvironment returned a Job not owned by the JobCommitStatus")
	}

	var jobs batchv1.JobList
	if err := cl.List(ctx, &jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("failed to list Jobs: %v", err)
	}
	if len(jobs.Items) != 2 {
		t.Fatalf("expected the spoofed Job and one newly-created owned Job (2 total), got %d", len(jobs.Items))
	}
}

// TestEnsureJobForEnvironment_ForbiddenCreate verifies that an RBAC-forbidden Job creation is
// returned as a plain error (for the caller to report via a warning event and retry), not a panic
// or a silently-swallowed failure.
func TestEnsureJobForEnvironment_ForbiddenCreate(t *testing.T) {
	t.Parallel()

	scheme := utils.GetScheme()
	ctx := t.Context()

	jcs := jobCommitStatusWithValidTemplate("gate", "default", "ps-a")
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(jcs).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					return apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName(), errors.New("rbac: no create permission"))
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	if err := cl.Get(ctx, client.ObjectKeyFromObject(jcs), jcs); err != nil {
		t.Fatalf("failed to get created JobCommitStatus: %v", err)
	}

	r := &JobCommitStatusReconciler{Client: cl, Scheme: scheme}
	parentLabelKey := utils.CommitStatusGateLabelKeyForParent(jcs)
	ps := &promoterv1alpha1.PromotionStrategy{
		ObjectMeta: metav1.ObjectMeta{Name: "ps-a", Namespace: "default"},
		Spec:       promoterv1alpha1.PromotionStrategySpec{RepositoryReference: promoterv1alpha1.ObjectReference{Name: "repo"}},
	}

	_, err := r.ensureJobForEnvironment(ctx, jcs, ps, parentLabelKey, testBranchDevelopment, "abc123def456789012345678901234567890abcd")
	if err == nil {
		t.Fatal("expected an error from a forbidden Job creation, got nil")
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected a wrapped Forbidden error, got: %v", err)
	}
}

// TestEnqueueJobCommitStatusForPromotionStrategy verifies that a changed PromotionStrategy only
// enqueues the JobCommitStatus resources that reference it (matched via the shared
// PromotionStrategyRefField index), scoped to its own namespace, and never panics on an
// unexpected object type.
func TestEnqueueJobCommitStatusForPromotionStrategy(t *testing.T) {
	t.Parallel()

	scheme := utils.GetScheme()

	matching := jobCommitStatusWithValidTemplate("matching", "default", "ps-a")
	nonMatching := jobCommitStatusWithValidTemplate("non-matching", "default", "ps-b")
	otherNamespace := jobCommitStatusWithValidTemplate("matching-other-ns", "other-ns", "ps-a")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(matching, nonMatching, otherNamespace).
		WithIndex(&promoterv1alpha1.JobCommitStatus{}, PromotionStrategyRefField, PromotionStrategyRefIndexValues).
		Build()

	r := &JobCommitStatusReconciler{Client: cl}
	h := r.enqueueJobCommitStatusForPromotionStrategy()

	ps := &promoterv1alpha1.PromotionStrategy{
		ObjectMeta: metav1.ObjectMeta{Name: "ps-a", Namespace: "default"},
	}

	q := &controllertest.Queue{TypedInterface: workqueue.NewTyped[reconcile.Request]()}
	h.Update(t.Context(), event.UpdateEvent{ObjectOld: ps, ObjectNew: ps}, q)

	if q.Len() != 1 {
		t.Fatalf("expected exactly 1 enqueued request, got %d", q.Len())
	}
	req, _ := q.Get()
	want := types.NamespacedName{Namespace: "default", Name: "matching"}
	if req.NamespacedName != want {
		t.Fatalf("expected request for %v, got %v", want, req.NamespacedName)
	}

	// A non-PromotionStrategy object must be ignored, not panic.
	q2 := &controllertest.Queue{TypedInterface: workqueue.NewTyped[reconcile.Request]()}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "not-a-promotion-strategy", Namespace: "default"}}
	h.Generic(t.Context(), event.GenericEvent{Object: pod}, q2)
	if q2.Len() != 0 {
		t.Fatalf("expected no enqueued requests for a non-PromotionStrategy object, got %d", q2.Len())
	}
}
