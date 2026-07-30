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
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
	"github.com/argoproj-labs/gitops-promoter/internal/types/constants"
	"github.com/argoproj-labs/gitops-promoter/internal/utils"
)

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
func jobCommitStatusForGate(name, namespace, psName, key string, labels, annotations map[string]string) *promoterv1alpha1.JobCommitStatus {
	jcs := jobCommitStatusWithValidTemplate(name, namespace, psName)
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
		jcs = jobCommitStatusForGate(name+"-gate", "default", name, gateKey,
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

		jcs = jobCommitStatusForGate(name+"-gate", "default", name, gateKey, nil, nil)
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
		jcs = jobCommitStatusForGate("reserved-label-gate", "default", name, "reserved-label-gate",
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
