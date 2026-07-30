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
		Expect(jobs.Items).To(BeEmpty())
	})
})

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
