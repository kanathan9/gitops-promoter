package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
)

func TestFinalizerRepro(t *testing.T) {
	ctx := context.Background()
	name := "finalizer-repro-job"

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Finalizers: []string{promoterv1alpha1.JobCommitStatusJobFinalizer},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "c", Image: "busybox"}},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = k8sClient.Delete(ctx, job) }()

	var fetched batchv1.Job
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("get: %v", err)
	}
	fmt.Printf("after create: finalizers=%v\n", fetched.Finalizers)

	original := fetched.DeepCopy()
	fetched.Finalizers = nil
	if err := k8sClient.Patch(ctx, &fetched, client.MergeFrom(original)); err != nil {
		t.Fatalf("patch remove finalizer: %v", err)
	}
	fmt.Printf("after patch: finalizers=%v resourceVersion=%v\n", fetched.Finalizers, fetched.ResourceVersion)

	var refetched batchv1.Job
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &refetched); err != nil {
		t.Fatalf("get after patch: %v", err)
	}
	fmt.Printf("refetched: finalizers=%v\n", refetched.Finalizers)

	if err := k8sClient.Delete(ctx, &refetched); err != nil {
		t.Fatalf("delete: %v", err)
	}
	fmt.Println("delete call returned no error")

	for i := 0; i < 20; i++ {
		var check batchv1.Job
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &check)
		if apierrors.IsNotFound(err) {
			fmt.Printf("iter %d: gone\n", i)
			return
		}
		if err != nil {
			t.Fatalf("get after delete: %v", err)
		}
		fmt.Printf("iter %d: still exists, finalizers=%v deletionTimestamp=%v\n", i, check.Finalizers, check.DeletionTimestamp)
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("job never disappeared")
}
