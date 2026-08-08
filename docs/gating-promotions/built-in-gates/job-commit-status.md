# Job Commit Status Controller

The Job Commit Status controller gates promotions on the result of a Kubernetes `Job` it creates and
runs for you. Use it for anything expressible as a one-shot batch task: evaluation suites, smoke
tests, database migration dry-runs, security scans, and similar checks that need to pass before (or
after) a promotion.

> [!IMPORTANT]
> This is a **one-shot** gate: one Job per proposed (or active) SHA, observed once to a terminal
> result. There is no recurring/scheduled re-check mode in this release — see [Non-goals](#non-goals).

## Overview

For each environment the gate applies to (resolved from the referenced `PromotionStrategy`, the same
way `GitCommitStatus` and `WebRequestCommitStatus` do — this resource does not enumerate environments
itself):

1. The controller resolves the SHA to gate, based on `spec.reportOn`:
   - `proposed` (default): the **proposed** hydrated commit — a pre-promotion gate that must pass
     before the change is promoted.
   - `active`: the **active** hydrated commit — a post-promotion check that re-validates the
     environment after a promotion has landed.
2. If no Job exists yet for that exact `(environment, SHA)` pair, the controller creates one from
   `spec.jobTemplate` and immediately publishes `pending`.
3. It observes the Job's own `Complete`/`Failed` conditions. No terminal condition means `pending`.
4. Once the Job reaches a terminal condition, `spec.success.when.expression` is evaluated against the
   finished Job to decide `success` vs. `failure` — never inferred directly from which condition
   fired. This lets you distinguish a real failure (nonzero exit) from an infrastructure problem
   (`ImagePullBackOff`, `OOMKilled`) the way Argo Workflows distinguishes Failed from Error.
5. The result is written to a `CommitStatus` correlated to that SHA, which `PromotionStrategy` checks
   before allowing promotion.

Once `success` or `failure` is durably recorded for a given SHA, the controller never recomputes it
for that SHA again — see [Superseded Jobs and stale-result isolation](#superseded-jobs-and-stale-result-isolation).

## Example Configuration

```yaml
apiVersion: promoter.argoproj.io/v1alpha1
kind: JobCommitStatus
metadata:
  labels:
    app.kubernetes.io/name: promoter
    app.kubernetes.io/managed-by: kustomize
  name: jobcommitstatus-sample
spec:
  key: eval-gate  # must match PromotionStrategy proposedCommitStatuses
  promotionStrategyRef:
    name: promotionstrategy-sample
  reportOn: proposed
  descriptionTemplate: 'eval check ({{ .Branch }}): {{ .Phase }}'
  success:
    when:
      # Documented reasonable default: the main container exited 0. Multi-completion Jobs should
      # compare against the configured completions instead (e.g. "Job.status.succeeded >= 3"), and
      # Jobs that need to distinguish a real failure from an infrastructure problem (ImagePullBackOff,
      # OOMKilled) can inspect Job.status and pod/container statuses in this expression.
      expression: 'Job.status.succeeded >= 1'
  jobTemplate:
    # Only metadata.labels/.annotations VALUES are templated (same variables as descriptionTemplate);
    # the rest of the Job spec, including the Pod template below, is used verbatim. Branch is
    # sanitized here because label values can't contain "/" (annotation values have no such limit).
    metadata:
      labels:
        example.com/branch: '{{ .Branch | replace "/" "-" }}'
      annotations:
        example.com/branch: '{{ .Branch }}'
    spec:
      # Native Job mechanics do all the lifecycle work the controller doesn't reimplement:
      backoffLimit: 0               # fail fast; set higher to let Job retry transient failures itself
      activeDeadlineSeconds: 600    # 10m timeout; Job is marked Failed if exceeded
      ttlSecondsAfterFinished: 86400 # 24h; Kubernetes garbage-collects the Job (and its Pods) after this
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: check
              image: busybox:stable
              # PROMOTER_JOB_SHA, PROMOTER_JOB_BRANCH, PROMOTER_JOB_PROMOTION_STRATEGY, and
              # PROMOTER_JOB_REPOSITORY are appended to every container/init container automatically —
              # no need to declare them here.
              command: ["sh", "-c", "echo \"checking $PROMOTER_JOB_BRANCH at $PROMOTER_JOB_SHA\" && exit 0"]
```

This mirrors [`config/samples/promoter_v1alpha1_jobcommitstatus.yaml`](https://github.com/argoproj-labs/gitops-promoter/blob/main/config/samples/promoter_v1alpha1_jobcommitstatus.yaml), which the controller's test suite loads from disk and creates against a real API server, so it's guaranteed to pass CRD validation.

Wire it into a `PromotionStrategy` by referencing the same key:

```yaml
apiVersion: promoter.argoproj.io/v1alpha1
kind: PromotionStrategy
metadata:
  name: promotionstrategy-sample
spec:
  gitRepositoryRef:
    name: promoter-testing
  proposedCommitStatuses:
    - key: eval-gate  # must match JobCommitStatus.spec.key
  environments:
    - branch: environment/development
    - branch: environment/staging
    - branch: environment/production
```

Other use cases follow the same shape as the example above — only `jobTemplate.spec.template` (the
container image and command) and `success.when.expression` change:

- **Smoke/integration tests** against the proposed environment: set `backoffLimit` above 0 to allow
  the Job's native retries to absorb transient flakiness.
- **Database migration dry-runs**: `backoffLimit: 0` (deterministic — same input, same result, so
  retrying won't help) with a generous `activeDeadlineSeconds`.
- **Security scans**: a longer `activeDeadlineSeconds`, and a `success.when.expression` that inspects
  the scan tool's own exit-code convention (many scanners use nonzero for "found issues" — decide
  whether that should be `failure` or an informational `success`).

There is intentionally only one full example above rather than one per use case: everything else is a
different container image, command, and `success.when.expression` on the same shape, and copying
several full manifests here would mean maintaining untested variants.

### `spec.key`

`spec.key` is the gate name your `PromotionStrategy` checks in `proposedCommitStatuses` or
`activeCommitStatuses` (per `spec.reportOn`). It must match exactly.

## Injected environment variables

Every container **and** init container in `jobTemplate.spec.template.spec` receives four plain
(pre-resolved, non-templated) environment variables, appended to whatever the container already
defines:

| Variable | Value |
|---|---|
| `PROMOTER_JOB_SHA` | The hydrated SHA this Job was created for (per `spec.reportOn`) |
| `PROMOTER_JOB_BRANCH` | The environment branch, e.g. `environment/production` |
| `PROMOTER_JOB_PROMOTION_STRATEGY` | The referenced `PromotionStrategy`'s name |
| `PROMOTER_JOB_REPOSITORY` | The `GitRepository` reference name from the `PromotionStrategy` |

These are literal strings resolved once at Job-creation time — not a template language, and not the
downward API. The running container just reads them like any other environment variable.

## Reserved labels

The controller sets three labels on every Job it creates, forming the identity it uses to detect an
already-created Job for a given `(parent, environment, SHA)` and to tell an owned Job apart from an
unrelated or spoofed one carrying the same labels (see [Security](#security)). `jobTemplate` must not
set these labels itself — doing so is rejected before any Job is created, reported through the `Ready`
condition:

| Label | Value |
|---|---|
| `promoter.argoproj.io/job-commit-status` | This `JobCommitStatus` resource's name |
| `promoter.argoproj.io/environment` | The environment branch (sanitized to a valid label value) |
| `promoter.argoproj.io/hydrated-sha` | The hydrated SHA the Job was created for |

These identify which Job belongs to which environment/SHA for the controller's own bookkeeping. They
are not intended as a stable integration point for user workloads — read `PROMOTER_JOB_*` env vars
instead.

## Templating

Two fields render as Go templates (`text/template` plus [Sprig](https://masterminds.github.io/sprig/)
functions, same engine as the other built-in gates): `spec.descriptionTemplate`, and — inside
`spec.jobTemplate` — only the **values** of `metadata.labels` and `metadata.annotations`. Keys are
never templated, and the rest of the Job spec (including `jobTemplate.spec.template`, the Pod
template) is used exactly as written — **arbitrary fields are not string-templated**.

Available variables (fields are Go struct names — `{{ .Job.Status.Succeeded }}`, not
`{{ .Job.status.succeeded }}` — since this is a different rendering path than
`success.when.expression`'s JSON-shaped `Job`, see below):

| Variable | Description |
|---|---|
| `{{ .Branch }}` | The environment branch |
| `{{ .PromotionStrategy }}` | The full `PromotionStrategy` object (spec and status) |
| `{{ .JobCommitStatus }}` | The full `JobCommitStatus` object (snapshot from the start of this reconcile) |
| `{{ .NamespaceMetadata.Labels }}` / `{{ .NamespaceMetadata.Annotations }}` | Labels/annotations of the namespace |
| `{{ .Job }}` | The child Job, only once it has reached a terminal phase; `nil` while still running (and always `nil` for `jobTemplate` labels/annotations, since the Job doesn't exist yet at that point) |

`spec.descriptionTemplate` is optional. Left unset (the default), the description is generated
automatically from the Job's own terminal condition (or `"Job <name> is running"` while pending) —
this is a reasonable default for most cases; set it only to customize the wording or surface extra
detail, for example:

```yaml
descriptionTemplate: 'eval check ({{ .Branch }}): {{ .Phase }}'
```

or, once terminal, something surfacing detail from the finished Job itself:

```yaml
descriptionTemplate: '{{ if .Job }}{{ (index .Job.Status.Conditions 0).Message }}{{ else }}running{{ end }}'
```

A raw branch (e.g. `environment/production`) contains `/` and is **not** a valid Kubernetes label
value — sanitize it if you use `{{ .Branch }}` in a label value:

```yaml
jobTemplate:
  metadata:
    labels:
      example.com/branch: '{{ .Branch | replace "/" "-" }}'
```

Annotation values have no such restriction.

**Error handling differs by field.** A `descriptionTemplate` that fails to render is reported via a
Warning event and the `Ready` condition, but the auto-generated description is used for that
reconcile — a broken description template can never block or flip a promotion decision. A
`jobTemplate` label/annotation template that fails to render **does** block Job creation for that
environment (reported the same way as any other creation failure — Warning event, `Ready` condition,
standard rate-limited retry), since at that point there's no Job yet to fall back to describing.

### `success.when.expression`

Evaluated against the finished Job **once it reaches a terminal condition**, converted to a
JSON-shaped map — so this expression uses JSON/lowercase field paths (`Job.status.succeeded`), unlike
the Go-template variables above. It must return a boolean: `true` means success, `false` means
failure. The documented default for most use cases:

```yaml
success:
  when:
    expression: "Job.status.succeeded >= 1"
```

Multi-completion Jobs should compare against the configured `completions` instead (e.g.
`"Job.status.succeeded >= 3"`). To distinguish a real failure from an infrastructure problem, inspect
`Job.status` (and pod/container statuses) directly in the expression, the way Argo Workflows
distinguishes Failed from Error.

## Namespace, service account, and credentials

The Job always runs in the `JobCommitStatus`'s own namespace — there is no cross-namespace target.
It uses whatever `serviceAccountName` you set on `jobTemplate.spec.template.spec` (the Pod template's
own field, used as-is); if you don't set one, it gets the namespace's `default` ServiceAccount, the
same as any other Job you'd create directly with `kubectl apply`. The controller does not create,
generate, or inject a ServiceAccount, and grants the Job no additional credentials beyond what you
configure on the Pod template yourself — it's a plain Job running beside its parent, nothing more.

## Native Job lifecycle controls

The controller does not reimplement retry, timeout, or cleanup semantics — it embeds
`spec.jobTemplate` verbatim into the created Job and lets native Job mechanics carry that weight:

- **`backoffLimit`** — retry semantics. `0` fails fast (appropriate for deterministic checks); higher
  values let the Job absorb transient failures on its own before the gate reports `failure`.
- **`activeDeadlineSeconds`** — timeout. The Job (and the gate) is marked `failure` if this is
  exceeded.
- **`ttlSecondsAfterFinished`** — cleanup. Kubernetes garbage-collects the Job (and its Pods) this long
  after it finishes. Not defaulted by the controller; a reasonable suggestion is `86400` (24h). A
  controller-managed finalizer is added to every Job at creation and removed only once its terminal
  outcome has been durably recorded in a `CommitStatus` — so even a very short TTL can't delete the
  Job before the controller has observed it.

None of these are defaulted by the controller: whatever you set (or don't) on `jobTemplate.spec` is
exactly what the Job does.

## Superseded Jobs and stale-result isolation

When a new proposed (or active, per `reportOn`) SHA appears for an environment, the controller
immediately publishes `pending` and creates a **new** Job for that SHA — it does not wait for, cancel,
or otherwise touch the previous SHA's Job. The two are never the same object: Job identity includes
the SHA (see [Reserved labels](#reserved-labels)), so a result from an old Job can never be attributed
to a newer SHA.

The controller does not eagerly delete superseded Jobs. They're left for `ttlSecondsAfterFinished` (or
a manual `kubectl delete job`) and Kubernetes' owner garbage collection (on parent deletion) to reclaim
— this keeps retention a native-Job concern rather than something the controller re-implements. If an
old Job finishes after a newer one has already started, its result is simply never looked at again:
the controller only ever asks "does a Job exist for the *current* SHA," never "what's the latest
Job's result regardless of SHA."

## Security

Job identity (the three [reserved labels](#reserved-labels)) is also a security boundary: anyone who
can create a Job in the same namespace can set matching labels on it, so the controller never trusts a
label match alone. Before treating an existing Job as "the" result for an environment/SHA, it also
checks that Job is actually owned (`metav1.IsControlledBy`) by this `JobCommitStatus`. An
unrelated or spoofed Job carrying the right labels but the wrong (or no) owner reference is ignored,
and the controller creates its own, properly owned Job instead.

## Troubleshooting

**Job stuck pending — scheduling or image pull failure.** Inspect the Job's Pods directly
(`kubectl describe pod -l job-name=<job>`); the gate reports `pending` for as long as the Job has no
terminal condition, which includes a Pod stuck in `ImagePullBackOff` or unschedulable. Set
`activeDeadlineSeconds` so a stuck Job eventually turns `failure` instead of blocking forever.

**Gate reports failure, but the container looked fine.** Check `success.when.expression` against the
actual `Job.status` — a expression bug (wrong field path, wrong comparison) reports as `failure` just
like a real Job failure. `kubectl get jobcommitstatus <name> -o yaml` and check `status.environments[].reason`.

**Job retried more than expected, or not at all.** That's `jobTemplate.spec.backoffLimit`, not
anything the controller controls — the controller creates the Job once and never touches it again;
retries within a single Job attempt are native Job/kubelet behavior.

**`JobCreateFailed` warning event / RBAC.** If the controller's ServiceAccount lacks `create` on
`batch/jobs` (or the Job spec itself is invalid), Job creation fails, the gate stays `pending` with
reason `JobCreateFailed`, and a Warning event and the `Ready` condition report the underlying error.
Check with `kubectl auth can-i create jobs --as=system:serviceaccount:<namespace>:<controller-sa>
-n <namespace>`; the controller only ever needs `get`, `list`, `watch`, `create`, and `patch` on
`batch/jobs` — never `update`, `delete`, or anything CronJob-related.

**Stale/completed Jobs piling up.** Expected if `ttlSecondsAfterFinished` is unset or generous — the
controller never deletes Jobs itself (see [Superseded Jobs](#superseded-jobs-and-stale-result-isolation)).
Set `ttlSecondsAfterFinished` on `jobTemplate.spec`, or clean up manually with
`kubectl delete job -l promoter.argoproj.io/job-commit-status=<name>`; either is safe at any time since
the controller only ever looks at the Job for the *current* SHA.

**A newer SHA doesn't seem to reflect an old Job's result.** That's correct — see
[Superseded Jobs and stale-result isolation](#superseded-jobs-and-stale-result-isolation). Check
`status.environments[].sha` on the `JobCommitStatus` against the `PromotionStrategy`'s current
proposed/active SHA for that branch; if they match, you're looking at the right (current) result.

## Non-goals

The following are explicitly out of scope for this release:

- **Cross-namespace Job targets.** The Job always runs in the `JobCommitStatus`'s own namespace.
- **A generated or managed ServiceAccount.** The Job uses whatever `serviceAccountName` the Pod
  template sets (or the namespace default); the controller does not create one for you.
- **Arbitrary field substitution.** Templating is limited to `descriptionTemplate` and
  `jobTemplate.metadata.labels`/`.annotations` values — the rest of `jobTemplate.spec` (including the
  Pod template) is used verbatim.
- **Rerunning a Job for the same SHA.** Once success or failure is durably recorded for a SHA, it's
  final; a new attempt requires a new proposed (or active) commit.
- **CronJob / recurring, self-recovering checks.** This gate is one Job per SHA, observed once to a
  terminal result. A periodic "is the environment healthy *right now*" check that can flip back to
  success without a new commit is a different mental model (environment-level, not commit-level) and
  needs its own API design — tracked as a possible follow-up in
  [issue #1597](https://github.com/argoproj-labs/gitops-promoter/issues/1597), not part of this gate.

## Field reference

Field-level documentation (required/optional, defaults, validation) is maintained on the API types.
Use either:

- **Godoc:** `api/v1alpha1/jobcommitstatus_types.go`
- **CLI:** `kubectl explain jobcommitstatus.spec` (and drill down, e.g.
  `kubectl explain jobcommitstatus.spec.jobTemplate`)
