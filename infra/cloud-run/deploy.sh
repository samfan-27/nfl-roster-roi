#!/usr/bin/env bash
# Optional deployment; NOT used for the user's selected free GitHub plan.
set -euo pipefail
: "${GCP_PROJECT:?Set an existing project ID}"
: "${GCP_REGION:?Set a region}"
: "${IMAGE_DIGEST:?Use an already-built image URL with @sha256 digest}"
: "${SUPABASE_URL_SECRET_VERSION:?Pin a Secret Manager version}"
: "${SUPABASE_KEY_SECRET_VERSION:?Pin a Secret Manager version}"
: "${ALLOW_GCP_BILLING:?Set to approved only after explicit cost approval}"
[[ "$ALLOW_GCP_BILLING" == approved ]]
[[ "$IMAGE_DIGEST" == *@sha256:* ]]
gcloud billing projects describe "$GCP_PROJECT" --format='value(billingEnabled)' | rg '^True$'
# Runtime SA has access to ONLY the two named secrets. Invoker gets only job run.
runtime_sa="nfl-roi-runtime@$GCP_PROJECT.iam.gserviceaccount.com"
invoker_sa="nfl-roi-scheduler@$GCP_PROJECT.iam.gserviceaccount.com"
for name in nfl-roi-runtime nfl-roi-scheduler; do
  gcloud iam service-accounts describe "$name@$GCP_PROJECT.iam.gserviceaccount.com" --project "$GCP_PROJECT" >/dev/null 2>&1 ||
    gcloud iam service-accounts create "$name" --project "$GCP_PROJECT"
done
for secret in nfl-roi-supabase-url nfl-roi-supabase-service-key; do
  gcloud secrets add-iam-policy-binding "$secret" --project "$GCP_PROJECT" \
    --member="serviceAccount:$runtime_sa" --role=roles/secretmanager.secretAccessor >/dev/null
done
for mode in refresh weekly health; do
  gcloud run jobs deploy "nfl-roi-$mode" --project "$GCP_PROJECT" --region "$GCP_REGION" \
    --image "$IMAGE_DIGEST" --service-account "$runtime_sa" --args "$mode" \
    --tasks 1 --parallelism 1 --max-retries 2 --task-timeout 3300s --cpu 2 --memory 4Gi \
    --set-secrets "SUPABASE_URL=nfl-roi-supabase-url:$SUPABASE_URL_SECRET_VERSION,SUPABASE_SERVICE_ROLE_KEY=nfl-roi-supabase-service-key:$SUPABASE_KEY_SECRET_VERSION"
  gcloud run jobs add-iam-policy-binding "nfl-roi-$mode" --project "$GCP_PROJECT" --region "$GCP_REGION" \
    --member="serviceAccount:$invoker_sa" --role=roles/run.invoker >/dev/null
done
# Create/update PAUSED schedules. Resume only after verified execution and cutover.
for entry in 'daily|refresh|23 12 * * *' 'weekly|weekly|37 12 * * 4' 'weekly-retry|weekly|37 12 * * 5' 'health|health|41 */6 * * *'; do
  IFS='|' read -r name mode cron <<< "$entry"
  action=create
  if gcloud scheduler jobs describe "nfl-roi-$name" --project "$GCP_PROJECT" --location "$GCP_REGION" >/dev/null 2>&1; then
    action=update
    gcloud scheduler jobs pause "nfl-roi-$name" --project "$GCP_PROJECT" --location "$GCP_REGION"
  fi
  gcloud scheduler jobs "$action" http "nfl-roi-$name" --project "$GCP_PROJECT" --location "$GCP_REGION" \
    --schedule "$cron" --time-zone America/New_York --http-method POST \
    --uri "https://run.googleapis.com/v2/projects/$GCP_PROJECT/locations/$GCP_REGION/jobs/nfl-roi-$mode:run" \
    --oauth-service-account-email "$invoker_sa" --oauth-token-scope https://www.googleapis.com/auth/cloud-platform \
    --headers Content-Type=application/json --message-body '{}' --max-retry-attempts 2
  gcloud scheduler jobs pause "nfl-roi-$name" --project "$GCP_PROJECT" --location "$GCP_REGION"
done
