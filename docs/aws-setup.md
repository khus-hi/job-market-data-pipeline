# AWS setup: S3 raw landing zone

This runbook creates the S3 bucket that the pipeline lands raw JSON into, plus an IAM user that can
reach **only** that bucket's `raw/` prefix. Everything is done in the AWS web console, so no AWS CLI
is needed. Teardown is at the bottom.

**Cost:** the pipeline writes about 4 to 12 small JSON files a day (well under 10 MB a month), which
costs a fraction of a cent on S3. The steps below still set up two safeguards so nothing can be
billed by surprise.

## 0. Account safeguards (do these first)

1. When you sign up at <https://aws.amazon.com/free/>, choose the **Free plan** (not the Paid
   plan). On the Free plan, AWS spends your free credits and does not charge your card. Check the
   current terms on that page before you sign up.
2. **Lock away the root user.** The email you sign up with is the root user. Turn on MFA for it
   (top right: your name → *Security credentials* → *Assign MFA device*) and never use it for
   day-to-day work or create access keys for it.
3. **Zero-spend budget:** *Billing and Cost Management → Budgets → Create budget → Use a template →
   Zero spend budget*. Enter your email. AWS emails you the moment anything costs even $0.01.

## 1. Create the bucket

*S3 → Create bucket*

| Setting | Value |
|---|---|
| Region | `us-east-1` (US East, N. Virginia). The region must match `AWS_REGION` in `.env`. |
| Bucket name | globally unique, e.g. `job-market-raw-<your-initials>-<random-4-digits>` |
| Object Ownership | ACLs disabled (default) |
| Block *all* public access | **On** (default). The data never needs to be public. |
| Bucket Versioning | Disabled. The pipeline is idempotent, so a re-run simply rewrites a day. |
| Default encryption | SSE-S3 (default) |

Click **Create bucket**.

## 2. Create a least-privilege policy

*IAM → Policies → Create policy → JSON tab*

1. Paste the contents of [`infra/iam-policy-raw-landing.json`](../infra/iam-policy-raw-landing.json).
2. Replace both `REPLACE_WITH_BUCKET_NAME` with your bucket name.
3. Name it `job-market-raw-landing` → **Create policy**.

What it allows, and nothing else:
- list objects under `raw/` in this one bucket
- read, write and delete objects under `raw/` in this one bucket

It does **not** allow creating or deleting buckets, reading other buckets, or any other AWS service.

## 3. Create the pipeline's IAM user

*IAM → Users → Create user*

1. User name: `job-market-pipeline`. Leave **"Provide user access to the AWS Management Console"**
   unchecked, because this user is for code only.
2. Permissions: **Attach policies directly** → tick `job-market-raw-landing` → **Create user**.
3. Open the user → *Security credentials* → **Create access key** → *Local code* → **Create**.
4. Copy the **Access key** and **Secret access key**. The secret is shown only once.

## 4. Configure `.env`

Add these lines to `.env` (never to any committed file):

```
STORAGE_BACKEND=s3
S3_BUCKET=<your bucket name>
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<access key>
AWS_SECRET_ACCESS_KEY=<secret access key>
```

Leave `S3_ENDPOINT_URL` unset. That variable is only for MinIO.

## 5. Verify

```
python -m ingestion.pipeline
```

The log shows `landed N files / M postings in s3://<bucket>/raw/postings/dt=YYYY-MM-DD/`. In the
console, *S3 → your bucket → raw/ → postings/* should show one `dt=` folder per day.

To move days already collected on disk into S3, run `python -m ingestion.migrate_local_to_s3`.

## Teardown

Do this when the project is finished, or if you ever see an unexpected budget alert:

1. *S3 → bucket → Empty* (type `permanently delete`), then *Delete bucket*.
2. *IAM → Users → job-market-pipeline → Security credentials →* deactivate and **delete the access
   key**, then delete the user.
3. *IAM → Policies → job-market-raw-landing → Delete*.
4. Remove the `AWS_*` and `S3_*` lines from `.env` and set `STORAGE_BACKEND=local`.
5. Keep the zero-spend budget. It costs nothing.

## If a key ever leaks

If a key is pushed to GitHub, pasted in a chat, or shared anywhere else: *IAM → Users →
job-market-pipeline → Security credentials →* **deactivate the key immediately**, then delete it
and create a new one. The policy limits what a leaked key can do, but treat any leak as urgent.
