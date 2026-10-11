# RetailLake private S3 connection — partially applied; validation blocked

## Status — local route selected, investigation paused

On 11 October 2026, the user selected local PySpark with AWS S3. This document is historical connection evidence, not the active implementation plan. Follow ../Local_PySpark_Plan.md and ../RetailLake_Progress_Checklist.md. No successful Databricks S3 read was confirmed. Free Edition incompatibility was not established by the observed failure.

The proposed RCP exception remains UNSAVED and must not be applied for the local route. The approved SCP exception was saved and remains pending cleanup review. Dedicated IAM/Databricks resources and failed file-event teardown also need cleanup review. Updating these documents makes no AWS or Databricks changes.

## Actual result — 11 October 2026

User approved the proposed access. Created the IAM role and inline policy in account 037315459859. Created Databricks credential retaillake_bronze_read with read-only enabled. Updated AWS trust with external ID 195ac705-d8af-4081-9cd3-3e679d9888ea and explicit self-trust; AWS confirmed success. Verified persisted trust and permission JSON in the console.

Two credential validations failed at Assume Role. Self Assume Role and External ID Condition were skipped. Exact error: PERMISSION_DENIED: Failed to get credentials: the AWS IAM role in the Unity Catalog credential is not configured correctly. Please contact your metastore admin or the credential owner to update the configuration in AWS.

At this initial attempt, no external location or volume was created. No AWS compute was launched. A later attempt included automatic file events and failed provisioning and teardown, so remaining notification resources are unconfirmed. No S3 read from Databricks has succeeded. Root cause is unconfirmed; subsequent organization policy findings are recorded below. Screenshot: validation-result.png.

Account: 037315459859. Bucket region: ap-south-1.

Proposed IAM role: RetailLakeDatabricksBronzeReadRole (root role path).
Proposed inline policy: RetailLakeBronzeReadOnly, using bronze-read-policy.json.
Proposed Databricks credential: retaillake_bronze_read, marked read-only.
Proposed external location: retaillake_bronze_olist, marked read-only.
URL: s3://retaillake-dev-037315459859-mumbai/bronze/olist/

The policy grants object reads only in this prefix, prefix-restricted listing, bucket region lookup, and role self-assumption. It grants no S3 writes/deletes, other bucket access, KMS access, or infrastructure creation. The bucket uses SSE-S3; KMS permissions are unnecessary.

Sequence after approval:
1. Verify authenticated AWS account and check for an existing role with this name.
2. Create the role with bootstrap-trust.json and attach the prepared read policy. The bootstrap external ID 0000 is a temporary setup value, not the completed configuration.
3. Create the read-only Databricks storage credential using the role ARN. Record its generated external ID. Credential creation is the first real check that the Free workspace permits this feature; visible forms alone do not prove backend support.
4. Replace the final trust template placeholder with the actual external ID and apply it. This also adds explicit self-trust.
5. Validate the credential, including self-assumption.
6. Create the read-only external location for the Bronze prefix. Disable file events if the UI permits; do not provision SNS/SQS or force-create past failed access validation.
7. Register an external volume if permitted and read the three CSVs from that S3-backed volume. Verify row counts and sales reconciliation against the prototype.

Keep S3 private and encrypted. The permission change allows Databricks Unity Catalog to obtain temporary AWS access to the specified prefix. No access keys are needed. S3 requests/downloads may incur usage charges. No AWS compute is provisioned. Cross-region read costs and performance should be reviewed before repeated runs.

If storage credential creation or validation is blocked by the Free tier, report the exact error and stop this branch. Do not expand permissions based only on a generic validation failure. Record any partial resources and agree cleanup before deletion. Local processing remains a fallback.

Silver/Gold output access is a separate later design and approval. This first connection does not grant that access.

Source checked 11 October 2026:
https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual

## Manual external-location retry (2026-10-11)
Submitted retaillake_bronze_olist_manual for s3://retaillake-dev-037315459859-mumbai/bronze/olist/ using retaillake_bronze_read, read-only, Automatic file events with user approval.
Validation returned Permission Denied: Read failed; Assume Role failed; Self Assume Role and External ID Condition skipped; File Events Resource Provision and Teardown failed.
Stopped before Force create. No successful external location or S3 read confirmed. File-event cleanup is unconfirmed because teardown failed. Evidence: manual-location-failed.png.

## Organization policy diagnosis and approved SCP change
Saved p-92jp4665 with the user-approved Oregon STS self-assumption exception for RetailLakeDatabricksBronzeReadRole and removal of invalid builderid:* entry. AWS confirmed successful update. Databricks credential validation failed twice at initial AssumeRole afterward.
Found root-attached RCP p-027sbkih2r ManagedAccountResourceControlPolicy: DenyAnyoneOutsideMyOrgAndAWS applies sts:* and denies non-service external organization principals. This matches Databricks master-role access and is a stronger blocker candidate.
Prepared but NOT saved rcp-proposed-databricks.json: exempt only exact Databricks UC master principal from the first external-org deny on exact RetailLake role resource. Other resources and external principals remain denied; second AWS source-org statement unchanged. AWS editor showed 0 errors and 1 warning. Superseded by the user's local-route decision: do not save this draft. Connection remains unverified.
