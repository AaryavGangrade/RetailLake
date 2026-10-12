# Outstanding AWS / Databricks housekeeping review

Status: blocked on correct management-account authentication and dependency review. Nothing below was deleted or applied during the completion segment. The new scoped pipeline role is active; it is separate from the abandoned Databricks role.

## Authentication blocker

Profile `retaillake-management` was created by the approved browser sign-in, but STS/login output returned project account **037315459859** again. Its name does not make it a management-account session. Select **Aaryav-Gangrade-Team Management Account**, account **209766594914**, during a new sign-in. Confirm the account ID before policy work:

```powershell
aws login --profile retaillake-management --region us-east-1
aws sts get-caller-identity --profile retaillake-management --region us-east-1 --query Account --output text
```

Expected: `209766594914`. Current member-account SCP/RCP DescribePolicy calls returned AccessDenied. Do not weaken permissions in response to that error.

## Concrete resources to review

| Resource | Required review before changing it |
| --- | --- |
| AWS role `RetailLakeDatabricksBronzeReadRole` in 037315459859 | Check attached/inline policies, RoleLastUsed, Databricks credentials/locations and setup requests. Delete only after confirming there are no dependencies and obtaining approval. Preserve the new `RetailLakeLocalPipelineRole`. |
| Databricks credential `retaillake_bronze_read` | Inspect any external locations referencing it and workspace setup requests. Never delete volumes, managed tables or notebooks while removing this failed connection attempt. |
| Automatic pending setup requests, including `4fb80b10-e4cb-4e14-942a-ff40b7884bee` and others in historical evidence | Inspect current status and possible created IAM/file-event resources. Teardown previously failed; failure is not proof that nothing exists. |
| SCP `p-92jp4665` | Fetch the live policy and attachment targets. Prepare a diff reversing only the Databricks-specific Oregon STS exception and its two narrow deny statements. Preserve `ap-south-1` and the prior invalid builderid action removal. Validate the whole resulting JSON before asking approval to save it. |
| RCP `p-027sbkih2r` | Fetch live policy and confirm whether the previously unsaved exception exists. No exception is required for the local pipeline; do not save the old draft. Current state is unconfirmed from the member session. |
| Current credits/expiry and Free Tier email preferences | Review billing for the actual payer/project account. Project GetAccountPlanState returned Missing data; empty free-tier usage is not a balance. Verify enabled email preferences separately from budget email verification. |

## Preserved policy history

The saved SCP exception permitted regional STS AssumeRole for the dedicated Databricks role and restricted other Oregon assumptions. Historical backups contain an invalid `builderid:*` action; blindly restoring them may fail or undo unrelated fixes. The Mumbai allowance is required by the current private bucket pipeline and must remain.

The RCP outside-organization denial was a likely Databricks blocker, but no successful connection was verified and no RCP change was saved in the observed draft session. Do not present that inference as a proven root cause or claim Free Edition universally cannot access S3.

## Approval boundary

Read and inventory first. Save a concrete current-policy diff and named resource/dependency inventory before requesting final cleanup approval. No broad organization-policy weakening, unrelated resource deletion or account-plan upgrade is needed for the completed local pipeline. Until access is corrected, these items remain visibly pending in the checklist and final status.
