# AI usage

Claude wrote most of the code in this repo. This file logs what it got wrong, how that
was caught, and what the fix was. The point is an honest record, not a highlight reel.

## Hot account share came out at 34 percent instead of 30

**Produced:** the generator picked the hot account's (business unit, currency) group
with probability 0.5 and, inside those journals, put each line on the hot account with
probability 0.6, aiming at the 30 percent share the data contract asks for.

**Wrong:** the fallback branch chose a group at random from all groups, including the
hot group. So hot group journals were selected more often than 0.5 and the share came
out at 34 percent.

**Caught by:** a verification script over the generated files that counted lines per
account. It reported `hot account ACC-000017: 575/1693 journal lines = 34.0%`.

**Fix:** the fallback now chooses from groups other than the hot group. Measured share
is 32.6 percent on a 2000 event run, which is within the variance of a run that also
deletes whole journals.

## Catalogs were created with no storage root

**Produced:** `databricks_catalog` resources with no `storage_root`, on the reasoning
that managed tables should live in the metastore's default managed storage and stay
separate from the landing bucket. That reasoning was written into ARCHITECTURE.md as a
deliberate decision.

**Wrong:** the assumption that the metastore had a storage root to fall back on. It does
not. The account has Default Storage enabled, and the metastore summary has no
`storage_root` field, which I read as a detail rather than the constraint it was. A
catalog created that way is rejected outright:

```
cannot create catalog: Metastore storage root URL does not exist. Default Storage is
enabled in your account.
```

**Caught by:** `terraform apply`, run by Anuj. `terraform plan` cannot catch it, because
the failure only happens when the Unity Catalog API is actually called. Everything
before the catalog applied cleanly, so the run got as far as the S3 bucket, the KMS key,
the IAM role, the storage credential and the landing external location before stopping.

**Fix:** a second external location per environment at `<env>/managed`, granted
`CREATE_MANAGED_STORAGE`, with each catalog's `storage_root` pointing at it. The
ARCHITECTURE.md section that argued for the old design has been rewritten rather than
quietly deleted. Re-planning showed 10 to add, 0 to change, 0 to destroy, so nothing
that had already been created was disturbed.

**Worth noting:** the error also shows the limit of `terraform plan` as a check. Provider
side validation of a resource that does not exist yet is not a thing plan can do.
