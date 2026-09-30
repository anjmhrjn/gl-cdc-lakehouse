#!/usr/bin/env bash
# Defines the gl-dev and gl-prod profiles that databricks.yml names, for GitHub OIDC.
#
# Environment variables alone are not enough. With CLI 1.8.0, when the bundle names a
# profile the CLI ignores DATABRICKS_AUTH_TYPE from the environment and fails with
# "cannot configure default credentials". So CI writes the profiles themselves.
#
# audience must match the federation policy's audience (the gl-cicd application ID).
# For a workspace host the CLI otherwise asks GitHub for a token with the workspace
# token endpoint as audience, which the policy does not list.
#
# Nothing here is secret: the ID token is fetched at run time from GitHub.
set -euo pipefail

: "${DATABRICKS_HOST:?}" "${DATABRICKS_CLIENT_ID:?}" "${DATABRICKS_TOKEN_AUDIENCE:?}"

for profile in gl-dev gl-prod; do
  cat >> ~/.databrickscfg <<CFG
[$profile]
host = $DATABRICKS_HOST
auth_type = github-oidc
client_id = $DATABRICKS_CLIENT_ID
audience = $DATABRICKS_TOKEN_AUDIENCE

CFG
done
