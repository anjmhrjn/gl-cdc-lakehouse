-- EXECUTE on the mask and filter functions for gl_engineers. Attaching a function as a
-- mask or filter needs EXECUTE on it, so any engineer can re-run 03_masks_filters.sql.
-- GRANT is idempotent: granting a privilege a principal already holds is a no-op.
--
-- The functions stay owned by whoever first ran this job. Terraform gives the catalogs
-- and schemas to gl_engineers, but the SQL reference documents no ALTER FUNCTION
-- ... OWNER TO, so ownership is not changed here. Transferring it is a one-off
-- Catalog Explorer step, described in ARCHITECTURE.md.
--
-- Catalog and schema grants come from Terraform. Analysts need no privilege on
-- bu_filter to query a filtered gold table. The docs do not say so, but the test user
-- toggle on dev confirmed it (ARCHITECTURE.md).

GRANT EXECUTE ON FUNCTION silver.mask_account_number TO `gl_engineers`;
GRANT EXECUTE ON FUNCTION silver.mask_pii TO `gl_engineers`;
GRANT EXECUTE ON FUNCTION silver.mask_payload TO `gl_engineers`;
GRANT EXECUTE ON FUNCTION silver.mask_account_row TO `gl_engineers`;
GRANT EXECUTE ON FUNCTION silver.bu_filter TO `gl_engineers`;
