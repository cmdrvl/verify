-- verify.constraint_set_id: fixtures.query_rules.orphan_rows
-- verify.rule_id: ORPHAN_PROPERTY_TENANT
-- verify.severity: error
-- verify.bindings: property,tenants

SELECT
  'property' AS binding,
  'tenant_id' AS field,
  property.tenant_id AS value,
  property.property_id AS key__property_id
FROM property
LEFT JOIN tenants
  ON property.tenant_id = tenants.tenant_id
WHERE tenants.tenant_id IS NULL;
