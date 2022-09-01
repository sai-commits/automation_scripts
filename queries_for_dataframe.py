
PLAN_CASH_EXTRACTED = """
SELECT
  DISTINCT pca.plan_id,
  pca.amount_cents AS cash_adjustment,
  SPLIT(REGEXP_EXTRACT(notes,r'CashOrder#401k-(.*)-ach'),'#')[
OFFSET
  (0)] AS failed_source_type,
  SPLIT(REGEXP_EXTRACT(notes,r'CashOrder#401k-(.*)-ach'),'#')[
OFFSET
  (1)] AS failed_source_id,
  CAST(REGEXP_EXTRACT(notes,r'Unit#([0-9]+)') AS int64) AS failed_plan_collection_unit_id,
  CAST(REGEXP_EXTRACT(notes,r'Collection#([0-9]+)') AS int64) AS failed_plan_collection_id,
  pcu.source_type,
  pcu.source_id,
  pca.id
FROM
  `data-gl.glproductionview.plan_cash_adjustments` pca
LEFT JOIN
  `data-gl.glproductionview.plan_collection_units` pcu
ON
  CAST(REGEXP_EXTRACT(notes,r'Unit#([0-9]+)') AS int64) = pcu.id 
"""

PLAN_COLLECTION_GENERATED = """
SELECT
  plan_id,
  amount_cents,
  source_type,
  source_id
  
FROM
  `data-gl.glproductionview.plan_collection_units`
WHERE
  state = 'generated'
  AND paused_at IS NULL  
  AND source_type = 'PlanCashAdjustment'
  AND collection_method = 'ach'
  ORDER BY case when source_type = 'PlanCashAdjustment' then 1 else 2 end
"""

COLLECTION_UNITS_OF_CASH = """
SELECT
  source_id,
  source_type,
  plan_collection_id,
  amount_cents
FROM
  `data-gl.glproductionview.plan_collection_units`
WHERE
  source_type = 'PlanCashAdjustment'
"""

ALL_PLAN_COLLECTIONS = """
SELECT
  source_id,
  source_type,
  plan_collection_id,
  amount_cents
FROM
  `data-gl.glproductionview.plan_collection_units`
  WHERE 
state IN('reversed','generated')
  """
NIP_AUDIT_QUERY = """SELECT
  dc_plan_id,
  plan_start_date,
  company_name,
  payroll_provider_identifier,
  ps_first_name,
  ps_email,
  ROUND(SUM(pretax),2) AS pretax,
  ROUND(SUM(Match),2) AS match,
  ROUND(SUM(Roth),2) AS roth,
  plan_number
FROM
  `data-gl.prod_ops_sandbox.nip_quarterly_audit`
WHERE
  CONCAT('Q',EXTRACT(QUARTER
    FROM
      basis_date),EXTRACT(YEAR
    FROM
      basis_date)) = 'Q12022'
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  10
ORDER BY
  4 limit 5"""
