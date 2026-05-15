# Databricks notebook source
# reconcile dash1 (Account Usage Dashboard V2) vs dash2 (Scapia Cost Attribution)
# pasting the exact dataset SQL from both dashboards, swapping in the same params
# that dash1 is being viewed with, then comparing the totals.

# COMMAND ----------

# dash1 params — these are the dataset defaults pulled from the dashboard JSON,
# except price_table which we flip to list_prices since account_prices is empty here
# (the same toggle the dashboard viewer is using to see non-zero numbers).
TIME_MIN  = "date_sub(current_date(), 30)"
TIME_MAX  = "current_date()"
WORKSPACE = "array('all')"
PRODUCTS  = "array('all')"
SERVERLES = "array('all')"
DISCOUNTS = "'sql=0.0;VECTOR_SEARCH=0.0;ALL_PURPOSE=0.0;MODEL_SERVING=0.0;JOBS=0.0;INTERACTIVE=0.0;LAKEHOUSE_MONITORING=0.0;DLT=0.0'"
PRICE_TBL = "'system.billing.list_prices'"
TIME_KEY  = "'Month'"
GROUP_KEY = "'Product'"
TOGGLE    = "'Dollars'"

# COMMAND ----------

# verbatim copy of the `usage_overview` dataset from dash1, params inlined
dash1_sql = f"""
with workspace as (
      SELECT
      account_id,
      workspace_id,
      workspace_name,
      workspace_url,
      status,
      concat(COALESCE(workspace_name, ''), ' (id: ', workspace_id, ')') AS workspace_full_name
      FROM system.access.workspaces_latest
),
usage_with_ws_filtered_by_date as (
  select
    case when workspace_name is null then concat('id: ', u.workspace_id) else workspace_full_name end as workspace,
    u.*
  from system.billing.usage as u
  left join workspace on u.workspace_id = workspace.workspace_id
  where u.usage_date between {TIME_MIN} and {TIME_MAX}
  and (array_contains({WORKSPACE}, concat(COALESCE(workspace.workspace_name, ''), ' (id: ', workspace.workspace_id, ')')) OR array_contains({WORKSPACE},'all'))
),
usage_filtered as (
  select *,
    CASE WHEN product_features.is_serverless = True THEN 'Serverless' ELSE 'Classic' END AS IsServerless,
    CASE WHEN product_features.is_photon = True THEN 'Photon' ELSE 'Spark' END AS IsPhoton
  from usage_with_ws_filtered_by_date
  where (array_contains({PRODUCTS}, billing_origin_product) OR array_contains({PRODUCTS},'all'))
    AND (array_contains({SERVERLES}, CASE WHEN product_features.is_serverless = True THEN 'Serverless' ELSE 'Classic' END) OR array_contains({SERVERLES},'all'))
),
parsed_discounts_table AS (
  WITH split_data AS (
    SELECT explode(split(regexp_replace(upper({DISCOUNTS}), '\\\\s+', ''), ';')) AS kv_pair FROM VALUES(1) AS dummy(x)
  ),
  clean_keys AS (
    SELECT ROW_NUMBER() OVER (ORDER BY kv_pair) AS order_id,
           split(kv_pair, '=')[0] AS product,
           try_cast(split(kv_pair, '=')[1] AS decimal(10,3)) AS discount,
           kv_pair AS combination,
           CASE WHEN contains(kv_pair, '=') THEN 1 ELSE 0 END AS ContainsValuePair
    FROM split_data
  )
  SELECT * FROM clean_keys WHERE ContainsValuePair = 1
),
prices as (
  select coalesce(price_end_time, date_add(current_date, 1)) as coalesced_price_end_time,
         sku_name, usage_unit, price_start_time,
         CASE WHEN {PRICE_TBL} = 'system.billing.list_prices'    THEN try_variant_get(to_variant_object(pricing), '$.effective_list.default', 'decimal(38,18)')
              WHEN {PRICE_TBL} = 'system.billing.account_prices' THEN try_variant_get(to_variant_object(pricing), '$.default', 'decimal(38,18)')
         END AS unit_px
  from IDENTIFIER({PRICE_TBL})
  WHERE currency_code = 'USD'
),
list_priced_usd as (
  select /*+ BROADCAST(p) */
      COALESCE(
        (1-try_cast(regexp_extract({DISCOUNTS}, '\\\\s*\\\\*\\\\s*=\\\\s*([0-9]*\\\\.?[0-9]+)', 1) AS decimal(10, 3))) * p.unit_px * u.usage_quantity,
        (1-COALESCE(discounts.discount, 0)) * p.unit_px * u.usage_quantity,
        p.unit_px * u.usage_quantity
      ) as usage_usd,
      usage_quantity AS usage_dbus,
      u.*
  from usage_filtered as u
  LEFT JOIN parsed_discounts_table AS discounts ON (discounts.product = u.sku_name OR discounts.product = u.billing_origin_product)
  left join prices as p
     ON u.sku_name = p.sku_name
    and u.usage_unit = p.usage_unit
    and (u.usage_end_time between p.price_start_time and p.coalesced_price_end_time)
)
select
  'dash1' as source,
  round(sum(usage_usd), 2)  as total_usd,
  round(sum(usage_dbus), 2) as total_dbus,
  count(*)                  as row_count
from list_priced_usd
"""
display(spark.sql(dash1_sql))

# COMMAND ----------

# verbatim copy of the `per_user` dataset from dash2, wrapped in a sum
dash2_sql = """
WITH per_user AS (
  WITH prices AS (
    SELECT
      sku_name,
      usage_unit,
      price_start_time,
      COALESCE(price_end_time, date_add(current_date, 1)) AS price_end_time,
      pricing.effective_list.default AS unit_price
    FROM system.billing.list_prices
    WHERE currency_code = 'USD'
  ),
  wh AS (SELECT warehouse_id, FIRST(warehouse_name IGNORE NULLS) AS warehouse_name FROM system.compute.warehouses GROUP BY warehouse_id),
  cl AS (SELECT cluster_id, FIRST(cluster_name IGNORE NULLS) AS cluster_name FROM system.compute.clusters GROUP BY cluster_id),
  jn AS (SELECT job_id, FIRST(name IGNORE NULLS) AS job_name FROM system.lakeflow.jobs GROUP BY job_id)
  SELECT
    COALESCE(u.identity_metadata.run_as, '(unattributed)') AS user_email,
    u.workspace_id,
    u.usage_date,
    u.billing_origin_product AS product,
    CASE
      WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN CONCAT('[Warehouse] ', COALESCE(wh.warehouse_name, u.usage_metadata.warehouse_id))
      WHEN u.usage_metadata.job_id       IS NOT NULL THEN CONCAT('[Job] ', COALESCE(jn.job_name, u.usage_metadata.job_id))
      WHEN u.usage_metadata.cluster_id   IS NOT NULL THEN CONCAT('[Cluster] ', COALESCE(cl.cluster_name, u.usage_metadata.cluster_id))
      WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN CONCAT('[Pipeline] ', u.usage_metadata.dlt_pipeline_id)
      WHEN u.usage_metadata.endpoint_id  IS NOT NULL OR u.usage_metadata.endpoint_name IS NOT NULL THEN CONCAT('[Endpoint] ', COALESCE(u.usage_metadata.endpoint_name, u.usage_metadata.endpoint_id))
      WHEN u.usage_metadata.app_id       IS NOT NULL OR u.usage_metadata.app_name IS NOT NULL THEN CONCAT('[App] ', COALESCE(u.usage_metadata.app_name, u.usage_metadata.app_id))
      ELSE '[Other] (none)'
    END AS compute_instance,
    CAST(u.usage_quantity * COALESCE(p.unit_price, 0) AS DOUBLE) AS cost_usd,
    CAST(u.usage_quantity AS DOUBLE) AS dbus
  FROM system.billing.usage u
  LEFT JOIN prices p
    ON  u.sku_name   = p.sku_name
    AND u.usage_unit = p.usage_unit
    AND u.usage_end_time BETWEEN p.price_start_time AND p.price_end_time
  LEFT JOIN wh ON wh.warehouse_id = u.usage_metadata.warehouse_id
  LEFT JOIN cl ON cl.cluster_id   = u.usage_metadata.cluster_id
  LEFT JOIN jn ON jn.job_id       = u.usage_metadata.job_id
  WHERE u.usage_date >= current_date() - 30
)
select
  'dash2' as source,
  round(sum(cost_usd), 2) as total_usd,
  round(sum(dbus), 2)     as total_dbus,
  count(*)                as row_count
from per_user
"""
display(spark.sql(dash2_sql))

# COMMAND ----------

# stack them side by side
display(spark.sql(f"""
({dash1_sql})
union all
({dash2_sql})
"""))

# COMMAND ----------

# also break it out per product to spot-check segment alignment
display(spark.sql(f"""
with d1 as (
  {dash1_sql.replace("'dash1' as source,", "billing_origin_product as product,").replace("from list_priced_usd","from list_priced_usd group by 1")}
), d2 as (
  {dash2_sql.replace("'dash2' as source,", "product,").replace("from per_user","from per_user group by 1")}
)
select coalesce(d1.product, d2.product) as product,
       d1.total_usd as dash1_usd, d2.total_usd as dash2_usd,
       round(d1.total_usd - d2.total_usd, 2) as delta_usd,
       d1.total_dbus as dash1_dbus, d2.total_dbus as dash2_dbus,
       round(d1.total_dbus - d2.total_dbus, 2) as delta_dbus
from d1 full outer join d2 on d1.product = d2.product
order by dash1_usd desc nulls last
"""))
