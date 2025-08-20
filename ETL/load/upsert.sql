BEGIN TRANSACTION;

-- Upsert into fact_order_items
MERGE INTO `{project_id}.{bigquery_dataset}.fact_order_items` t
USING (
  SELECT
    COALESCE(s.order_id, f.order_id) AS order_id,
    COALESCE(s.product_key, f.product_key) AS product_key,
    COALESCE(s.user_key, f.user_key) AS user_key,
    COALESCE(s.distribution_center_key, f.distribution_center_key) AS distribution_center_key,
    COALESCE(s.status, f.status) AS status,
    COALESCE(s.created_at, f.created_at) AS created_at,
    COALESCE(s.shipped_at, f.shipped_at) AS shipped_at,
    COALESCE(s.delivered_at, f.delivered_at) AS delivered_at,
    COALESCE(s.returned_at, f.returned_at) AS returned_at,
    COALESCE(s.cost, f.cost) AS cost,
    COALESCE(s.item_quantity, f.item_quantity) AS item_quantity,
    COALESCE(s.sale_price, f.sale_price) AS sale_price,
    COALESCE(s.total_cost, f.total_cost) AS total_cost,
    COALESCE(s.total_sale_price, f.total_sale_price) AS total_sale_price,
    COALESCE(s.profit, f.profit) AS profit
  FROM `{project_id}.{bigquery_dataset}.fact_order_items` f
  FULL JOIN `{project_id}.{bigquery_dataset}.staging_fact_order_items` s
  ON f.order_id = s.order_id AND f.product_key = s.product_key
) u
ON t.order_id = u.order_id AND t.product_key = u.product_key
WHEN MATCHED THEN
UPDATE SET
  t.status = u.status,
  t.created_at = u.created_at,
  t.shipped_at = u.shipped_at,
  t.delivered_at = u.delivered_at,
  t.returned_at = u.returned_at,
  t.cost = u.cost,
  t.item_quantity = u.item_quantity,
  t.sale_price = u.sale_price,
  t.total_cost = u.total_cost,
  t.total_sale_price = u.total_sale_price,
  t.profit = u.profit
WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    product_key,
    user_key,
    distribution_center_key,
    status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    cost,
    item_quantity,
    sale_price,
    total_cost,
    total_sale_price,
    profit
  )
  VALUES (
    u.order_id,
    u.product_key,
    u.user_key,
    u.distribution_center_key,
    u.status,
    u.created_at,
    u.shipped_at,
    u.delivered_at,
    u.returned_at,
    u.cost,
    u.item_quantity,
    u.sale_price,
    u.total_cost,
    u.total_sale_price,
    u.profit
  );

-- Upsert into dim_products
MERGE `{project_id}.{bigquery_dataset}.dim_products` p
USING `{project_id}.{bigquery_dataset}.staging_dim_products` sp
ON sp.product_key = p.product_key
WHEN MATCHED THEN
UPDATE SET
  p.product_id = sp.product_id,
  p.category = sp.category,
  p.name = sp.name,
  p.brand = sp.brand,
  p.department = sp.department,
  p.sku = sp.sku
WHEN NOT MATCHED THEN
  INSERT (
    product_key,
    product_id,
    category,
    name,
    brand,
    department,
    sku
  )
  VALUES (
    sp.product_key,
    sp.product_id,
    sp.category,
    sp.name,
    sp.brand,
    sp.department,
    sp.sku
  );

-- Upsert into dim_users
MERGE `{project_id}.{bigquery_dataset}.dim_users` u
USING `{project_id}.{bigquery_dataset}.staging_dim_users` su
ON su.user_key = u.user_key
WHEN MATCHED THEN
UPDATE SET
  u.user_id = su.user_id,
  u.username = su.username,
  u.email = su.email,
  u.age = su.age,
  u.gender = su.gender,
  u.state = su.state,
  u.street_address = su.street_address,
  u.postal_code = su.postal_code,
  u.city = su.city,
  u.country = su.country,
  u.latitude = su.latitude,
  u.longitude = su.longitude,
  u.traffic_source = su.traffic_source
WHEN NOT MATCHED THEN
  INSERT (
    user_key,
    user_id,
    username,
    email,
    age,
    gender,
    state,
    street_address,
    postal_code,
    city,
    country,
    latitude,
    longitude,
    traffic_source
  )
  VALUES (
    su.user_key,
    su.user_id,
    su.username,
    su.email,
    su.age,
    su.gender,
    su.state,
    su.street_address,
    su.postal_code,
    su.city,
    su.country,
    su.latitude,
    su.longitude,
    su.traffic_source
  );

-- Upsert into dim_distributions
MERGE `{project_id}.{bigquery_dataset}.dim_distributions` d
USING `{project_id}.{bigquery_dataset}.staging_dim_distributions` sd
ON sd.distribution_center_key = d.distribution_center_key
WHEN MATCHED THEN
UPDATE SET
  d.distribution_center_id = sd.distribution_center_id,
  d.name = sd.name,
  d.latitude = sd.latitude,
  d.longitude = sd.longitude
WHEN NOT MATCHED THEN
  INSERT (
    distribution_center_key,
    distribution_center_id,
    name,
    latitude,
    longitude
  )
  VALUES (
    sd.distribution_center_key,
    sd.distribution_center_id,
    sd.name,
    sd.latitude,
    sd.longitude
  );

COMMIT TRANSACTION;
