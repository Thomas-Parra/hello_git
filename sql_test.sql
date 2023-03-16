WITH base AS (
    SELECT
      DATE_TRUNC('day', orders.created_at) AS order_day,
      clients.HUNTER_ID as HUNTER_ID,
      orders.client_id AS client_id,
      clients.name  as cliente_name ,
      clients.warehouse AS warehouse,
      clients.BASKET_SEGMENTATION AS BASKET_SEGMENTATION,
      clients.SEGMENTATION_MONETARY AS SEGMENTATION_MONETARY,
      AVG_TICKET as AVG_TICKET,
      case when AUTONOMY_RATE is null then 0  else AUTONOMY_RATE end as AUTONOMY_RATE , 
      clients.source_country AS source_country,
      clients.R9_H3 as nanozone,
      order_details.order_id AS order_id,
      orders.total AS total,
      ROW_NUMBER() OVER (PARTITION BY orders.client_id,orders.source_country, order_day ORDER BY products.goal_category_name) AS orders_by_day
    FROM prod_modeled.order_details AS order_details
      left JOIN prod_modeled.orders AS orders
         ON order_details.order_id = orders.id
           AND order_details.client_id = orders.client_id 
           AND order_details.source_country = orders.source_country
      left JOIN prod_modeled.clients as clients 
         ON order_details.client_id = clients.id
           AND order_details.source_country = clients.source_country
      left JOIN prod_modeled.products as products
         ON order_details.product_id =products.id
           AND order_details.source_country = products.source_country
           AND order_details.warehouse_id = products.warehouse_id
      WHERE orders.status != 'cancelled' 
          AND clients.CLASS in  ('Retention', 'On Boarding', 'Acquisition')
          and clients.STATUS = 'enable'
          and clients.source_country  = 'COLOMBIA'
),base2 AS (
    SELECT
      *,
      CASE WHEN DATEDIFF('day',order_day,(LAG(order_day, 1) OVER (PARTITION BY client_id,source_country ORDER BY order_day DESC) )) IS NULL THEN 0 
      WHEN DATEDIFF('day',order_day,(LAG(order_day, 1) OVER (PARTITION BY client_id,source_country ORDER BY order_day DESC) )) > 45 then 45 
      ELSE DATEDIFF('day',order_day,(LAG(order_day, 1) OVER (PARTITION BY client_id,source_country ORDER BY order_day DESC))) END AS dif_last_order
    FROM
      base
    WHERE
      orders_by_day = 1
)
,avg_orders_client_2 AS(
    SELECT HUNTER_ID,
      client_id,
      cliente_name,
      warehouse,
      base2.source_country,
      BASKET_SEGMENTATION,
      SEGMENTATION_MONETARY,
      AVG_TICKET,
      AUTONOMY_RATE,
      nanozone,
      datediff(day,max(order_day),current_date) as last_order,
      max(order_day) as orden_actual,
      max(order_id) as order_id_max ,
      count(distinct order_id) as order_days,
      (avg(case when dif_last_order = 0 then null else dif_last_order end)) :: INT AS avg_frec 
    FROM
      base2
    WHERE
      dif_last_order IS NOT NULL
    GROUP BY
      client_id,
      warehouse,
      base2.source_country,
      BASKET_SEGMENTATION,
      SEGMENTATION_MONETARY,
      AUTONOMY_RATE,
      AVG_TICKET,
      HUNTER_ID,
      nanozone,
      cliente_name)
      
,avg_orders_client AS(
    SELECT HUNTER_ID,
      client_id,
      cliente_name,
      warehouse,
      source_country,
      BASKET_SEGMENTATION,
      SEGMENTATION_MONETARY,
      AVG_TICKET,
      AUTONOMY_RATE,
      nanozone,
      last_order,
      case when categ_mix != 0 then 'Mix' else 'No_Mix' end as mix_check,
      orden_actual,
      order_id_max ,
      order_days,
      avg_frec 
    FROM
      avg_orders_client_2
      inner join (select order_details.order_id as order_idcat 
      , count(distinct case when products.GOAL_CATEGORY_NAME in ('Varillas','Cementos') then products.GOAL_CATEGORY_NAME else null end) as categ_comod
      , count(distinct case when products.GOAL_CATEGORY_NAME not in ('Varillas','Cementos') then  products.GOAL_CATEGORY_NAME else null end) as categ_mix
      FROM prod_modeled.order_details AS order_details
       left JOIN prod_modeled.orders AS orders
         ON order_details.order_id = orders.id
           AND order_details.client_id = orders.client_id 
           AND order_details.source_country = orders.source_country
      left JOIN prod_modeled.clients as clients 
         ON order_details.client_id = clients.id
           AND order_details.source_country = clients.source_country
      left JOIN prod_modeled.products as products
         ON order_details.product_id =products.id
           AND order_details.source_country = products.source_country
           AND order_details.warehouse_id = products.warehouse_id
           WHERE orders.status != 'cancelled' 
          AND clients.CLASS in  ('Retention', 'On Boarding', 'Acquisition')
          and clients.STATUS = 'enable'
          and clients.source_country  = 'COLOMBIA'
          group by 1
           ) as cate on cate.order_idcat = order_id_max