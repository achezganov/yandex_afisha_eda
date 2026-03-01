SELECT
    p.user_id, 
    p.device_type_canonical, 
    p.order_id,
    p.created_dt_msk AS order_dt, 
    p.created_ts_msk AS order_ts, 
    p.currency_code, 
    p.revenue, 
    p.tickets_count,
    p.days_since_prev, 
    e.event_id, 
    e.event_type_main, 
    p.service_name,
    r.region_name, 
    c.city_name
FROM (
    SELECT
        *,
        (created_dt_msk::date
         - LAG(created_dt_msk::date) OVER (
             PARTITION BY user_id
             ORDER BY created_ts_msk, order_id
         )
        )::int AS days_since_prev
    FROM afisha.purchases
    WHERE device_type_canonical IN ('mobile', 'desktop')
) AS p
LEFT JOIN afisha.events AS e 
    ON p.event_id = e.event_id
LEFT JOIN afisha.city AS c 
    ON e.city_id = c.city_id
LEFT JOIN afisha.regions AS r 
    ON c.region_id = r.region_id
ORDER BY p.user_id, p.created_ts_msk, p.order_id;
