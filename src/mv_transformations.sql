-- Enzyme Demo: Materialized View Definitions
-- E-commerce analytics MVs demonstrating different Enzyme microtechniques

-- MV 1: Regional Customer Analysis (SPUJ - Aggregation over Left Outer Join)
-- Powers the regional sales dashboard for sales managers
CREATE MATERIALIZED VIEW mv_regional_customers AS (
    SELECT
        c.region,
        c.membership_tier,
        c.customer_id,
        c.customer_name,
        COUNT(s.sale_id) AS order_count,
        COALESCE(SUM(s.quantity * s.unit_price), 0) AS total_spend
    FROM customers c
    LEFT JOIN sales s ON c.customer_id = s.customer_id
    GROUP BY c.region, c.membership_tier, c.customer_id, c.customer_name
);

-- MV 2: Customer Lifetime Value (Window Function)
-- Powers the loyalty program dashboard for marketing teams
CREATE MATERIALIZED VIEW mv_customer_ltv AS (
    SELECT
        customer_id,
        sale_date,
        quantity * unit_price * (1 - discount_percent/100) AS order_value,
        SUM(quantity * unit_price * (1 - discount_percent/100))
            OVER (PARTITION BY customer_id ORDER BY sale_date) AS cumulative_spend,
        COUNT(*) OVER (PARTITION BY customer_id ORDER BY sale_date) AS order_number
    FROM sales
    WHERE status != 'Returned'
);

-- MV 3: Category Performance (Composable Aggregation - agg over agg)
-- Powers the merchandising team's executive dashboard
CREATE MATERIALIZED VIEW mv_category_perf AS (
    SELECT
        category,
        SUM(daily_revenue) AS total_revenue,
        AVG(daily_revenue) AS avg_daily_revenue,
        MAX(daily_orders) AS peak_orders
    FROM (
        SELECT
            p.category,
            s.sale_date,
            SUM(s.quantity * s.unit_price) AS daily_revenue,
            COUNT(*) AS daily_orders
        FROM sales s
        INNER JOIN products p ON s.product_id = p.product_id
        GROUP BY p.category, s.sale_date
    )
    GROUP BY category
);

-- MV 4: Daily Sales Summary (Temporal Filter with Aggregation and JOIN)
-- Powers the operations team's daily sales report (last 90 days)
CREATE MATERIALIZED VIEW mv_daily_sales AS (
    SELECT
        s.sale_date,
        s.channel,
        p.category,
        COUNT(*) AS order_count,
        SUM(s.quantity) AS total_units,
        SUM(s.quantity * s.unit_price * (1 - s.discount_percent/100)) AS revenue
    FROM sales s
    INNER JOIN products p ON s.product_id = p.product_id
    WHERE s.status IN ('Completed', 'Shipped', 'Delivered')
        AND s.sale_date >= DATE_SUB(CURRENT_DATE(), 90)
    GROUP BY s.sale_date, s.channel, p.category
);
