CREATE SCHEMA graph_table_tests;
GRANT USAGE ON SCHEMA graph_table_tests TO PUBLIC;
SET search_path = graph_table_tests;

CREATE TABLE products (
    product_no integer PRIMARY KEY,
    name varchar,
    price numeric
);

CREATE TABLE customers (
    customer_id integer PRIMARY KEY,
    name varchar,
    address varchar
);

CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    ordered_when date
);

CREATE TABLE order_items (
    order_items_id integer PRIMARY KEY,
    order_id integer REFERENCES orders (order_id),
    product_no integer REFERENCES products (product_no),
    quantity integer
);

CREATE TABLE customer_orders (
    customer_orders_id integer PRIMARY KEY,
    customer_id integer REFERENCES customers (customer_id),
    order_id integer REFERENCES orders (order_id)
);

CREATE PROPERTY GRAPH myshop
    VERTEX TABLES (
        products,
        customers,
        orders
    )
    EDGE TABLES (
        order_items KEY (order_items_id)
            SOURCE KEY (order_id) REFERENCES orders (order_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no),
        customer_orders KEY (customer_orders_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
    );

SELECT customer_name FROM GRAPH_TABLE (xxx MATCH (c IS customers WHERE address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (pg_class MATCH (c IS customers WHERE address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- TODO

CREATE VIEW customers_us AS SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));

SELECT pg_get_viewdef('customers_us'::regclass);

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA graph_table_tests CASCADE;
