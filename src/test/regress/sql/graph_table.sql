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

SELECT customer_name FROM GRAPH_TABLE (xxx MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (pg_class MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (cx.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.namex AS customer_name));  -- error

INSERT INTO products VALUES
    (1, 'product1', 10),
    (2, 'product2', 20),
    (3, 'product3', 30);
INSERT INTO customers VALUES
    (1, 'customer1', 'US'),
    (2, 'customer2', 'CA'),
    (3, 'customer3', 'GL');
INSERT INTO orders VALUES
    (1, '2024-01-01'),
    (2, '2024-01-02'),
    (3, '2024-01-03');
INSERT INTO order_items (order_items_id, order_id, product_no, quantity) VALUES
    (1, 1, 1, 5),
    (2, 1, 2, 10),
    (3, 2, 1, 7);
INSERT INTO customer_orders (customer_orders_id, customer_id, order_id) VALUES
    (1, 1, 1),
    (2, 2, 2);

SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- TODO

-- TODO: should approximately match this query:
SET debug_print_parse = on;
SELECT customer_name FROM (
    SELECT c.name AS customer_name
    FROM customers c, customer_orders _co, orders o
    WHERE _co.customer_id = c.customer_id
      AND _co.order_id = o.order_id
      AND c.address = 'US'
) myshop;
RESET debug_print_parse;

CREATE VIEW customers_us AS SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));

SELECT pg_get_viewdef('customers_us'::regclass);

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA graph_table_tests CASCADE;
