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

SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c:customers)-[co:customer_orders]->(o:orders WHERE o.ordered_when = '2024-01-02') COLUMNS (c.name, c.address));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)-[IS customer_orders]->(c IS customers) COLUMNS (c.name, o.ordered_when));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)<-[IS customer_orders]-(c IS customers) COLUMNS (c.name, o.ordered_when));

CREATE VIEW customers_us AS SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));

SELECT pg_get_viewdef('customers_us'::regclass);

-- test view/graph nesting

CREATE VIEW customers_view AS SELECT customer_id, 'redacted' || customer_id AS name_redacted, address FROM customers;
SELECT * FROM customers;
SELECT * FROm customers_view;

CREATE PROPERTY GRAPH myshop2
    VERTEX TABLES (
        products,
        customers_view KEY (customer_id) LABEL customers,
        orders
    )
    EDGE TABLES (
        order_items KEY (order_items_id)
            SOURCE KEY (order_id) REFERENCES orders (order_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no),
        customer_orders KEY (customer_orders_id)
            SOURCE KEY (customer_id) REFERENCES customers_view (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
    );

CREATE VIEW customers_us_redacted AS SELECT * FROM GRAPH_TABLE (myshop2 MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name_redacted AS customer_name_redacted));

SELECT * FROM customers_us_redacted;

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA graph_table_tests CASCADE;
