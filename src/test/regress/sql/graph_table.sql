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

CREATE TABLE wishlists (
    wishlist_id integer PRIMARY KEY,
    wishlist_name varchar
);

CREATE TABLE wishlist_items (
    wishlist_items_id integer PRIMARY KEY,
    wishlist_id integer REFERENCES wishlists (wishlist_id),
    product_no integer REFERENCES products (product_no)
);

CREATE TABLE customer_wishlists (
    customer_wishlist_id integer PRIMARY KEY,
    customer_id integer REFERENCES customers (customer_id),
    wishlist_id integer REFERENCES wishlists (wishlist_id)
);

CREATE PROPERTY GRAPH myshop
    VERTEX TABLES (
        products
    		DEFAULT LABEL PROPERTIES (product_no, name AS product_name),
        customers
		    DEFAULT LABEL PROPERTIES (customer_id, name AS customer_name, address),
        orders
           DEFAULT LABEL
            LABEL link PROPERTIES (order_id as node_id, 'order'::varchar(10) as link_type),
        wishlists
           DEFAULT LABEL
            LABEL link PROPERTIES (wishlist_id as node_id, 'wishlist'::char(10) as link_type)
    )
    EDGE TABLES (
        order_items KEY (order_items_id)
            SOURCE KEY (order_id) REFERENCES orders (order_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no)
            DEFAULT LABEL
            LABEL link_items PROPERTIES (order_id as link_id, product_no),
        wishlist_items KEY (wishlist_items_id)
            SOURCE KEY (wishlist_id) REFERENCES wishlists (wishlist_id)
            DESTINATION KEY (product_no) REFERENCES products (product_no)
            DEFAULT LABEL
            LABEL link_items PROPERTIES (wishlist_id as link_id, product_no),
        customer_orders KEY (customer_orders_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
            DEFAULT LABEL
            LABEL cust_link PROPERTIES (customer_id, order_id as link_id),
        customer_wishlists KEY (customer_wishlist_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (wishlist_id) REFERENCES wishlists (wishlist_id)
            DEFAULT LABEL
            LABEL cust_link PROPERTIES (customer_id, wishlist_id as link_id)
    );

SELECT customer_name FROM GRAPH_TABLE (xxx MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (pg_class MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (cx.name AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.namex AS customer_name));  -- error
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders] COLUMNS (c.customer_name AS customer_name));  -- error

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
INSERT INTO wishlists VALUES
    (1, 'wishlist1'),
    (2, 'wishlist2'),
    (3, 'wishlist3');
INSERT INTO order_items (order_items_id, order_id, product_no, quantity) VALUES
    (1, 1, 1, 5),
    (2, 1, 2, 10),
    (3, 2, 1, 7);
INSERT INTO customer_orders (customer_orders_id, customer_id, order_id) VALUES
    (1, 1, 1),
    (2, 2, 2);
INSERT INTO customer_wishlists (customer_wishlist_id, customer_id, wishlist_id) VALUES
    (1, 2, 3),
    (2, 3, 1),
    (3, 3, 2);
INSERT INTO wishlist_items (wishlist_items_id, wishlist_id, product_no) VALUES
    (1, 1, 2),
    (2, 1, 3),
    (3, 2, 1),
    (4, 3, 1);

-- single element path pattern
SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers) COLUMNS (c.customer_name));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.customer_name AS customer_name));
-- graph element specification without label or underlying table
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[]->(o IS orders) COLUMNS (c.customer_name AS customer_name));
SELECT * FROM GRAPH_TABLE (myshop MATCH (c:customers)-[co:customer_orders]->(o:orders WHERE o.ordered_when = '2024-01-02') COLUMNS (c.customer_name, c.address));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)-[IS customer_orders]->(c IS customers) COLUMNS (c.customer_name, o.ordered_when));
SELECT * FROM GRAPH_TABLE (myshop MATCH (o IS orders)<-[IS customer_orders]-(c IS customers) COLUMNS (c.customer_name, o.ordered_when));
-- TODO: spaces between graph elements and connectors don't work () -> [] -> ()
-- does not work,
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS cust_link]->(l IS link)-[ IS link_items]->(p IS products) COLUMNS (c.customer_name, p.product_name, l.link_type)) ORDER BY customer_name, product_name, link_type;
-- same as above but disjuncts labels but queries a property not associated with those labels
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS customer_orders | customer_wishlists ]->(l IS orders | wishlists)-[ IS link_items]->(p IS products) COLUMNS (c.customer_name, p.product_name, l.link_type)) ORDER BY customer_name, product_name, link_type;
-- correct query with disjuncted labels
SELECT * FROM GRAPH_TABLE (myshop MATCH (c IS customers)-[IS customer_orders | customer_wishlists ]->(l IS orders | wishlists)-[ IS link_items]->(p IS products) COLUMNS (c.customer_name, p.product_name)) ORDER BY customer_name, product_name;
-- vertex to vertex abbreviation
SELECT customer_name, ordered_when FROM GRAPH_TABLE (myshop MATCH (c IS customers)->(o IS orders) COLUMNS (c.customer_name AS customer_name, o.ordered_when)) ORDER BY customer_name;
-- empty element path pattern, counts number of edges in the graph
SELECT count(*) FROM GRAPH_TABLE (myshop MATCH ()-[]->() COLUMNS (1 as one));
SELECT count(*) FROM GRAPH_TABLE (myshop MATCH ()->() COLUMNS (1 as one));

create table v1 (id int primary key,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

create table v2 (id1 int,
					id2 int,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

create table v3 (id int primary key,
					vname varchar(10),
					vprop1 int,
					vprop2 int);

-- edge connecting v1 and v2
create table e1_2 (id_1 int,
					id_2_1 int,
					id_2_2 int,
					ename varchar(10),
					eprop1 int);

-- edge connecting v1 and v3
create table e1_3 (id_1 int,
					id_3 int,
					ename varchar(10),
					eprop1 int,
					primary key (id_1, id_3));

create table e2_3 (id_2_1 int,
                    id_2_2 int,
                    id_3 int,
                    ename varchar(10),
                    eprop1 int);

create property graph g1
vertex tables (
	v1
        label vl1 properties (vname, vprop1),
	v2 key (id1, id2) -- labels sharing an element
		label vl2 properties (vname, vprop2, 'vl2_prop'::varchar(10) as lprop1)
        label vl3 properties (vname, vprop1, 'vl2_prop'::varchar(10) as lprop1),
	v3
		label vl3 properties (vname, vprop1, 'vl3_prop'::varchar(10) as lprop1)
)
edge tables ( -- edges with differing number of destination keys
	e1_2 key (id_1, id_2_1, id_2_2)
		source key (id_1) references v1 (id)
		destination key (id_2_1, id_2_2) references v2 (id1, id2)
		label el1 properties (eprop1, ename),
	e1_3
		-- the number of columns in keys doesn't matter
		source key (id_1) references v1 (id)
		destination key (id_3) references v3 (id)
		-- order of property names doesn't matter
		label el1 properties (ename, eprop1),
    e2_3 key (id_2_1, id_2_2, id_3)
        source key (id_2_1, id_2_2) references v2 (id1, id2)
        destination key (id_3) references v3 (id)
        -- new property lprop2 not shared by other label
        -- does not share eprop1 from other label
        label el2 properties (ename, eprop1 * 10 as lprop2)
);

insert into v1 values (1, 'v11', 10, 100),
                      (2, 'v12', 20, 200),
                      (3, 'v13', 30, 300);

insert into v2 values (1000, 1, 'v21', 1010, 1100),
                      (1000, 2, 'v22', 1020, 1200),
                      (1000, 3, 'v23', 1030, 1300);

insert into v3 values (2001, 'v31', 2010, 2100),
                      (2002, 'v32', 2020, 2200),
                      (2003, 'v33', 2030, 2300);

insert into e1_2 values (1, 1000, 2, 'e121', 10001),
                        (2, 1000, 1, 'e122', 10002);

insert into e1_3 values (1, 2003, 'e131', 10003),
                        (1, 2001, 'e132', 10004);
insert into e2_3 values (1000, 2, 2002, 'e231', 10005);

select src, conn, dest, lprop1, vprop2, vprop1 from graph_table (g1 match (a is vl1)-[b is el1]->(c is vl2 | vl3) columns (a.vname as src, b.ename as conn, c.vname as dest, c.lprop1, c.vprop2, c.vprop1));

-- vl1 is not associated with property vprop2
select src, src_vprop2, conn, dest from graph_table (g1 match (a is vl1)-[b is el1]->(c is vl2 | vl3) columns (a.vname as src, a.vprop2 as src_vprop2, b.ename as conn, c.vname as dest));

-- ename is not a property of any label associated with a vertex
select * from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svname, src.ename as sename));
-- vname is not a property of any label associated with an edge
select * from graph_table (g1 match (src)-[conn]->(dest) columns (conn.vname as cvname, conn.ename as cename));
-- label not associated with any element of given kind
select * from graph_table (g1 match (src is el1)-[conn]->(dest) columns (conn.vname as cvname, conn.ename as cename));

-- select all the properties across all the labels associated with a given type
-- of graph element
select * from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svname, conn.ename as cename, dest.vname as dvname, src.vprop1 as svp1, src.vprop2 as svp2, src.lprop1 as slp1, dest.vprop1 as dvp1, dest.vprop2 as dvp2, dest.lprop1 as dlp1, conn.eprop1 as cep1, conn.lprop2 as clp2));
-- three label disjunction
select * from graph_table (g1 match (src IS vl1 | vl2 | vl3)-[conn]->(dest) columns (src.vname as svname, conn.ename as cename, dest.vname as dvname));
-- graph'ical query: find a vertex which is not connected to any other vertex as a source or a destination.
with all_connected_vertices as (select svn, dvn from graph_table (g1 match (src)-[conn]->(dest) columns (src.vname as svn, dest.vname as dvn))),
all_vertices as (select vn from graph_table (g1 match (vertex) columns (vertex.vname as vn)))
select vn from all_vertices except (select svn from all_connected_vertices union select dvn from all_connected_vertices);
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

CREATE VIEW customers_us AS SELECT customer_name FROM GRAPH_TABLE (myshop MATCH (c IS customers WHERE c.address = 'US')-[IS customer_orders]->(o IS orders) COLUMNS (c.customer_name AS customer_name));

SELECT pg_get_viewdef('customers_us'::regclass);

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA graph_table_tests CASCADE;

-- TODO: Create two property graphs with some same named tables and properties
-- but differing definitions to test that properties and labels corresponding
-- to a property graph mentioned in the query are picked up.