-- Create the schema that we'll use to populate data and watch the effect in the binlog
CREATE SCHEMA inventory;
SET search_path TO inventory;

-- enable PostGis
CREATE EXTENSION postgis;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id SERIAL NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight FLOAT
);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14),
       (default,'car battery','12V car battery',8.1),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),
       (default,'hammer','12oz carpenter''s hammer',0.75),
       (default,'hammer','14oz carpenter''s hammer',0.875),
       (default,'hammer','16oz carpenter''s hammer',1.0),
       (default,'rocks','box of assorted rocks',5.3),
       (default,'jacket','water resistent black wind breaker',0.1),
       (default,'spare tire','24 inch spare tire',22.2);

-- Create and populate the products on hand using multiple inserts
CREATE TABLE products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);
ALTER TABLE products_on_hand REPLICA IDENTITY FULL;

INSERT INTO products_on_hand VALUES (101,3);
INSERT INTO products_on_hand VALUES (102,8);
INSERT INTO products_on_hand VALUES (103,18);
INSERT INTO products_on_hand VALUES (104,4);
INSERT INTO products_on_hand VALUES (105,5);
INSERT INTO products_on_hand VALUES (106,0);
INSERT INTO products_on_hand VALUES (107,44);
INSERT INTO products_on_hand VALUES (108,2);
INSERT INTO products_on_hand VALUES (109,5);

-- Create some customers ...
CREATE TABLE customers (
  id SERIAL NOT NULL PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE
);
ALTER SEQUENCE customers_id_seq RESTART WITH 1001;
ALTER TABLE customers REPLICA IDENTITY FULL;

INSERT INTO customers
VALUES (default,'Sally','Thomas','sally.thomas@acme.com'),
       (default,'George','Bailey','gbailey@foobar.com'),
       (default,'Edward','Walker','ed@walker.com'),
       (default,'Anne','Kretchmar','annek@noanswer.org');

-- Create some very simple orders
CREATE TABLE orders (
  id SERIAL NOT NULL PRIMARY KEY,
  order_date DATE NOT NULL,
  purchaser INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  FOREIGN KEY (purchaser) REFERENCES customers(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);
ALTER SEQUENCE orders_id_seq RESTART WITH 10001;
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders
VALUES (default, '2016-01-16', 1001, 1, 102),
       (default, '2016-01-17', 1002, 2, 105),
       (default, '2016-02-19', 1002, 2, 106),
       (default, '2016-02-21', 1003, 1, 107);

-- Create table with Spatial/Geometry type
CREATE TABLE geom (
        id SERIAL NOT NULL PRIMARY KEY,
        g GEOMETRY NOT NULL,
        h GEOMETRY);

INSERT INTO geom
VALUES(default, ST_GeomFromText('POINT(1 1)')),
      (default, ST_GeomFromText('LINESTRING(2 1, 6 6)')),
      (default, ST_GeomFromText('POLYGON((0 5, 2 5, 2 7, 0 7, 0 5))'));

CREATE TABLE IF NOT EXISTS inventory.test_data_types
    (
        c_id INTEGER             ,
        c_json JSON              ,
        c_jsonb JSONB            ,
        c_date DATE              ,
        c_timestamp0 TIMESTAMP(0),
        c_timestamp1 TIMESTAMP(1),
        c_timestamp2 TIMESTAMP(2),
        c_timestamp3 TIMESTAMP(3),
        c_timestamp4 TIMESTAMP(4),
        c_timestamp5 TIMESTAMP(5),
        c_timestamp6 TIMESTAMP(6),
        c_timestamptz TIMESTAMPTZ,
        PRIMARY KEY(c_id)
    ) ;
-- ALTER TABLE inventory.test_data_types REPLICA IDENTITY FULL;
INSERT INTO
   inventory.test_data_types 
VALUES
   (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ), 
   (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01' ), 
   (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01' )
;

CREATE TABLE IF NOT EXISTS inventory.test_table
    (
        c_id INTEGER           ,
        c_id2 VARCHAR(64)      ,
        c_data TEXT            ,
        c_text TEXT            ,
        c_varchar VARCHAR(1666),
        PRIMARY KEY(c_id, c_id2)
    ) ;
-- ALTER TABLE inventory.test_table REPLICA IDENTITY FULL;

