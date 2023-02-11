SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

SET SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

CREATE SCHEMA olist;
USE olist;

CREATE TABLE order_customer_dataset (
  customer_id VARCHAR(255),
  customer_unique_id VARCHAR(255),
  customer_zip_code_prefix INT,
  customer_city VARCHAR(255),
  customer_state VARCHAR(255),
  PRIMARY KEY (customer_id, customer_unique_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE order_dataset (
  order_id VARCHAR(255) NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  order_status VARCHAR(255),
  order_purchase_timestamp DATETIME,
  order_approved_at DATETIME,
  order_delivered_carrier_date DATETIME,
  order_delivered_customer_date DATETIME,
  order_estimated_delivery_date DATETIME,
  PRIMARY KEY (order_id,customer_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE order_items_dataset (
  order_id VARCHAR(255),
  order_item_id INT NOT NULL,
  product_id VARCHAR(255),
  seller_id VARCHAR(255),
  shipping_limit_date DATETIME,
  price FLOAT,
  freight_value FLOAT,
  PRIMARY KEY(order_id, order_item_id, product_id, seller_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE payments_dataset (
  Id INT NOT NULL auto_increment PRIMARY KEY,
  order_id VARCHAR(255) NOT NULL,
  payment_sequential INT NOT NULL,
  payment_type VARCHAR(255),
  payment_installments INT NOT NULL,
  payment_value FLOAT,
  KEY(order_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE products_dataset (
  product_id VARCHAR(255),
  product_category_name VARCHAR(255),
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT,
  PRIMARY KEY (product_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE sellers_dataset (
  seller_id VARCHAR(255),
  seller_zip_code_prefix INT NOT NULL,
  seller_city VARCHAR(255),
  seller_state VARCHAR(255),
  PRIMARY KEY(seller_id)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE geolocation_dataset (
  Id INT NOT NULL auto_increment PRIMARY KEY,
  geolocation_zip_code_prefix INT NOT NULL,
  geolocation_lat DECIMAL(16,13),
  geolocation_lng DECIMAL(16,13),
  geolocation_city VARCHAR(255),
  geolocation_state VARCHAR(255),
  KEY (geolocation_zip_code_prefix)
)ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE order_reviews_dataset (
  review_id VARCHAR(255)NOT NULL,
  order_id VARCHAR(255)NOT NULL,
  review_score INT,
  review_comment_title VARCHAR(255),
  review_comment_message VARCHAR(255),
  review_creation_date DATETIME,
  review_answer_timestamp DATETIME,
  PRIMARY KEY (review_id,order_id)
  )ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;