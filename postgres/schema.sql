CREATE TABLE IF NOT EXISTS USERS (
    user_id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT UNIQUE,
    user_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS PRODUCTS (
    product_id SERIAL PRIMARY KEY,
    product_name TEXT,
    price NUMERIC(10, 2),
    product_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ORDERS (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    total_price NUMERIC(10, 2),
    order_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'unique_product_name'
        AND table_name = 'products'
    ) THEN
        ALTER TABLE products
        ADD CONSTRAINT unique_product_name UNIQUE (product_name);
    END IF;
END;
