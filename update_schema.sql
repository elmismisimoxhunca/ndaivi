-- Create the products table first
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manufacturer TEXT NOT NULL,
    product_name TEXT NOT NULL,
    description TEXT,
    download_link TEXT,
    manufacturer_website TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for products table
CREATE INDEX IF NOT EXISTS idx_products_manufacturer ON products(manufacturer);
CREATE INDEX IF NOT EXISTS idx_products_product_name ON products(product_name);

-- Create tables for each language
CREATE TABLE IF NOT EXISTS categories_en (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    category_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS categories_es (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    category_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_categories_en_product_id ON categories_en(product_id);
CREATE INDEX IF NOT EXISTS idx_categories_es_product_id ON categories_es(product_id);

-- Add triggers to update the updated_at timestamp
CREATE TRIGGER IF NOT EXISTS products_updated_at
AFTER UPDATE ON products
BEGIN
    UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS categories_en_updated_at
AFTER UPDATE ON categories_en
BEGIN
    UPDATE categories_en SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS categories_es_updated_at
AFTER UPDATE ON categories_es
BEGIN
    UPDATE categories_es SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Drop the old category_translations table if it exists
DROP TABLE IF EXISTS category_translations;
