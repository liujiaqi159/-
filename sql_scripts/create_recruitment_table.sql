CREATE TABLE IF NOT EXISTS recruitment_data ( -- 或者你实际的表名
    id INT AUTO_INCREMENT PRIMARY KEY,
    excel_id INT UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock INT,
    remarks TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;