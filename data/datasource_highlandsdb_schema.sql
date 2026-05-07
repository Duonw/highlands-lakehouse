-- =========================================
-- HIGHLANDS LAKEHOUSE — SCHEMA V2
-- Chạy trên database: HighlandsDB
-- =========================================

-- Tạo database nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'HighlandsDB')
    CREATE DATABASE HighlandsDB;
GO

USE HighlandsDB;
GO

-- =========================================
-- MASTER DATA
-- =========================================

-- Thành phố — Airflow đọc bảng này để biết cào thời tiết ở đâu
CREATE TABLE Cities (
    city_id   INT IDENTITY(1,1) PRIMARY KEY,
    city_name VARCHAR(50)    NOT NULL,
    lat       DECIMAL(9,6)   NOT NULL, -- DECIMAL(p,s): p là precision, s là scale. nghĩa là tổng có p chữ số, trong đó có s chữ số sau dấu phẩy
    lon       DECIMAL(9,6)   NOT NULL,
    is_active BIT            NOT NULL DEFAULT 1
);

-- Cửa hàng — trỏ về Cities để biết thuộc thành phố nào
CREATE TABLE Stores (
    store_id   INT IDENTITY(1,1) PRIMARY KEY, --IDENTITY(s,i) --s: giá trị seed, i: giá trị increment => cột tăng tự động
    store_name VARCHAR(100)  NOT NULL,
    city_id    INT           NOT NULL,
    is_active  BIT           NOT NULL DEFAULT 1,
    FOREIGN KEY (city_id) REFERENCES Cities(city_id)
);

-- Khách hàng
CREATE TABLE Customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    full_name   NVARCHAR(100) NOT NULL,
    phone       VARCHAR(15),
    tier        VARCHAR(20)   NOT NULL DEFAULT 'Standard', -- Standard, Silver, Gold, Diamond
    created_at  DATETIME2     NOT NULL DEFAULT GETDATE()
);

-- Sản phẩm — phân loại rõ Hot/Cold để sau này phân tích demand theo thời tiết
CREATE TABLE Products (
    product_id   INT IDENTITY(1,1) PRIMARY KEY,
    product_name NVARCHAR(100) NOT NULL,
    category     VARCHAR(50)   NOT NULL, -- 'Hot Drink', 'Cold Drink', 'Food'
    price        DECIMAL(10,2) NOT NULL,
    is_active    BIT           NOT NULL DEFAULT 1
);

-- =========================================
-- TRANSACTIONAL DATA
-- =========================================

-- Hóa đơn — UUID để chống xung đột khi đồng bộ từ nhiều nguồn
-- updated_at là cột CDC dùng để Debezium/watermark phát hiện thay đổi
CREATE TABLE Orders (
    order_id     UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    store_id     INT              NOT NULL,
    customer_id  INT              NOT NULL,
    order_type   VARCHAR(20)      NOT NULL, -- 'Dine-in', 'Delivery'
    status       VARCHAR(20)      NOT NULL, -- 'Pending', 'Preparing', 'Completed', 'Cancelled'
    total_amount DECIMAL(10,2)    NOT NULL DEFAULT 0,
    created_at   DATETIME2        NOT NULL DEFAULT GETDATE(), -- lấy được chính xác hơn DATETIME
    updated_at   DATETIME2        NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (store_id)    REFERENCES Stores(store_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

-- Chi tiết hóa đơn
CREATE TABLE OrderDetails (
    detail_id   UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    order_id    UNIQUEIDENTIFIER NOT NULL,
    product_id  INT              NOT NULL,
    quantity    INT              NOT NULL,
    unit_price  DECIMAL(10,2)    NOT NULL,
    subtotal    DECIMAL(10,2)    NOT NULL,
    FOREIGN KEY (order_id)   REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

-- Trạng thái thời tiết mới nhất — Airflow UPDATE vào đây sau mỗi lần cào
-- Simulator đọc bảng này để biết thời tiết hiện tại khi tạo đơn hàng
CREATE TABLE CurrentWeatherState (
    city_id           INT          NOT NULL PRIMARY KEY,
    temperature       DECIMAL(5,2),
    weather_condition VARCHAR(50),  -- 'Clear', 'Rain', 'Clouds', 'Thunderstorm'
    wind_speed        DECIMAL(5,2),
    humidity          INT,
    last_updated      DATETIME2    NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (city_id) REFERENCES Cities(city_id)
);
