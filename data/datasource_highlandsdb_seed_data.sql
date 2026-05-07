-- =========================================
-- HIGHLANDS LAKEHOUSE — SEED DATA
-- Chạy SAU schema_v2.sql
-- =========================================

USE HighlandsDB;
GO

-- Thành phố — 2 thành phố, khí hậu khác nhau để dễ so sánh insight
INSERT INTO Cities (city_name, lat, lon) VALUES
('Hanoi',         21.027763, 105.834160),
('Ho Chi Minh',   10.823099, 106.629662);

-- Cửa hàng — mỗi thành phố 1 store để demo đơn giản
INSERT INTO Stores (store_name, city_id) VALUES
('Highlands Nhà Hát Lớn', 1),   -- Hanoi
('Highlands Landmark 81',  2);  -- Ho Chi Minh

-- Khách hàng — đủ các tier để demo segment analysis
INSERT INTO Customers (full_name, phone, tier) VALUES
('Nguyen Van An',  '0901234567', 'Gold'),
('Tran Thi Bich',  '0912345678', 'Standard'),
('Le Van Cuong',   '0923456789', 'Diamond'),
('Pham Thi Dung',  '0934567890', 'Silver'),
('Hoang Van Em',   '0945678901', 'Standard');

-- Sản phẩm — phân loại Hot/Cold rõ ràng vì đây là core của insight thời tiết
INSERT INTO Products (product_name, category, price) VALUES
('Tra Sen Vang',        'Cold Drink', 45000),
('Phindi Hanh Nhan',    'Cold Drink', 50000),
('Tra Thach Dao',       'Cold Drink', 45000),
('Sinh To Bo',          'Cold Drink', 55000),
('Espresso',            'Hot Drink',  35000),
('Bac Xiu',             'Hot Drink',  40000),
('Tra Lay',             'Hot Drink',  35000),
('Banh Mi Thit',        'Food',       45000),
('Cookie Socola',       'Food',       25000);

-- Khởi tạo CurrentWeatherState — cần có dòng trước khi Airflow UPDATE
-- Giá trị mặc định, Airflow sẽ ghi đè sau lần chạy đầu tiên
INSERT INTO CurrentWeatherState (city_id, temperature, weather_condition, wind_speed, humidity)
VALUES
(1, 25.0, 'Clear', 3.0, 70),
(2, 30.0, 'Clear', 2.5, 80);
