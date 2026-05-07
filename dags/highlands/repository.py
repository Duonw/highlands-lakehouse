# repository.py — HighlandsRepository
# ─────────────────────────────────────────────────────────────────────────────
# Tất cả DB operations cho Highlands pipeline tập trung ở đây.
# Tương tự MetadataRepository ở weather/ — pattern nhất quán giữa 2 domain.
#
# Tại sao OOP thay vì function rời?
#   - Engine được tạo 1 lần, inject vào constructor → không tạo lại mỗi call
#   - Dễ test: mock engine, test từng method độc lập
#   - Dễ maintain: ai muốn sửa DB logic thì chỉ cần vào file này

# LOGIC VS DB: SELECT WEATHE, MASTER DATA + INSERT UPDATE STATUS CỦA ORDERS

from sqlalchemy import text
from sqlalchemy.engine import Engine


class HighlandsRepository:
    """
    Đọc/ghi tất cả data cho Highlands pipeline.

    Schema: HighlandsDB (cross-database query không cần, vì engine đã trỏ vào HighlandsDB)
        Cities             : city_id, city_name, lat, lon, is_active
        Stores             : store_id, city_id, is_active
        Customers          : customer_id
        Products           : product_id, category, price, is_active
        CurrentWeatherState: city_id, weather_condition, temperature
        Orders             : order_id, store_id, customer_id, order_type, status, total_amount
        OrderDetails       : detail_id, order_id, product_id, quantity, unit_price, subtotal
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine # truyền engine từ ngoài vào

    # ─── MASTER DATA ──────────────────────────────────────────────────────────

    def load_master_data(self) -> dict:
        """
        Load Stores, Customers, Products vào dict để truyền sang task tiếp theo.

        Trả về dict thay vì dataclass vì Airflow serialize dữ liệu giữa các task
        qua XCom (một dạng key-value store trong DB Airflow) — chỉ hỗ trợ
        JSON-serializable types: dict, list, str, int, float.
        """
        with self._engine.connect() as conn:
            stores = [
                {"store_id": r.store_id, "city_id": r.city_id}
                for r in conn.execute(
                    text("SELECT store_id, city_id FROM Stores WHERE is_active = 1")
                ).fetchall()
                # stores = [{"store_id": 1, "city_id": 1},
                #           {"store_id": 2, "city_id": 2}]
            ]

            customer_ids = [
                r.customer_id
                for r in conn.execute(text("SELECT customer_id FROM Customers")).fetchall()
                # customer_ids = [1, 2, 3]
            ]

            products = [
                {"product_id": r.product_id, "category": r.category, "price": float(r.price)}
                for r in conn.execute(
                    text("SELECT product_id, category, price FROM Products WHERE is_active = 1")
                ).fetchall()
                # products = [{"product_id": 1, "category": "Hot Drink", "price": 40000},
                #             {"product_id": 2, "category": "Cold Drink", "price": 55000}]
            ]

        return {"stores": stores, "customer_ids": customer_ids, "products": products}
        # XCom sau khi chạy load_master_data là đúng 1 cái dict key-value

    # ─── WEATHER ──────────────────────────────────────────────────────────────

    def fetch_weather(self) -> dict:
        """
        Đọc CurrentWeatherState để biết thời tiết mỗi thành phố.
        Trả về dict city_id (string) → weather info.

        Lưu ý: XCom serialize key thành string → dùng str(city_id) làm key.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT c.city_id, c.city_name, w.weather_condition, w.temperature
                    FROM   CurrentWeatherState w
                    JOIN   Cities c ON c.city_id = w.city_id
                """)
            ).fetchall()

        return {
            str(r.city_id): {
                "city_name":   r.city_name,
                "condition":   r.weather_condition or "Clouds",
                # Nếu r.weather_condition là None, "", 0, False → lấy "Clouds"
                # Nếu r.weather_condition có giá trị thật → lấy giá trị đó
                # tránh lấy NULL từ DB, generate đơn trung tính
                "temperature": float(r.temperature or 25.0),
            }
            for r in rows
        }
        # weather_map = {"1": {"city_name": "Hanoi",        "condition": "Clouds", "temperature": 25.0},
        #                "2": {"city_name": "Ho Chi Minh",  "condition": "Rain",   "temperature": 19.7}}

    # ─── ORDERS ───────────────────────────────────────────────────────────────

    def insert_orders(self, orders: list[dict]) -> None:
        """
        INSERT toàn bộ batch vào Orders + OrderDetails trong 1 transaction.

        engine.begin() = transaction tự động:
        - Thành công → tự COMMIT khi ra khỏi with
        - Có exception → tự ROLLBACK — không có đơn nửa vời trong DB

        Status bắt đầu là 'Pending' — giống đơn thật vừa đặt.
        advance_order_status() sẽ UPDATE sang Preparing → Completed ở lần chạy sau.
        Mỗi UPDATE thay đổi updated_at → Datastream CDC bắt từng sự kiện riêng biệt.
        """
        with self._engine.begin() as conn:
            for order in orders:
                conn.execute(
                    text("""
                        INSERT INTO Orders
                            (order_id, store_id, customer_id, order_type,
                             status, total_amount, created_at, updated_at)
                        VALUES
                            (:order_id, :store_id, :customer_id, :order_type,
                             'Pending', :total_amount, GETDATE(), GETDATE())
                    """),
                    # Lọc bỏ key bắt đầu bằng _ (dùng để log) và key "details" (list, không phải column)
                    # {k: v for k, v in order.items() if ...} = dict comprehension — duyệt từng cặp key-value
                    {k: v for k, v in order.items() if not k.startswith("_") and k != "details"},
                )
                for detail in order["details"]:
                    conn.execute(
                        text("""
                            INSERT INTO OrderDetails
                                (detail_id, order_id, product_id, quantity, unit_price, subtotal)
                            VALUES
                                (:detail_id, :order_id, :product_id, :quantity, :unit_price, :subtotal)
                        """),
                        # ** unpack toàn bộ detail rồi thêm order_id vào
                        # vì detail dict không có order_id (sinh ra ở vòng lặp ngoài)
                        {**detail, "order_id": order["order_id"]},
                    )

    def advance_order_status(self) -> tuple[int, int, int]:
        """
        Mô phỏng vòng đời đơn hàng thực tế — mỗi lần DAG chạy:

          Pending   (> 10 phút) or 60% may mắn → Preparing   [đang pha chế]
          Preparing (> 10 phút) or 60% may mắn → Completed   [giao xong]
          Pending   (random 5%) → Cancelled  [khách huỷ]

        Mỗi UPDATE thay đổi cột updated_at → Datastream CDC
        bắt từng sự kiện riêng biệt và đẩy lên GCS Bronze.
        Đây là lý do tại sao cần CDC thay vì chỉ snapshot cuối cùng.

        Returns:
            (n_preparing, n_completed, n_cancelled) — số dòng bị ảnh hưởng
        """
        with self._engine.begin() as conn:
            # Pending → Preparing
            #
            # 2 điều kiện OR (không phải AND):
            #   - 60% random: tạo staggering tự nhiên — mỗi run chỉ ~60% số đơn đủ điều kiện chuyển
            #   - safety net > 10 phút: đảm bảo đơn nào cũng thoát khỏi Pending trong tối đa 3 run
            #     (nếu xui liên tục 3 lần bị bỏ qua → run thứ 3 forced advance)
            #
            # Tại sao không cần threshold tối thiểu (> N phút)?
            #   advance chạy TRƯỚC generate trong cùng run → không có đơn nào từ run hiện tại
            #   khi advance chạy, chỉ có đơn từ các run trước → tất cả đều "đủ già"
            r1 = conn.execute(text("""
                UPDATE Orders
                SET    status     = 'Preparing',
                       updated_at = GETDATE()
                WHERE  status = 'Pending'
                  AND (
                      ABS(CHECKSUM(NEWID())) % 100 < 60
                      OR DATEDIFF(MINUTE, created_at, GETDATE()) > 120
                  )
            """))

            # Preparing → Completed
            #
            # Điều kiện tối thiểu updated_at > 5 phút: đơn phải ở trạng thái Preparing
            # ít nhất 5 phút → tránh chuyển Completed ngay trong cùng run với Pending→Preparing
            # (edge case: nếu advance chạy 2 lần nhanh)
            #
            # Safety net > 10 phút: đảm bảo không có Preparing stuck mãi mãi
            r2 = conn.execute(text("""
                UPDATE Orders
                SET    status     = 'Completed',
                       updated_at = GETDATE()
                WHERE  status = 'Preparing'
                  AND  DATEDIFF(MINUTE, updated_at, GETDATE()) > 60
                  AND (
                      ABS(CHECKSUM(NEWID())) % 100 < 60
                      OR DATEDIFF(MINUTE, updated_at, GETDATE()) > 120
                  )
            """))

            # Random cancel ~5% đơn Pending — khách đổi ý trước khi pha chế
            # ORDER BY NEWID() = shuffle ngẫu nhiên toàn bảng → TOP 5% = lấy 5% đầu sau shuffle
            r3 = conn.execute(text("""
                UPDATE Orders
                SET    status     = 'Cancelled',
                       updated_at = GETDATE()
                WHERE  status = 'Pending'
                  AND  order_id IN (
                      SELECT TOP 5 PERCENT order_id
                      FROM   Orders
                      WHERE  status = 'Pending'
                      ORDER BY NEWID()
                  )
            """))

        return r1.rowcount, r2.rowcount, r3.rowcount
