"""
dag_order_simulator.py
──────────────────────
DAG giả lập đơn hàng Highlands Coffee theo thời tiết hiện tại.

Disclaimer: Không sửa masterdata randomly lắm. Nếu mà sửa profile khách hàng/trạng thái
active của store, product thì lại phải handle historical data của khách, stores, products
để phản ánh đúng trạng thái đơn, nếu ko nó đang link trực tiếp thì đơn cũng bị thay đổi
nếu master data thay đổi => schema ở highlands bị thay đổi.

Schedule: mỗi 10 phút — đủ dày để có data CDC nhưng không spam DB.
Mỗi lần chạy tạo ra một batch đơn cho tất cả stores.

Tại sao là DAG thay vì script while-True?
  - Airflow quản lý retry, log, alert → không cần tự viết
  - Có thể trigger thủ công từ UI để test
  - Dừng/bật bằng toggle trên UI, không cần ssh vào container

Cấu trúc package highlands/:
  profiles.py   — WEATHER_PROFILES (business logic, giữ trong code để version control)
  repository.py — HighlandsRepository (tất cả DB operations)
  simulator.py  — generate_orders() (pure Python, không phụ thuộc DB/Airflow → dễ test)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timezone, timedelta

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import dag, task

from highlands.repository import HighlandsRepository
from highlands.simulator import generate_orders

_MSSQL_CONN_ID = "mssql_highlands"   # Connection riêng trỏ vào HighlandsDB

_VN_TZ = timezone(timedelta(hours=7))  # UTC+7
_OPEN_HOUR  = 7   # 07:00 VN — quán mở cửa
_CLOSE_HOUR = 22  # 22:00 VN — quán đóng cửa


def _get_repo() -> HighlandsRepository:
    engine = MsSqlHook(mssql_conn_id=_MSSQL_CONN_ID).get_sqlalchemy_engine()
    return HighlandsRepository(engine)


@dag(
    start_date=datetime(2026, 4, 26),
    schedule="10 * * * *",       # phút :10 mỗi giờ — sau weather DAGs (~:00) đã xong
    catchup=False,             # không chạy bù các khoảng thời gian đã qua
    tags=["highlands", "simulator", "orders"],
)
def order_simulator_dag():

    @task
    def load_master_data() -> dict:
        repo = _get_repo()
        master = repo.load_master_data()
        print(f"Loaded: {len(master['stores'])} stores, "
              f"{len(master['customer_ids'])} customers, "
              f"{len(master['products'])} products")
        return master

    @task
    def fetch_weather() -> dict:
        repo = _get_repo()
        weather_map = repo.fetch_weather()
        print(f"Weather snapshot: {weather_map}")
        return weather_map

    @task
    def generate_and_insert_orders(master: dict, weather_map: dict) -> int:
        """
        Nhận master + weather_map từ 2 task trước qua XCom —
        Airflow tự inject khi truyền return value của task vào đây.
        """
        # Kiểm tra giờ hoạt động: 07:00–22:00 VN (UTC+7)
        # Ngoài khung giờ này → bỏ qua, không tạo đơn !! chỉ có insert là ngừng, các task khác vẫn chạy nốt kệ nó
        now_vn = datetime.now(_VN_TZ)
        if not (_OPEN_HOUR <= now_vn.hour < _CLOSE_HOUR): 
            print(f"[{now_vn.strftime('%H:%M')} VN] Ngoài giờ hoạt động ({_OPEN_HOUR}:00–{_CLOSE_HOUR}:00). Bỏ qua tạo đơn.")
            return 0

        orders = generate_orders(
            stores=master["stores"],
            customer_ids=master["customer_ids"],
            products=master["products"],
            weather_map=weather_map,
        )

        repo = _get_repo()
        repo.insert_orders(orders)

        for order in orders:
            print(
                f"  [{order['_city']} | {order['_condition']} {order['_temp']:.1f}°C] "
                f"{order['order_type']:8s} | {len(order['details'])} items | "
                f"{order['total_amount']:,.0f} VND"
            )

        print(f"Inserted {len(orders)} orders total.")
        return len(orders)

    @task
    def advance_order_status() -> None:
        repo = _get_repo()
        n_prep, n_done, n_cancel = repo.advance_order_status()
        print(f"Advanced: {n_prep} → Preparing, {n_done} → Completed, {n_cancel} → Cancelled")

    # ─── LUỒNG CHẠY ───────────────────────────────────────────────────────────
    #
    #  advance_order_status ─┐
    #                        │  (cả 3 chạy song song, generate chờ tất cả xong)
    #  load_master_data    ──┼─► generate_and_insert_orders
    #                        │
    #  fetch_weather       ──┘
    #
    # advance + load_master + fetch_weather chạy SONG SONG (không phụ thuộc nhau)
    # generate_and_insert_orders phụ thuộc cả 3 → chạy sau cùng
    #
    # Tại sao advance phải chạy TRƯỚC generate (không phải sau)?
    #   - advance có logic: random 5% Cancelled
    #   - Nếu advance chạy SAU generate, các đơn vừa tạo (< 1 giây) sẽ bị
    #     cancel ngẫu nhiên ngay lập tức → sai hoàn toàn về mặt business
    #   - advance chạy TRƯỚC → chỉ advance các đơn CŨ từ run trước
    #   - Đơn mới tạo ra trong run này sẽ được advance ở run KẾ TIẾP (10 phút sau)
    #     → mô phỏng đúng vòng đời đơn thực tế

    advanced    = advance_order_status()
    master      = load_master_data()
    weather_map = fetch_weather()
    advanced >> generate_and_insert_orders(master, weather_map)


order_simulator_dag()
