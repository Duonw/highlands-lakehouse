# simulator.py — Order generation logic
# ─────────────────────────────────────────────────────────────────────────────
# Pure Python — không import DB, không import Airflow.
# Tách ra file riêng để:
#   - Test được mà không cần DB (unit test thuần)
#   - Logic rõ ràng, không bị lẫn với SQL/Airflow boilerplate

# LOGIC GENERATE ORDERS IN ONE BATCH ONLY

import random
import uuid

from highlands.profiles import get_profile

# Số đơn tạo ra mỗi lần chạy — mỗi store
# Chạy 1 lần/giờ → cần volume đủ dày để Gold layer có meaningful aggregation
ORDERS_PER_STORE = (40, 45)


def _pick_product(products: list[dict], category_weights: dict[str, int]) -> dict:
    """
    Chọn 1 sản phẩm có trọng số theo category.

    random.choices(population, weights) — không phải xác suất tuyệt đối,
    mà là tỷ lệ tương đối. weights=[4,1,2] → Hot Drink được chọn 4/7 ≈ 57%.

    dict.get(p["category"], 1):
      Nếu category có trong dict → trả về weight đó
      Nếu KHÔNG có → trả về 1 (default) thay vì crash
      Phòng trường hợp thêm category mới vào DB mà chưa update profiles
      → sản phẩm đó vẫn có cơ hội được chọn thay vì weight=0 never đc chọn
    """
    weights = [category_weights.get(p["category"], 1) for p in products]
    # random.choices luôn trả về LIST dù k=1 → [0] để bóc ra dict thật sự
    return random.choices(products, weights=weights, k=1)[0]


def generate_orders(stores: list[dict], customer_ids: list[int],
                    products: list[dict], weather_map: dict) -> list[dict]:
    """
    Tạo batch đơn hàng dựa vào thời tiết của từng thành phố.

    Với từng store trong active stores:
        1. Quyết định store này có bao nhiêu orders (random trong ORDERS_PER_STORE)
        2. Với từng order:
            - Quyết định Delivery hay Dine-in theo delivery_prob
            - Quyết định số items (1-3)
            - Với từng item: chọn product theo category_weights, random quantity
        → append vào list orders của batch đang làm việc

    Args:
        stores      : list active stores với city_id
        customer_ids: list tất cả customer_id để random chọn
        products    : list active products với category và price
        weather_map : dict str(city_id) → {"condition": ..., "temperature": ...}

    Returns:
        list[dict] — mỗi dict là 1 order, key bắt đầu _ chỉ dùng để log, không INSERT vào DB
    """
    orders = []

    for store in stores:
        # lấy weather từ weather_map ứng với city_id của store đang xét
        weather = weather_map.get(str(store["city_id"]))
        if not weather:
            # Không có weather data → bỏ qua store này, không crash cả batch
            print(f"[WARN] Không có weather cho city_id={store['city_id']}, bỏ qua store {store['store_id']}")
            continue

        # lấy weather profile ứng với condition → quyết định hành vi mua hàng
        profile  = get_profile(weather["condition"])
        n_orders = random.randint(*ORDERS_PER_STORE)

        for _ in range(n_orders):
            # random.random() sinh float trong [0.0, 1.0)
            # So sánh với delivery_prob → 80% thời gian < 0.8 → Delivery
            order_type  = "Delivery" if random.random() < profile["delivery_prob"] else "Dine-in"
            customer_id = random.choice(customer_ids)

            items = []
            total = 0.0
            for _ in range(random.randint(1, 4)):  # mỗi đơn 1-3 sản phẩm
                product  = _pick_product(products, profile["category_weights"])
                qty      = 1
                subtotal = product["price"] * qty
                total   += subtotal
                items.append({
                    "detail_id":  str(uuid.uuid4()),
                    "product_id": product["product_id"],
                    "quantity":   qty,
                    "unit_price": product["price"],
                    "subtotal":   round(subtotal, 2),
                })

            orders.append({
                "order_id":     str(uuid.uuid4()),
                "store_id":     store["store_id"],
                "customer_id":  customer_id,
                "order_type":   order_type,
                "total_amount": round(total, 2),
                "details":      items,
                # key bắt đầu _ = chỉ để log trong DAG, không INSERT vào DB
                "_city":        weather["city_name"],
                "_condition":   weather["condition"],
                "_temp":        weather["temperature"],
            })

    return orders
