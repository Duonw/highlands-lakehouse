# profiles.py
# ─────────────────────────────────────────────────────────────────────────────
# Business logic: thời tiết → hành vi mua hàng của khách Highlands Coffee.
#
# Tại sao giữ ở đây thay vì DB?
#   - Đây là business logic, không phải data — nên qua code review, không ai
#     tự ý đổi trên DB. Version controlled trong git → biết ai đổi, khi nào.
#   - Đưa vào DB chỉ có lợi nếu business analyst cần tự tune mà không muốn
#     deploy lại code. Ở scale này chưa cần.
#
# delivery_prob   : xác suất đơn là Delivery (còn lại là Dine-in)
# category_weights: trọng số chọn category sản phẩm
#   Nếu mưa thì khách mua delivery tăng + hot drink tăng.
#   Ngược lại trời đẹp thì dine-in tăng, cold drink tăng.
#   random.choices dùng weights → số càng cao càng dễ được chọn
#   weights=[4,1,2] → tổng=7 → Hot Drink 4/7≈57%, Cold 1/7≈14%, Food 2/7≈29%

WEATHER_PROFILES: dict[str, dict] = {
    # ── Mưa ───────────────────────────────────────────────────────────────────
    "Rain":              {"delivery_prob": 0.80, "category_weights": {"Hot Drink": 7, "Cold Drink": 1, "Food": 2}},
    "Thunderstorm":      {"delivery_prob": 0.90, "category_weights": {"Hot Drink": 9, "Cold Drink": 1, "Food": 3}},
    "Drizzle":           {"delivery_prob": 0.65, "category_weights": {"Hot Drink": 8, "Cold Drink": 1, "Food": 2}},
    "Snow":              {"delivery_prob": 0.70, "category_weights": {"Hot Drink": 7, "Cold Drink": 1, "Food": 2}},
    # ── Trời tốt ──────────────────────────────────────────────────────────────
    "Clear":             {"delivery_prob": 0.25, "category_weights": {"Hot Drink": 1, "Cold Drink": 6, "Food": 1}},
    "Clouds":            {"delivery_prob": 0.45, "category_weights": {"Hot Drink": 1, "Cold Drink": 5, "Food": 1}},
    "Partly cloudy":     {"delivery_prob": 0.35, "category_weights": {"Hot Drink": 1, "Cold Drink": 5, "Food": 1}},
    # ── Sương mù / Tầm nhìn kém ───────────────────────────────────────────────
    "Fog":               {"delivery_prob": 0.55, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Mist":              {"delivery_prob": 0.50, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Haze":              {"delivery_prob": 0.50, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Smoke":             {"delivery_prob": 0.55, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Dust":              {"delivery_prob": 0.60, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Sand":              {"delivery_prob": 0.60, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Ash":               {"delivery_prob": 0.65, "category_weights": {"Hot Drink": 2, "Cold Drink": 1, "Food": 1}},
    "Squall":            {"delivery_prob": 0.80, "category_weights": {"Hot Drink": 4, "Cold Drink": 1, "Food": 2}},
    "Tornado":           {"delivery_prob": 0.95, "category_weights": {"Hot Drink": 3, "Cold Drink": 1, "Food": 1}},
    # ── OpenMeteo WMO codes (trả về dạng string mô tả) ────────────────────────
    "Overcast":          {"delivery_prob": 0.45, "category_weights": {"Hot Drink": 2, "Cold Drink": 2, "Food": 1}},
    "Freezing rain":     {"delivery_prob": 0.85, "category_weights": {"Hot Drink": 5, "Cold Drink": 1, "Food": 2}},
    "Heavy rain":        {"delivery_prob": 0.85, "category_weights": {"Hot Drink": 4, "Cold Drink": 1, "Food": 2}},
    "Light rain":        {"delivery_prob": 0.60, "category_weights": {"Hot Drink": 3, "Cold Drink": 1, "Food": 2}},
    "Moderate rain":     {"delivery_prob": 0.70, "category_weights": {"Hot Drink": 4, "Cold Drink": 1, "Food": 2}},
    "Heavy snow":        {"delivery_prob": 0.85, "category_weights": {"Hot Drink": 5, "Cold Drink": 1, "Food": 1}},
    "Light snow":        {"delivery_prob": 0.65, "category_weights": {"Hot Drink": 4, "Cold Drink": 1, "Food": 2}},
}

# Mặc định nếu condition không có trong bảng trên
# Neutral: không bias Hot hay Cold — tránh làm sai số liệu khi API trả về condition lạ
DEFAULT_PROFILE: dict = {
    "delivery_prob":    0.40,
    "category_weights": {"Hot Drink": 1, "Cold Drink": 1, "Food": 1},
}

# Lookup index không phân biệt hoa thường để tránh miss khi API trả về "rain" / "RAIN"
_PROFILES_LOWER: dict[str, dict] = {k.lower(): v for k, v in WEATHER_PROFILES.items()}


def get_profile(condition: str) -> dict:
    """Trả về weather profile theo condition. Case-insensitive. Trả về DEFAULT nếu không tìm thấy."""
    profile = _PROFILES_LOWER.get(condition.lower())
    if profile is None:
        print(f"[WARN] profiles.py: unknown condition '{condition}' → dùng DEFAULT_PROFILE")
    return profile or DEFAULT_PROFILE
