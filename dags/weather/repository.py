from sqlalchemy import text
from sqlalchemy.engine import Engine

from weather.models import City


class MetadataRepository:
    """
    DB operations cho Highlands pipeline.

    Kết nối tới HighlandsDB qua engine được inject từ ngoài (DAG).
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def get_cities(self) -> list[City]:
        """
        Đọc danh sách thành phố từ HighlandsDB.
        Airflow dùng list này để biết cào thời tiết ở đâu.
        Chỉ lấy city is_active = 1 — muốn tắt thành phố nào thì UPDATE is_active = 0 trong DB.
        """
        with self._engine.connect() as conn: # mở kết nối, đặt tên là conn
            rows = conn.execute( # dùng conn để execute
                text("""
                    SELECT city_id, city_name, lat, lon
                    FROM   HighlandsDB.dbo.Cities
                    WHERE  is_active = 1
                """)
            ).fetchall()
        return [
            City(id=r.city_id, name=r.city_name, lat=float(r.lat), lon=float(r.lon))
            for r in rows
        ] # ra khỏi with -> tự động đóng kết nối, dù có lỗi hay không, không cần gọi conn.close()

    def upsert_weather_state(self, city_id: int, temperature: float,
                             condition: str, wind_speed: float, humidity: int) -> None:
        """
        UPSERT CurrentWeatherState: UPDATE nếu city_id đã có, INSERT nếu chưa có.

        Dùng MERGE thay vì UPDATE thuần vì:
        - seed_data.sql có thể chưa seed đủ mọi thành phố
        - Nếu UPDATE mà city_id chưa tồn tại → 0 rows affected, không raise exception
          → fail silent, simulator chạy với data thời tiết cũ/sai
        - MERGE đảm bảo luôn có đúng 1 dòng/thành phố sau khi chạy

        Chỉ 1 dòng/thành phố — overwrite, không giữ lịch sử (lịch sử lưu ở GCS Silver).
        """
        with self._engine.begin() as conn:
            conn.execute(
                text("""
                    MERGE HighlandsDB.dbo.CurrentWeatherState AS target
                    USING (VALUES (:city_id, :temp, :condition, :wind, :humidity))
                        AS source (city_id, temperature, weather_condition, wind_speed, humidity)
                    ON target.city_id = source.city_id
                    WHEN MATCHED THEN
                        UPDATE SET temperature       = source.temperature,
                                   weather_condition = source.weather_condition,
                                   wind_speed        = source.wind_speed,
                                   humidity          = source.humidity,
                                   last_updated      = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (city_id, temperature, weather_condition, wind_speed, humidity, last_updated)
                        VALUES (source.city_id, source.temperature, source.weather_condition,
                                source.wind_speed, source.humidity, GETDATE());
                """),
                {
                    "city_id":   city_id,
                    "temp":      temperature,
                    "condition": condition,
                    "wind":      wind_speed,
                    "humidity":  humidity,
                },
            )

"""
Lúc này DB biết chắc `city_id` chỉ là **giá trị dữ liệu**, không thể là lệnh SQL nữa — vì SQL đã compile xong rồi. 
Chuỗi `; DROP TABLE Orders; --` sẽ bị treat như literal string, không parse thêm.
## Tóm lại

| | f-string | Parameterized |
|---|---|---|
| SQL và data | Trộn lẫn thành 1 string | Tách biệt hoàn toàn |
| DB nhận | 1 string → tự parse lại | Template + data riêng |
| Hacker nhét SQL | **Được** — DB thấy và chạy | **Không được** — chỉ là data |
"""