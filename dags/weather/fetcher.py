from abc import ABC, abstractmethod

import requests

from .models import City


class BaseWeatherFetcher(ABC):
    """
    Abstract base class cho tất cả weather fetcher.

    Tại sao dùng ABC thay vì chỉ document?
    - Python duck typing không enforce interface lúc define class
    - ABC raise TypeError ngay lúc instantiate nếu subclass quên implement
      fetch_all_with_raw → bắt lỗi sớm, không phải lúc DAG chạy lúc 3 giờ sáng
    - IDE tự gợi ý phải implement method nào
    - Exam: thể hiện biết dùng design pattern đúng chỗ
    """

    @abstractmethod
    def fetch_all_with_raw(self, cities: list[City]) -> list[dict]:
        """
        Fetch weather cho tất cả thành phố, trả về list dict gồm:
            city_id, city_name, temp, humidity, pressure, wind, condition, raw_response
        """


class OpenWeatherFetcher(BaseWeatherFetcher):
    """Fetch weather từ OpenWeatherMap API (theo lat/lon — ổn định hơn q=city_name)."""

    _BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def fetch_all_with_raw(self, cities: list[City]) -> list[dict]:
        """
        Gọi API cho từng thành phố, trả về list dict gồm:
            city_id      : ID trong HighlandsDB.Cities
            city_name    : tên thành phố
            temp         : nhiệt độ (°C)
            humidity     : độ ẩm (%)
            pressure     : áp suất (hPa)
            wind         : tốc độ gió (m/s)
            condition    : text condition ("Rain", "Clear", "Clouds", "Thunderstorm", "Drizzle")
            raw_response : toàn bộ JSON từ API (để ghi GCS Bronze)

        Dùng lat/lon thay vì q=city_name vì OpenWeather không nhận dạng
        được tên tiếng Việt không dấu (Hai Phong, Can Tho, Da Nang...).
        """
        results = []
        for city in cities:
            response = requests.get(
                self._BASE_URL,
                params={"lat": city.lat, "lon": city.lon, "appid": self._api_key, "units": "metric"},
                timeout=10,
            )
            response.raise_for_status()
            raw = response.json()
            results.append({
                "city_id":      city.id,
                "city_name":    city.name,
                "temp":         raw["main"]["temp"],
                "humidity":     raw["main"]["humidity"],
                "pressure":     raw["main"]["pressure"],
                "wind":         raw["wind"]["speed"],
                "condition":    raw["weather"][0]["main"],  # "Rain", "Clear", "Clouds"...
                "raw_response": raw,
            })
        return results


class OpenMeteoFetcher(BaseWeatherFetcher):
    """Fetch weather từ Open-Meteo API (free, không cần API key, dùng lat/lon)."""

    _WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

    # Open-Meteo WMO weather code → condition text
    # https://open-meteo.com/en/docs (mục Weather variable descriptions)
    _WMO_TO_CONDITION = {
        0: "Clear", 1: "Clear", 2: "Clouds", 3: "Clouds",
        45: "Fog", 48: "Fog",
        51: "Drizzle", 53: "Drizzle", 55: "Drizzle",
        61: "Rain", 63: "Rain", 65: "Rain",
        71: "Snow", 73: "Snow", 75: "Snow",
        80: "Rain", 81: "Rain", 82: "Rain",
        95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm",
    }

    def fetch_all_with_raw(self, cities: list[City]) -> list[dict]:
        results = []
        for city in cities:
            response = requests.get(
                self._WEATHER_URL,
                params={
                    "latitude":  city.lat,
                    "longitude": city.lon,
                    "current":   "temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,weather_code",
                },
                timeout=10,
            )
            response.raise_for_status()
            raw     = response.json()
            current = raw["current"]
            results.append({
                "city_id":      city.id,
                "city_name":    city.name,
                "temp":         current["temperature_2m"],
                "humidity":     current["relative_humidity_2m"],
                "pressure":     current["surface_pressure"],
                "wind":         current["wind_speed_10m"],
                "condition":    self._WMO_TO_CONDITION.get(current.get("weather_code", 0), "Clouds"),
                "raw_response": raw,
            })
        return results


class TomorrowIoFetcher(BaseWeatherFetcher):
    """Fetch weather từ Tomorrow.io Realtime API (units=metric, dùng lat/lon)."""

    _BASE_URL = "https://api.tomorrow.io/v4/weather/realtime"

    # Tomorrow.io weather code → condition text
    _TIO_TO_CONDITION = {
        1000: "Clear", 1001: "Clouds", 1100: "Clear", 1101: "Clouds", 1102: "Clouds",
        2000: "Fog", 2100: "Fog",
        4000: "Drizzle", 4001: "Rain", 4200: "Rain", 4201: "Rain",
        5000: "Snow", 5001: "Snow", 5100: "Snow", 5101: "Snow",
        8000: "Thunderstorm",
    }

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def fetch_all_with_raw(self, cities: list[City]) -> list[dict]:
        results = []
        for city in cities:
            response = requests.get(
                self._BASE_URL,
                params={
                    "location": f"{city.lat},{city.lon}",
                    "apikey":   self._api_key,
                    "units":    "metric",
                },
                timeout=10,
            )
            response.raise_for_status()
            raw    = response.json()
            values = raw["data"]["values"]
            results.append({
                "city_id":      city.id,
                "city_name":    city.name,
                "temp":         values["temperature"],
                "humidity":     values["humidity"],
                "pressure":     values["pressureSurfaceLevel"],
                "wind":         values["windSpeed"],
                "condition":    self._TIO_TO_CONDITION.get(values.get("weatherCode", 1001), "Clouds"),
                "raw_response": raw,
            })
        return results
