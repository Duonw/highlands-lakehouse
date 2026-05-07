from dataclasses import dataclass


@dataclass(frozen=True) 
class City:
    """
    Đại diện 1 dòng trong bảng HighlandsDB.Cities.
    Airflow dùng để biết fetch thời tiết ở đâu cho pipeline scrape weather data.
    """
    id: int          # city_id
    name: str        # vd: "Hanoi"
    lat: float
    lon: float


# Viết dataclass để không cần tự viết __init__
# Class Region:
#   def __init__(self, id, grid_id, name, api_name, lat=None, lon=None):
#       self.id = id
#       self.grid_id = grid_id
#       self.name = name
#       self.api_name = api_name
#       self.lat = lat
#       self.lon = lon #nếu NULL thì mặc định truyền None