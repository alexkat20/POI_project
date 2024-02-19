import airflow
import pandas as pd
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from sqlalchemy import create_engine
from geoalchemy2 import Geometry

import osmnx as ox
import geopandas as gpd


import vk
import time


default_args = {
    "owner": "Alexander Katynsus",
    # 'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="Ingest_data_DAG",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="Get data of a city",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
) as dag:
    token = "vk1.a.JnoCwGWUs_3-iONmrB1h-WgKvXHbyER_pZ9CWrIE37_1UKFdaTDLOLgMsdtyiTyulz9F8kBh0IJBrTdm_qMtpPsx_sX3S9bt1Uy2ndrYAqBeK19TvXjo8bgV7JgDShU-YghTstW9_qnIqwljMi_ABYy3WjOH6Q4O57boz1h-YEz3zLE0o6f0Y50cPCkHRLNUOVkZ2K_xQ_hkJVJC9Xiy4w"
    vk_api = vk.API(access_token=token)

    start_date = datetime.now().date()

    final_date = start_date - timedelta(days=7)

    start_date, final_date = time.mktime(start_date.timetuple()), time.mktime(final_date.timetuple())
    latitude, longitude = 53.0816435, 49.9100919

    def create_main_tables():
        engine = create_engine(f"postgresql://docker:docker@postgis:5432/gis")

        with engine.connect() as conn:
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS Buildings (index INT PRIMARY KEY,
                  address TEXT,
                  name TEXT,
                  centroid GEOMETRY(Geometry, 4326),
                  lat FLOAT,
                  lon float)"""
            )

            conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS Reviews (
                        place TEXT,
                        review TEXT,
                        date TEXT,
                        location GEOMETRY(Point, 4326))"""
            )

            conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS Posts (
                        group_name TEXT, post TEXT, date BIGINT)"""
            )

    def geocode_null_addresses(row):
        from geopy.geocoders import Nominatim
        import geopy

        API_KEY = " Avs8af3bGJxAbDECx-tEiM3C53lXeIOCX53-SV-StILZI6OUJq_F4wZ6kIS2RPWn"
        #  geolocator = Nominatim(user_agent="POI_app")
        geolocator = geopy.geocoders.Bing(API_KEY, timeout=5)
        lat = row["lat"]
        lon = row["lon"]

        try:
            location = geolocator.reverse(f"{lat}, {lon}")
        except:
            location = "Undefined"

        address = str(location).split(", ")
        print(", ".join(address[:3]))

        return ", ".join(address[:3])

    def get_centroid(row):
        centroid = row["geometry"].centroid

        return centroid

    def get_city_geometry(city: str = "Новокуйбышевск"):
        import geopandas as gpd
        import osmnx as ox

        territory = ox.geocode_to_gdf(city)

        print(territory)

        gdf_territory = gpd.GeoDataFrame(territory, geometry="geometry")

        gdf_territory.to_file("/opt/airflow/dags/city_geometry.geojson", driver="GeoJSON")

    def get_all_buildings(territory: str = "Новокуйбышевск"):
        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        buildings = ox.geometries_from_place(territory, {"building": True})
        buildings = buildings.reset_index()

        buildings["centroid"] = buildings.apply(get_centroid, axis=1)

        buildings["lat"] = buildings.centroid.y
        buildings["lon"] = buildings.centroid.x

        cols = ["name", "address", "centroid", "lat", "lon"]

        city = "Новокуйбышевск"

        buildings["address"] = city + " " + buildings["addr:street"] + " " + buildings["addr:housenumber"]

        buildings = gpd.GeoDataFrame(buildings, geometry="centroid")

        buildings = buildings.set_crs(4326)

        buildings_new = buildings[cols]

        buildings_new.to_postgis(
            "buildings", engine, if_exists="replace", index=True, dtype={"centroid": Geometry("POINT", srid=4326)}
        )

    def geocode_buildings_first_part():
        import geopandas as gpd

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        buildings = gpd.read_postgis("select * from Buildings where address is Null", con=engine, geom_col="centroid")

        length = len(buildings) // 4

        buildings = buildings.loc[:length]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        buildings = buildings[buildings["address"] != "Undefined"]
        buildings = buildings.set_crs(4326)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_postgis(
            "buildings", engine, if_exists="append", index=False, dtype={"centroid": Geometry("POINT", srid=4326)}
        )

    def geocode_buildings_second_part():
        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        buildings = gpd.read_postgis("select * from Buildings where address is Null", con=engine, geom_col="centroid")

        length = len(buildings) // 4

        buildings = buildings.loc[length : length * 2]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        buildings = buildings[buildings["address"] != "Undefined"]

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings = buildings.set_crs(4326)

        buildings.to_postgis(
            "buildings", engine, if_exists="append", index=False, dtype={"centroid": Geometry("POINT", srid=4326)}
        )

    def geocode_buildings_third_part():
        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        buildings = gpd.read_postgis("select * from Buildings where address is Null", con=engine, geom_col="centroid")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 2 : length * 3]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        buildings = buildings[buildings["address"] != "Undefined"]

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)
        buildings = buildings.set_crs(4326)

        buildings.to_postgis(
            "buildings", engine, if_exists="append", index=False, dtype={"centroid": Geometry("POINT", srid=4326)}
        )

    def geocode_buildings_fourth_part():
        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        buildings = gpd.read_postgis("select * from Buildings where address is Null", con=engine, geom_col="centroid")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 3 :]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        buildings = buildings[buildings["address"] != "Undefined"]

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)
        buildings = buildings.set_crs(4326)

        buildings.to_postgis(
            "buildings", engine, if_exists="append", index=False, dtype={"centroid": Geometry("POINT", srid=4326)}
        )

        with engine.connect() as conn:
            conn.execute("""DELETE FROM Buildings WHERE address IS NULL""")

    def get_reviews():
        import geopandas as gpd
        from Yandex_parser import GrabberApp
        from shapely import Point

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        data = gpd.read_postgis("select * from Buildings", con=engine, geom_col="centroid")

        addresses = data["address"].tolist()
        locations: list[Point] = data["centroid"].to_list()

        grabber = GrabberApp()

        for i in range(len(addresses)):
            try:
                data = grabber.grab_data(locations[i], addresses[i])
                name = addresses[i].replace(",", "_").replace(" ", "_").replace("/", "_")
                if len(data) != 0:
                    print(name)
                    print(data)
                    data = gpd.GeoDataFrame(data, geometry="location", crs=4326)
                    data.to_postgis(
                        "reviews",
                        engine,
                        if_exists="append",
                        index=False,
                        dtype={"location": Geometry("POINT", srid=4326)},
                    )
                else:
                    print("Empty DataFrame")
            except Exception as e:
                print(e)
        grabber.driver.quit()

    def get_vk_groups(query="Новокуйбышевск"):
        search_groups = vk_api.groups.search(q=query, sort=6, v="5.131")
        groups = [search_groups["items"][i]["screen_name"] for i in range(len(search_groups["items"]))]

        return groups

    def get_posts(domain, offset, count, start_date):
        for i in range(100):
            try:
                result = vk_api.wall.get(domain=domain, offset=offset, count=count, v="5.131")
                result = [[result["items"][i]["text"], result["items"][i]["date"]] for i in range(len(result["items"]))]
                return result
            except:
                continue
        return [["", start_date]]

    def get_group_posts():
        import pandas as pd

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        groups = get_vk_groups()

        groups_posts = pd.DataFrame({"group_name": [], "post": [], "date": []})

        for i in range(len(groups)):
            if i % 3 == 0:
                time.sleep(1)
            group_info = []
            current_info = get_posts(domain=groups[i], offset=0, count=10, start_date=start_date)
            if len(current_info) == 0 or current_info[0][1] < final_date:
                continue
            group_info += current_info
            j = 1
            while current_info and start_date > current_info[0][1] > final_date:
                current_info = get_posts(domain=groups[i], offset=10 * j, count=10, start_date=start_date)
                j += 1
                for post in current_info:
                    groups_posts.loc[groups_posts.shape[0]] = {
                        "group_name": groups[i],
                        "post": post[0],
                        "date": post[1],
                    }

        groups_posts.to_sql("posts", engine, if_exists="append", index=False)

    def get_news(query="Новокуйбышевск"):
        import time

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        start_date = datetime.now().date()

        final_date = start_date - timedelta(days=3)

        start_date, final_date = time.mktime(start_date.timetuple()), time.mktime(final_date.timetuple())

        step = 800

        city_posts = pd.DataFrame({"group_name": [], "post": [], "date": []})

        while start_date >= final_date:
            search_by_query = vk_api.newsfeed.search(
                q=query, count=200, v="5.131", start_time=final_date, end_time=start_date
            )
            search_by_query = [
                [search_by_query["items"][i]["text"], search_by_query["items"][i]["date"]]
                for i in range(len(search_by_query["items"]))
            ]
            start_date -= step
            for post in search_by_query:
                city_posts.loc[city_posts.shape[0]] = {"group_name": query, "post": post[0], "date": post[1]}
            time.sleep(1)

        city_posts.to_sql("posts", engine, if_exists="append", index=False)

    def get_photos(city="Новокуйбышевск"):
        import time

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        start_date = datetime.now().date()

        final_date = start_date - timedelta(days=7)

        start_date, final_date = time.mktime(start_date.timetuple()), time.mktime(final_date.timetuple())

        step = 800

        city_photos = pd.DataFrame({"group_name": city, "post": [], "date": []})

        while start_date >= final_date:
            search_by_coordinates = vk_api.photos.search(
                count=1000,
                latitude=latitude,
                longitude=longitude,
                v="5.131",
                start_time=start_date,
                end_time=final_date,
                radius=50000,
            )
            search_by_coordinates = [
                [search_by_coordinates["items"][i]["text"], search_by_coordinates["items"][i]["date"]]
                for i in range(len(search_by_coordinates["items"]))
            ]
            start_date -= step
            for post in search_by_coordinates:
                print(post)
                city_photos.loc[city_photos.shape[0]] = {"group_name": city, "post": post[0], "date": post[1]}

            time.sleep(1)

        city_photos.to_sql("posts", engine, if_exists="append", index=False)

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_main_tables,
        provide_context=True,
    )

    get_city_geometry_task = PythonOperator(
        task_id="get_city_geometry",
        python_callable=get_city_geometry,
        provide_context=True,
    )

    get_all_buildings_task = PythonOperator(
        task_id="get_all_buildings",
        python_callable=get_all_buildings,
        provide_context=True,
    )

    geocode_buildings_task1 = PythonOperator(
        task_id="geocode_buildings1",
        python_callable=geocode_buildings_first_part,
        provide_context=True,
    )

    geocode_buildings_task2 = PythonOperator(
        task_id="geocode_buildings2",
        python_callable=geocode_buildings_second_part,
        provide_context=True,
    )

    geocode_buildings_task3 = PythonOperator(
        task_id="geocode_buildings3",
        python_callable=geocode_buildings_third_part,
        provide_context=True,
    )

    geocode_buildings_task4 = PythonOperator(
        task_id="geocode_buildings4",
        python_callable=geocode_buildings_fourth_part,
        provide_context=True,
    )

    reviews_task = PythonOperator(
        task_id="get_reviews",
        python_callable=get_reviews,
        provide_context=True,
    )

    vk_groups_task = PythonOperator(
        task_id="get_group_posts",
        python_callable=get_group_posts,
        provide_context=True,
    )

    vk_news_task = PythonOperator(
        task_id="get_news",
        python_callable=get_news,
        provide_context=True,
    )

    vk_photos_task = PythonOperator(
        task_id="get_photos",
        python_callable=get_photos,
        provide_context=True,
    )

(
    create_tables_task
    >> get_city_geometry_task
    >> get_all_buildings_task
    >> vk_groups_task
    >> vk_news_task
    >> vk_photos_task
    >> [geocode_buildings_task1, geocode_buildings_task2, geocode_buildings_task3, geocode_buildings_task4]
    >> reviews_task
)
