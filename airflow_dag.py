import airflow
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import timedelta


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
    dag_id="POI_DAG",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="Get data of a city",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
) as dag:

    def geocode_null_addresses(row):
        from geopy.geocoders import Nominatim
        import geopy

        API_KEY = " Avs8af3bGJxAbDECx-tEiM3C53lXeIOCX53-SV-StILZI6OUJq_F4wZ6kIS2RPWn"
        #  geolocator = Nominatim(user_agent="POI_app")
        geolocator = geopy.geocoders.Bing(API_KEY)
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

    def get_city_geometry(city: str = "Миасс, Челябинская область"):
        import osmnx as ox

        territory = ox.geocode_to_gdf(city)

        print(territory)

        #  territory.to_file('city_geometry.geojson', driver='GeoJSON')

    def get_all_buildings(territory: str = "Миасс, Челябинская область"):
        import osmnx as ox

        buildings = ox.geometries_from_place(territory, {"building": True})
        buildings = buildings.reset_index()

        buildings["centroid"] = buildings.apply(get_centroid, axis=1)

        buildings["lat"] = buildings.centroid.y
        buildings["lon"] = buildings.centroid.x

        buildings = buildings.set_crs(4326)

        buildings.to_csv("/opt/airflow/dags/buildings.csv")
        print(buildings)

    def split_buildings():
        import pandas as pd

        buildings = pd.read_csv("/opt/airflow/dags/buildings.csv")

        cols = ["name", "geometry", "addr:street", "addr:housenumber", "centroid", "lat", "lon"]

        buildings_with_addresses = buildings[~buildings["addr:housenumber"].isna()][cols]
        buildings_without_addresses = buildings[buildings["addr:housenumber"].isna()][cols]

        #  buildings_with_addresses[["name", "addr:street", "addr:housenumber"]].to_csv("buildings_with_addresses.csv")
        #  buildings_without_addresses.to_file("buildings_without_addresses.geojson", driver="GeoJSON")

        buildings_with_addresses.to_csv("/opt/airflow/dags/buildings_with_addresses.csv")
        buildings_without_addresses.to_csv("/opt/airflow/dags/buildings_without_addresses.csv")
        print(buildings_without_addresses)

    def geocode_buildings_first_part():
        import pandas as pd

        buildings = pd.read_csv("/opt/airflow/dags/buildings_without_addresses.csv")

        length = len(buildings) // 4

        buildings = buildings.loc[:length]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_csv("/opt/airflow/dags/geocoded_buildings_without_addresses_part1.csv")

    def geocode_buildings_second_part():
        import pandas as pd

        buildings = pd.read_csv("/opt/airflow/dags/buildings_without_addresses.csv")

        length = len(buildings) // 4

        buildings = buildings.loc[length : length * 2]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_csv("/opt/airflow/dags/geocoded_buildings_without_addresses_part2.csv")

    def geocode_buildings_third_part():
        import pandas as pd

        buildings = pd.read_csv("/opt/airflow/dags/buildings_without_addresses.csv")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 2: length * 3]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_csv("/opt/airflow/dags/geocoded_buildings_without_addresses_part3.csv")

    def geocode_buildings_fourth_part():
        import pandas as pd

        buildings = pd.read_csv("/opt/airflow/dags/buildings_without_addresses.csv")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 3 :]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_csv("/opt/airflow/dags/geocoded_buildings_without_addresses_part4.csv")

    def get_reviews():
        import pandas as pd
        from Yandex_parser import GrabberApp

        data = pd.read_csv("/opt/airflow/dags/geocoded_buildings_without_addresses_part4.csv")

        addresses = data["address"].tolist()

        for address in addresses[0:10]:
            grabber = GrabberApp(address)
            data = grabber.grab_data()
            name = address.replace(',', '_').replace(' ', '_').replace("/", "_")
            data.to_csv(f"./data/{name}.csv")


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

    split_buildings_task = PythonOperator(
        task_id="split_buildings",
        python_callable=split_buildings,
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

(
    get_city_geometry_task
    >> get_all_buildings_task
    >> split_buildings_task
    >> [geocode_buildings_task1, geocode_buildings_task2, geocode_buildings_task3, geocode_buildings_task4]
    >> reviews_task
)
