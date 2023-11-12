import airflow
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import timedelta
import pip
import os
import sys
import shutil

# Get directory name

#  shutil.rmtree("/home/airflow/.local/lib/python3.9/site-packages/OpenSSL")



pip.main(["install", "pyOpenSSL==22.0.0"])

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
    dag_id="KALININGRAD_DAG",
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

    def modify_buildings():
        import geopandas as gpd

        buildings = gpd.read_file("/opt/airflow/dags/buildings.geojson")

        buildings["centroid"] = buildings.apply(get_centroid, axis=1)

        buildings["lat"] = buildings.centroid.y
        buildings["lon"] = buildings.centroid.x

        buildings = buildings.drop(columns=["centroid"])

        print(buildings)

        #  buildings = buildings.set_crs(4326)

        buildings.to_file("/opt/airflow/dags/buildings.geojson", driver='GeoJSON')


    def geocode_buildings_first_part():
        import geopandas as gpd

        buildings = gpd.read_file("/opt/airflow/dags/buildings.geojson")

        length = len(buildings) // 4

        buildings = buildings.loc[:length]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_file("/opt/airflow/dags/geocoded_buildings_part1.geojson", driver='GeoJSON')

    def geocode_buildings_second_part():
        import geopandas as gpd

        buildings = gpd.read_file("/opt/airflow/dags/buildings.geojson")

        length = len(buildings) // 4

        buildings = buildings.loc[length : length * 2]

        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_file("/opt/airflow/dags/geocoded_buildings_part2.geojson", driver='GeoJSON')

    def geocode_buildings_third_part():
        import geopandas as gpd

        buildings = gpd.read_file("/opt/airflow/dags/buildings.geojson")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 2: length * 3]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_file("/opt/airflow/dags/geocoded_buildings_part3.geojson", driver='GeoJSON')

    def geocode_buildings_fourth_part():
        import geopandas as gpd

        buildings = gpd.read_file("/opt/airflow/dags/buildings.geojson")
        length = len(buildings) // 4

        buildings = buildings.loc[length * 3 :]
        buildings["address"] = buildings.apply(geocode_null_addresses, axis=1)

        print(buildings)

        buildings = buildings.drop_duplicates(["address"], ignore_index=True)

        buildings.to_file("/opt/airflow/dags/geocoded_buildings_part4.geojson", driver='GeoJSON')



    get_city_geometry_task = PythonOperator(
        task_id="modify_buildings",
        python_callable=modify_buildings,
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


(
    get_city_geometry_task
    >> [geocode_buildings_task1, geocode_buildings_task2, geocode_buildings_task3, geocode_buildings_task4]

)
