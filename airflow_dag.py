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

#  IMAGE = "registry.neomsa.ru/docker-mlops/mlops/airflow:2.2.5-demo-v4"

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
        geolocator = Nominatim(user_agent="POI_app")
        centroid = row["geometry"].centroid
        lat = centroid.y
        lon = centroid.x

        location = geolocator.reverse(f"{lat}, {lon}")

        return location

    def get_city_geometry(city: str):
        import osmnx as ox

        territory = ox.geocode_to_gdf(city)

        territory.to_file('city_geometry.geojson', driver='GeoJSON')


    def get_all_buildings(territory: str):
        import osmnx as ox

        buildings = ox.geometries_from_place(territory, {"building": True})
        buildings = buildings.reset_index()

        buildings["lat"] = buildings.centroid.y
        buildings["lon"] = buildings.centroid.x

        buildings = buildings.set_crs(4326)
        cols = ["name", "geometry", "addr:street", "addr:housenumber", "centroid", "building", "lat", "lon"]

        buildings_with_addresses = buildings[~buildings["addr:housenumber"].isna()][cols]
        buildings_without_addresses = buildings[buildings["addr:housenumber"].isna()][cols]

        buildings_with_addresses[["name", "addr:street", "addr:housenumber"]].to_csv("buildings_with_addresses.csv")
        #  buildings_without_addresses.to_file("buildings_without_addresses.geojson", driver="GeoJSON)


    def geocode_buildings(buildings):
        buildings["adddress"] = buildings.apply(geocode_null_addresses, axis=1)

        return geocode_buildings


    get_city_geometry_task = PythonOperator(
        task_id="get_city_geometry",
        python_callable=get_city_geometry,
        provide_context=True
    )

    get_all_buildings_task = PythonOperator(
        task_id="get_all_buildings",
        python_callable=get_all_buildings,
        provide_context=True,
    )


get_city_geometry_task >> get_all_buildings_task
