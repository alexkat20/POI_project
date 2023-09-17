from prefect import flow, task


def geocode_null_addresses(row):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="POI_app")
    centroid = row["geometry"].centroid
    lat = centroid.y
    lon = centroid.x

    location = geolocator.reverse(f"{lat}, {lon}")

    return location

@task
def get_city_geometry(city: str):
    import osmnx as ox

    territory = ox.geocode_to_gdf(city)

    return territory


@task
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

    return buildings_with_addresses, buildings_without_addresses


@task
def geocode_buildings(buildings):
    buildings["adddress"] = buildings.apply(geocode_null_addresses, axis=1)

    return geocode_buildings


@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def get_city_buildings(city_name: str):
    city_geometry = get_city_geometry(city_name)
    buildings_with_addresses, buildings_without_addresses = get_all_buildings(city_name)
    city_geometry.to_file('city_geometry.geojson', driver='GeoJSON')
    buildings_with_addresses.to_file('buildings_with_addresses.geojson', driver='GeoJSON')
    buildings_without_addresses.to_file('buildings_without_addresses.geojson', driver='GeoJSON')




