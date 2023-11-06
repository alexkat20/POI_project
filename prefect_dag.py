from prefect import flow, task


def geocode_null_addresses(row):
    from geopy.geocoders import Nominatim

    geolocator = Nominatim(user_agent="POI_app")
    centroid = row["geometry"].centroid
    lat = centroid.y
    lon = centroid.x

    location = geolocator.reverse(f"{lat}, {lon}")

    return location


def get_centroid(row):
    centroid = row["geometry"].centroid

    return centroid


@task
def get_city_geometry(city: str = "Миасс, Челябинская область"):
    import osmnx as ox

    territory = ox.geocode_to_gdf(city)

    return territory


@task
def get_all_buildings(territory: str = "Миасс, Челябинская область"):
    import osmnx as ox

    buildings = ox.geometries_from_place(territory, {"building": True})
    buildings = buildings.reset_index()

    buildings["centroid"] = buildings.apply(get_centroid, axis=1)

    buildings["lat"] = buildings.centroid.y
    buildings["lon"] = buildings.centroid.x

    buildings = buildings.set_crs(4326)
    cols = ["name", "geometry", "addr:street", "addr:housenumber", "centroid", "lat", "lon"]

    buildings_with_addresses = buildings[~buildings["addr:housenumber"].isna()][cols]
    buildings_without_addresses = buildings[buildings["addr:housenumber"].isna()][cols]

    return buildings_with_addresses[["addr:street", "addr:housenumber"]], buildings_without_addresses


@task
def geocode_buildings(buildings):
    buildings["adddress"] = buildings.apply(geocode_null_addresses, axis=1)

    return geocode_buildings


@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def get_city_buildings(city_name: str = "Миасс, Челябинская область"):
    city_geometry = get_city_geometry(city_name)
    buildings_with_addresses, buildings_without_addresses = get_all_buildings(city_name)
    city_geometry.to_file("city_geometry.geojson", driver="GeoJSON")
    print(buildings_with_addresses)
    #  buildings_with_addresses.to_csv('buildings_with_addresses.csv')
    #  buildings_without_addresses.to_file('buildings_without_addresses.geojson', driver='GeoJSON')
