import json
import os
import surfpy
import math
from pymongo import MongoClient
from datetime import datetime
import time



def calculate_angle(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # Calculate differences
    dlon = lon2 - lon1

    # Calculate angle
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    angle = math.atan2(y, x)

    # Convert angle from radians to degrees
    angle = math.degrees(angle)

    # Normalize angle to be between 0 and 360
    angle = (angle + 360) % 360

    return angle

if __name__ == '__main__':

    mongo_connection_string = os.environ['MONGO_CONNECTION']
    

    # Connection URL
    dburl = mongo_connection_string

    # Database Name
    dbName = "test"

    # Create a MongoClient
    client = MongoClient(dburl)
    # Connect to the database
    db = client[dbName]

    # Assuming `db` is your connected database from the previous step
    spots_collection = db['spots']
    surf_datas_collection = db['surfDatas']



    # Get all spots
    all_spots = spots_collection.find()

    # Create a dictionary where each key is a spot name and each value is the corresponding document
    spots_dict = {spot['name']: spot for spot in all_spots}

    west_model = surfpy.wavemodel.us_west_coast_gfs_wave_model()
    west_gfs = west_model.fetch_grib_datas(0, 192)
    east_model = surfpy.wavemodel.atlantic_gfs_wave_model()
    east_gfs = east_model.fetch_grib_datas(0, 192)

    for key in spots_dict.keys():
        spot = spots_dict[key]
        spotId = spot['_id']
        name = spot['name']
        buoyId = spot['buoyId']
        buoy_x = float(spot['buoy_x'])
        buoy_y = float(spot['buoy_y'])
        lat = float(spot['lat'])
        lon = float(spot['lon'])
        depth = float(spot['depth'])
        slope = float(spot['slope'])
        model = spot['model']

        print("Processing spot: ", name)

        # Create Location objects
        buoy_location = surfpy.Location(buoy_x, buoy_y, altitude=depth, name='Buoy Location')
        buoy_location.depth = depth
        buoy_location.angle = calculate_angle(buoy_x, buoy_y, lat, lon)
        buoy_location.slope = slope

        beach_location = surfpy.Location(lat, lon, altitude=0.0, name='Break Location')

        # Fetch and parse wave data
        wave_model = None
        raw_wave_data = None
        if model == 'west':
            wave_model = west_model
            raw_wave_data = wave_model.parse_grib_datas(buoy_location, west_gfs)

        elif model == 'east':
            wave_model = east_model
            raw_wave_data = wave_model.parse_grib_datas(buoy_location, east_gfs)

        # Convert to buoy data
        data = wave_model.to_buoy_data(raw_wave_data)

        max_retries = 3
        for i in range(max_retries):
            try:
                # Fetch weather data and merge
                weather_data = surfpy.WeatherApi.fetch_hourly_forecast(beach_location)
                surfpy.merge_wave_weather_data(data, weather_data)

                break
            except Exception as e:
                print(f"Attempt {i+1} failed with error: {e}")
                if i < max_retries - 1:  # No delay on the last attempt
                    time.sleep(5)  # Wait for 5 seconds before the next retry
                else:
                    raise  # Re-raise the last exception if all attempts failed

        # Process data
        for dat in data:
            dat.solve_breaking_wave_heights(buoy_location)
            dat.change_units(surfpy.units.Units.english)
        
        json_data = surfpy.serialize(data)
        # Write some of the results to the surfDatas collection

        total_surf_data = json.loads(json_data)
       
        for dat in total_surf_data:
            swell_components = [
                {
                    'period': component['period'],
                    'direction': component['direction'],
                    'wave_height': component['wave_height'],
                }
                for component in dat['swell_components']
            ]

            surf_data_document = {
                'spotId': spotId,
                'name': name,
                'date': dat['date'],
                'sessionDatetime': datetime.fromtimestamp(dat['date']),  # convert epoch to datetime
                'windSpeed': dat['wind_speed'],
                'windDirection': dat['wind_direction'],
                'windCompassDirection': dat['wind_compass_direction'],
                'primarySwellPeriod': dat['wave_summary']['period'],
                'primarySwellHeight': dat['wave_summary']['wave_height'],
                'primarySwellDirection': dat['wave_summary']['direction'],
                'primarySwellCompassDirection': dat['wave_summary']['compass_direction'],
                'swellComponents': swell_components,
            }

            # Update the document if it exists, insert it if it doesn't
            surf_datas_collection.update_one(
                {'spotId': spotId, 'date': surf_data_document['date']},  # filter
                {'$set': surf_data_document},  
                upsert=True  
            )
        
    

        

