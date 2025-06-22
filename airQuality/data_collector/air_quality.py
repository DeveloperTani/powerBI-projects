import requests
import csv
import time
import datetime
import argparse
import threading
import sys
import itertools

parser = argparse.ArgumentParser()
parser.add_argument('--country', help='Optional: Single country code (e.g., FI). If omitted, runs all in list.')
args = parser.parse_args()

BASE_URL = "https://airquality-frost.k8s.ilt-dmz.iosb.fraunhofer.de/v1.1"

class Spinner:
    def __init__(self, message="Loading... "):
        self.spinner = itertools.cycle(['|', '/', '-', '\\'])
        self.stop_running = False
        self.thread = threading.Thread(target=self._spin)
        self.message = message

    def _spin(self):
        while not self.stop_running:
            sys.stdout.write(f"\r{self.message}{next(self.spinner)}")
            sys.stdout.flush()
            time.sleep(0.1)

    def start(self):
        self.stop_running = False
        self.thread.start()

    def stop(self):
        self.stop_running = True
        self.thread.join()
        sys.stdout.write("\rDone! ✅\n")
        sys.stdout.flush()

def get_locations(country_code):
    url = f"{BASE_URL}/Locations?$filter=properties/countryCode eq '{country_code}'"
    response = requests.get(url)
    return response.json().get("value", []) if response.status_code == 200 else []

def get_datastreams(location_id):
    url = f"{BASE_URL}/Locations({location_id})/Things?$expand=Datastreams"
    response = requests.get(url)
    if response.status_code != 200:
        return []
    things = response.json().get("value", [])
    datastreams = []
    for thing in things:
        datastreams.extend(thing.get("Datastreams", []))
    return datastreams

def get_latest_observation(datastream_id):
    url = f"{BASE_URL}/Datastreams({datastream_id})/Observations?$top=1000&$orderby=phenomenonTime desc"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    observations = response.json().get("value", [])
    return observations[0] if observations else None

def fetch_and_write(country_code):
    print(f"\n🚀 Fetching air quality data for country: {country_code}")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"air_quality_data_{country_code}_{timestamp}.csv"

    locations = get_locations(country_code)

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Station ID", "Datastream Name", "Observed Property", "Unit", "Result", "Timestamp", "Latitude", "Longitude", "CountryCode"
        ])

        for loc in locations:
            loc_id = loc.get("@iot.id")
            coordinates = loc.get("location", {}).get("coordinates", [None, None])
            lat, lon = coordinates[1], coordinates[0] if coordinates else (None, None)

            datastreams = get_datastreams(loc_id)
            for stream in datastreams:
                datastream_id = stream.get("@iot.id")
                name = stream.get("name")
                description = stream.get("description", "")
                observed_property = description.split(" ")[0] if description else "Unknown"
                unit = stream.get("unitOfMeasurement", {}).get("symbol", "Unknown")
                obs = get_latest_observation(datastream_id)

                if obs:
                    result = obs.get("result")
                    time_ = obs.get("phenomenonTime")
                    writer.writerow([
                        loc_id, name, observed_property, unit, result, time_, lat, lon, country_code
                    ])
                time.sleep(0.1)  # Avoid rate limit

    print(f"✅ Data for {country_code} saved to '{filename}'.")

if __name__ == "__main__":
    spinner = Spinner("Fetching and writing air quality data... ")
    spinner.start()

    try:
        if args.country:
            fetch_and_write(args.country.upper())
        else:
            countries = ["FI", "SE", "NO", "DK", "DE", "FR", "IT", "PL"]
            for code in countries:
                fetch_and_write(code)
                time.sleep(1)  # Slight pause between countries
    finally:
        spinner.stop()
