from datetime import datetime, time, timedelta, timezone
from io import BytesIO

import hassapi as hass
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import pytz
from influxdb_client import InfluxDBClient, Point
from mastodon import Mastodon

intensity_indexes = {
    2021: [50, 140, 220, 330],
    2022: [45, 130, 210, 310],
    2023: [40, 120, 200, 290],
    2024: [35, 110, 190, 270],
    2025: [30, 100, 180, 250],
    2026: [25, 90, 170, 230],
    2027: [20, 80, 160, 210],
    2028: [15, 70, 150, 190],
    2029: [10, 60, 140, 170],
    2030: [5, 50, 130, 150],
}


class InformationBot(hass.Hass):
    def initialize(self):
        self.tidal_broadcast = self.args["tidal_broadcast"]
        self.pressure_sensor = self.args["pressure_sensor"]
        self.temperature_sensor = self.args["temperature_sensor"]
        self.feellike_temperature_sensor = self.args["feellike_temperature_sensor"]
        self.humidity_sensor = self.args["humidity_sensor"]
        self.uv_sensor = self.args["uv_sensor"]
        self.rain_sensor = self.args["rain_sensor"]
        self.pressure_last_read = self.args["pressure_last_read"]
        self.temperature_last_read = self.args["temperature_last_read"]
        self.feellike_temperature_last_read = self.args[
            "feellike_temperature_last_read"
        ]
        self.humidity_last_read = self.args["humidity_last_read"]
        self.update_previous = self.args["update_previous"]
        self.carbon_intensity_sensor = self.args["carbon_intensity_sensor"]
        self.location = self.args["location"]
        self.hashtags = self.args["hashtags"]
        self.airplane_count_sensor = self.args["airplane_count_sensor"]

        self.disclaimer = (
            "\nDEVELOPMENT TEST - PLEASE IGNORE THIS MESSAGE\n\n"
            if self.args["dev_test"]
            else ""
        )

        self.mastodon = Mastodon(
            client_id=self.args["mastodon_key"],
            client_secret=self.args["mastodon_secret"],
            access_token=self.args["mastodom_token"],
            api_base_url=self.args["mastodon_base_url"],
        )

        # Weather report every 3 hours
        for each in [0, 3, 6, 9, 12, 15, 18, 21]:
            runtime = time(each, 0, 0)
            self.run_daily(
                self.publish_weather,
                runtime,
                ice_warning=True if each in [6, 9] else False,
            )

        # Energy report at 7am, 12pm and 7pm
        for each in [7, 13, 19]:
            runtime = time(each, 0, 0)
            self.run_daily(self.publish_energy_report, runtime)

        # airplane tally for the day at 8.10am
        self.run_daily(self.publish_airplane_count, time(8, 10, 0))

        # Request status update for each of the monitored flood areas
        self.flood_areas_list = self.args["flood_areas"]
        for each in self.flood_areas_list:
            self.log(f"Listening for updated on {each}")
            self.listen_state(
                self.publish_flood_update, entity_id=each, attribute="all"
            )

        # Listen to a home assistant event to force republishing weather and energy message
        self.listen_event(self.publish_updates_event, "info_bot_update")

        # Monitor tidal water levels
        for each in self.args["water_level"]:
            self.log(f"Listening for updated on {each['sensor']}")
            self.listen_state(
                self.publish_tidal_cycle,
                entity_id=each["sensor"],
                attribute="all",
                sensor_label=each["label"],
            )

    def get_localized_time(self, time):
        """Convert a utc hass timestame in a localised datetime object"""
        utc = self.convert_utc(time)
        local_tz = pytz.timezone(self.get_timezone())
        return utc.replace(tzinfo=local_tz)

    def get_influx_db_client(self):
        client = InfluxDBClient(
            url=self.args["influx_creds"]["url"],
            token=self.args["influx_creds"]["token"],
            org=self.args["influx_creds"]["org"],
        )
        return client

    def post_to_mastodon(self, message, medias=None, limit=480):
        """
        Post a message to mastodon. Will split in multiple posts if needed
        """
        uploaded_medias = None
        if medias is not None:
            uploaded_medias = []
            for each in medias:
                res = self.mastodon.media_post(
                    media_file=each["bytes"].getvalue(),
                    mime_type=each["mime_type"],
                    file_name=each["file_name"],
                )
                uploaded_medias.append(res.id)
        if len(message) < limit:
            res = self.mastodon.status_post(message, media_ids=uploaded_medias)
        else:
            previous = None
            for each in split_chunks(message, limit):
                res = self.mastodon.status_post(
                    each,
                    in_reply_to_id=previous,
                    visibility=None if previous == None else "unlisted",
                    media_ids=uploaded_medias,
                )
                previous = res.id

    def read_numerical_sensor(self, sensor):
        """
        Read all the possible source for a sensor and return
        a float if readable or None if they are all 'unavailable'
        """
        for each in sensor:
            state = self.get_state(each)
            if state == "unavailable":
                next()
            else:
                return float(state)
        return None

    def publish_updates_event(self, event_name, data, kwargs):
        """Callback function to force the republishing of the weater / energy report"""
        self.log("update event caught")
        # self.publish_energy_report(None)
        # self.publish_weather(kwargs={"ice_warning": True})
        self.publish_airplane_count(None)

    def publish_flood_update(self, entity, attribute, old, new, kwargs):
        """Publish an update when a flood sensor is update"""
        self.log(f"{entity} has been updated considering a broadcast")

        location = self.get_state(entity, "friendly_name")
        tz = datetime.now(timezone.utc).astimezone().tzinfo
        now = datetime.now(tz)
        message = f'{self.disclaimer}[{now.strftime("%H:%M %Z")}] '

        if new["state"] == "unavailable":
            return
        elif old["state"] == "unavailable":
            if new["attributes"]["risk_level"] > 0:
                message = message + f"New flood alert at {location}\n"
                ea_message = self.get_state(entity, "current_warning")
                message = (
                    message
                    + f'Description: {ea_message[0]["message"]}'
                    + f"""\n\n{self.hashtags} #Tide #Thames"""
                )
                self.post_to_mastodon(message)
        elif (
            old["attributes"]["risk_level"] == 0 and new["attributes"]["risk_level"] > 0
        ):  # This is a new alert
            message = message + f"New flood alert at {location}\n"
            ea_message = self.get_state(entity, "current_warning")
            message = (
                message
                + f'Description: {ea_message[0]["message"]}'
                + f"""\n\n{self.hashtags} #Tide #Thames"""
            )
            self.post_to_mastodon(message)
        elif (
            new["attributes"]["risk_level"] == 0 and old["attributes"]["risk_level"] > 0
        ):  # This is the end of the alert
            message = (
                message
                + f"End of flood alert at {location}"
                + f"""\n\n{self.hashtags} #Tide #Thames"""
            )
            self.post_to_mastodon(message)
        elif (
            new["attributes"]["risk_level"] > old["attributes"]["risk_level"]
        ):  # Increasing risk
            message = message + f"INCREASED flood alert at {location}\n"
            ea_message = self.get_state(entity, "current_warning")
            message = (
                message
                + f'Description: {ea_message[0]["message"]}'
                + f"""\n\n{self.hashtags} #Tide #Thames"""
            )
            self.post_to_mastodon(message)
        elif (
            old["attributes"]["risk_level"] < new["attributes"]["risk_level"]
        ):  # decreasing risk
            message = message + f"Decreased flood alert at {location}\n"
            ea_message = self.get_state(entity, "current_warning")
            message = (
                message
                + f'Description: {ea_message[0]["message"]}'
                + f"""\n\n{self.hashtags} #Tide #Thames"""
            )
            self.post_to_mastodon(message)
        elif (
            not new["attributes"]["current_warning"][0]["message"]
            == old["attributes"]["current_warning"][0]["message"]
        ):
            message = (
                message
                + f"""The EA updated the information for the flood alert at {location}"""
            )
            ea_message = self.get_state(entity, "current_warning")
            message = (
                message
                + f'Description: {ea_message[0]["message"]}'
                + f"""\n\n{self.hashtags} #Tide #Thames"""
            )
            self.post_to_mastodon(message)

    def publish_weather(self, kwargs):
        """Publish the weather report"""
        tz = datetime.now(timezone.utc).astimezone().tzinfo
        now = datetime.now(tz)

        temperature = self.read_numerical_sensor(self.temperature_sensor)
        feellike_temperature = self.read_numerical_sensor(
            self.feellike_temperature_sensor
        )
        pressure = self.read_numerical_sensor(self.pressure_sensor)
        humidity = self.read_numerical_sensor(self.humidity_sensor)
        rain = self.read_numerical_sensor(self.rain_sensor)
        uv_index = self.read_numerical_sensor(self.uv_sensor)

        previous_temperature = float(self.get_state(self.temperature_last_read))
        previous_feellike_temperature = float(
            self.get_state(self.feellike_temperature_last_read)
        )
        previous_pressure = float(self.get_state(self.pressure_last_read))
        previous_humidity = float(self.get_state(self.humidity_last_read))

        ice_warning = ""

        if "ice_warning" in kwargs and kwargs["ice_warning"]:
            # graph the historical graph
            temperature_serie = self.temperature_sensor[0].split(".")[1]
            client = self.get_influx_db_client()
            query = f"""
                from(bucket: \"{self.args["influx_creds"]["bucket"]}\")
                  |> range(start: -12h, stop: now())
                  |> filter(fn: (r) => r.entity_id == "{temperature_serie}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """
            temperature_history = client.query_api().query_data_frame(query=query)
            temperature_history = temperature_history[["_time", "value"]]
            #self.log(temperature_history)
            negative_temps = temperature_history[temperature_history.value < 0]
            if negative_temps.count().value > 0:
                ice_warning = "\n❄️❄️❄️ ICE WARNING - Overnight temperature dipped below 0°C ❄️❄️❄️\n"

        message = f"""{self.disclaimer}[{now.strftime("%H:%M %Z")}] \
{self.location} 

🌡️: {temperature:,.1f}°C{trend_indicator(previous_temperature, temperature)} 
🌡️ feel like: {feellike_temperature:,.1f}°C{trend_indicator(previous_feellike_temperature, feellike_temperature)} 
🌀: {round(pressure,0):,.0f}hpa{trend_indicator(previous_pressure, pressure)} 
💧: {humidity:.0f}%{trend_indicator(previous_humidity, humidity)}
☔: {rain:.1f}mm today
☀️: {uv_index} UV index
{ice_warning}
{self.hashtags} #Weather #WeatherStation"""

        self.post_to_mastodon(message)

        if self.update_previous:
            self.set_state(self.temperature_last_read, state=temperature)
            self.set_state(
                self.feellike_temperature_last_read, state=feellike_temperature
            )
            self.set_state(self.pressure_last_read, state=pressure)
            self.set_state(self.humidity_last_read, state=humidity)

    def publish_energy_report(self, kwargs):
        """
        Publish an energy report
        """
        tz = datetime.now(timezone.utc).astimezone().tzinfo
        now = datetime.now(tz)

        # Placeholder message if data is not available
        if self.get_state(self.carbon_intensity_sensor) == "unavailable":
            message = f"""{self.disclaimer}[{now.strftime("%H:%M %Z")}] \
{self.location} ⚡: Carbon intensity data is not available currently."""

            self.post_to_mastodon(message, medias=medias)
            return

        carbon_intensity_value = float(
            self.get_state(self.carbon_intensity_sensor, "current_period_forecast")
        )
        carbon_intensity = self.get_state(self.carbon_intensity_sensor)
        carbon_intensity_optimal_start = self.get_localized_time(
            self.get_state(self.carbon_intensity_sensor, "optimal_window_from")
        )
        carbon_intensity_optimal_end = self.get_localized_time(
            self.get_state(self.carbon_intensity_sensor, "optimal_window_to")
        )

        time_to_carbon_intensity_optimal = (
            carbon_intensity_optimal_start
            - datetime.now(tz=pytz.timezone(self.get_timezone()))
        )
        hours, remainder = divmod(
            time_to_carbon_intensity_optimal.total_seconds(), 3600
        )
        minutes, seconds = divmod(remainder, 60)

        message = f"""{self.disclaimer}[{now.strftime("%H:%M %Z")}] \
{self.location} ⚡: The carbon intensity of our electricity is currently estimated at \
{float(carbon_intensity_value):,.0f}g/kWh which is {carbon_intensity}. """
        if hours < 0 or minutes < 0:
            message = (
                message
                + "If you have energy-hungry devices to run, you'd better do it now. "
            )
        else:
            message = (
                message
                + f"""The best time to run energy-hungry devices is between {carbon_intensity_optimal_start.strftime("%H:%M %Z")} \
and {carbon_intensity_optimal_end.strftime("%H:%M %Z")}. This is {hours:.0f} hours \
and {minutes:.0f} minutes from now. """
            )
        message = (
            message
            + f"""This information is derived from https://carbonintensity.org.uk/

{self.hashtags} #CarbonIntensity #Energy"""
        )

        # graph the forecast graph
        forecast = self.get_state(self.carbon_intensity_sensor, "forecast")
        df = pd.DataFrame(forecast)
        df["time"] = pd.to_datetime(df["from"])
        df["to"] = pd.to_datetime(df["to"])
        df = df.set_index("time")
        df["clean"] = df["intensity"] * df["optimal"]

        # Make most of the ticklabels empty so the labels don't get too crowded
        ticklabels = [""] * len(df.index)
        # Every 4th ticklable shows the month and day
        ticklabels[::4] = [item.strftime("%-I %p") for item in df.index[::4]]
        df["labels"] = ticklabels

        f1 = plt.figure()
        ax = df.plot(
            y="intensity",
            label="forecasted intensity (g/kWh)",
            xlabel="forecast period",
            ylabel="carbon intensity - g/kWh",
            title="Carbon intensity forecast for the next 48 hours",
        )
        ax = df.plot(
            y="clean", label="optimal time window", color="g", kind="area", ax=ax
        )
        ax.set_xticks(ticks=df.index, labels=df.labels, rotation=60)

        # ax.set_xticklabels([pandas_datetime.strftime("%-I %p") for pandas_datetime in df.index])
        grid_lines = intensity_indexes[datetime.now().year]
        ax.fill_between(
            df.index, 0, grid_lines[0], facecolor="darkgreen", alpha=0.3, zorder=-10
        )
        ax.fill_between(
            df.index,
            grid_lines[0],
            grid_lines[1],
            facecolor="lightgreen",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            df.index,
            grid_lines[1],
            grid_lines[2],
            facecolor="orange",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            df.index,
            grid_lines[2],
            grid_lines[3],
            facecolor="red",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            df.index, grid_lines[3], 1000, facecolor="darkred", alpha=0.3, zorder=-10
        )
        plt.margins(x=0, tight=True)
        plt.xlabel("48h forecast period")

        forecast_io = BytesIO()
        plt.savefig(forecast_io, format="png")

        # graph the historical graph
        client = self.get_influx_db_client()
        query = f"""
                from(bucket: \"{self.args["influx_creds"]["bucket"]}\")
                  |> range(start: -24h, stop: now())
                  |> filter(fn: (r) => r.entity_id == "carbon_intensity_uk_current")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """
        carbon_intensity = client.query_api().query_data_frame(query=query)
        carbon_intensity = carbon_intensity[["_time", "value"]].set_index("_time")

        # Make most of the ticklabels empty so the labels don't get too crowded
        ticklabels = [""] * len(carbon_intensity.index)
        # Every 4th ticklable shows the month and day
        ticklabels[::4] = [
            item.strftime("%-I %p") for item in carbon_intensity.index[::4]
        ]
        carbon_intensity["labels"] = ticklabels

        f2 = plt.figure()
        ax = carbon_intensity.plot(
            y="value",
            label="Carbon intensity (g/kWh)",
            xlabel="last 24 hours",
            ylabel="carbon intensity - g/kWh",
            title="Carbon intensity over the last 24 hours",
        )
        ax.set_xticks(
            ticks=carbon_intensity.index, labels=carbon_intensity.labels, rotation=60
        )

        ax.fill_between(
            carbon_intensity.index,
            0,
            grid_lines[0],
            facecolor="darkgreen",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            carbon_intensity.index,
            grid_lines[0],
            grid_lines[1],
            facecolor="lightgreen",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            carbon_intensity.index,
            grid_lines[1],
            grid_lines[2],
            facecolor="orange",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            carbon_intensity.index,
            grid_lines[2],
            grid_lines[3],
            facecolor="red",
            alpha=0.3,
            zorder=-10,
        )
        ax.fill_between(
            carbon_intensity.index,
            grid_lines[3],
            1000,
            facecolor="darkred",
            alpha=0.3,
            zorder=-10,
        )
        plt.margins(x=0, tight=True)
        plt.xlabel("last 24 hours")
        plt.ylim(bottom=0, top=carbon_intensity.value.max() * 1.15)

        history_io = BytesIO()
        plt.savefig(history_io, format="png")

        medias = [
            {
                "bytes": forecast_io,
                "file_name": "forecast.png",
                "mime_type": "image/png",
            },
            {"bytes": history_io, "file_name": "history.png", "mime_type": "image/png"},
        ]
        self.post_to_mastodon(message, medias=medias)

    def publish_tidal_cycle(self, entity, attribute, old, new, kwargs):
        """
        Publish the latest tidal cycle high low
        """
        # self.log(f"{entity} - {attribute} - '{old} - {new} - Kwargs:  {kwargs}")

        if not self.tidal_broadcast:
            return False

        client = self.get_influx_db_client()
        # tidal cycle is 12h and 15 minutes
        # if we have just peaked, the last 13 hours should therfore catch the low
        query = f"""
            from(bucket: \"{self.args["influx_creds"]["bucket"]}\")
              |> range(start: -13h, stop: now())
              |> filter(fn: (r) => r._measurement == \"m\" and r._field == \"value\")
              |> filter(fn: (r) => r.entity_id == \"{entity.split(".")[1]}\")
             |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")
        """
        water_level = client.query_api().query_data_frame(query=query)

        # High tide detection
        # Older state needs to be >= new state and <= one before it
        if (
            old["state"] != "unavailable"
            and new["state"] != "unavailable"
            and old["state"] >= new["state"]
        ):
            self.log("Tide is decreasing, checking if we have just passed high tide")

            # in case of duplicate value we take the last available measure
            old_index = water_level[
                water_level.value == float(old["state"])
            ].index.tolist()[-1]

            self.log(
                f'New: {new["state"]}, Old: {old["state"]}, One before old {water_level.iloc[old_index -1].value}'
            )
            self.log(water_level.iloc[range(old_index - 10, old_index)])
            if np.all(
                water_level.iloc[range(old_index - 10, old_index)].value
                <= float(old["state"])
            ):
                self.log("High Tide detected")
                last_hour = water_level[
                    water_level["_time"]
                    > datetime.now(timezone.utc) - timedelta(hours=1)
                ]
                high = last_hour[last_hour.value == last_hour.value.max()]

                local_tz = pytz.timezone(self.get_timezone())
                low = water_level[water_level.value == water_level.value.min()]
                message = f"""\
{self.disclaimer}[{high.iloc[0]._time.astimezone(local_tz).strftime("%H:%M %Z")}] \
High tide detected in {kwargs["sensor_label"]}: {high.iloc[0].value:0.2f}m. \
Previous low tide at {low.iloc[0]._time.astimezone(local_tz).strftime("%H:%M %Z")} with {low.value.to_numpy()[0]:0.2f}m

{self.hashtags} #Tide #Thames
"""

                self.post_to_mastodon(message)

    def publish_airplane_count(self, kwargs):
        """Toot about number of airplanes"""
        tz = datetime.now(timezone.utc).astimezone().tzinfo
        now = datetime.now(tz)

        end_period = datetime(year=now.year, month=now.month, day=now.day, hour=3)
        day = timedelta(days=1)
        second = timedelta(seconds=1)
        yesterday = end_period - day
        end_period = end_period - second

        client = self.get_influx_db_client()
        query = f"""
            from(bucket: \"{self.args["influx_creds"]["bucket"]}\")
              |> range(start: {yesterday.isoformat()}Z, stop: {end_period.isoformat()}Z)
              |> filter(fn: (r) => r.entity_id == "aircraft_monitor")
             |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """
        airplanes = client.query_api().query_data_frame(query=query)
        airplanes["diff"] = airplanes["count"] - airplanes["count"].shift(1)

        airplanes = airplanes[airplanes["diff"] == 1][["_time", "diff"]]

        if airplanes.shape[0] > 0:
            airplane_count = airplanes.shape[0]

            earliest = airplanes.iloc[0]["_time"]
            latest = airplanes.iloc[-1]["_time"]

            message = f"""\
{self.disclaimer} [{now.strftime("%H:%M %Z")}] \
{self.location} #PlaneSpotters

✈️✈️✈️ Yesterday {yesterday.strftime("%A %d %B %Y")}, we counted \
{airplane_count} low-flying aeroplanes. Earliest was \
at {earliest.strftime("%H:%M")} and the latest at {latest.strftime("%H:%M")}

#SW14 #EastSheen #Mortlake #RichomdUponThames #Richmond #Aeroplanes
"""
            self.post_to_mastodon(message)


def trend_indicator(old_read, new_read):
    """Return an arrow trend indicator to include in messages"""
    if float(new_read) > float(old_read):
        return "↗️"
    elif float(new_read) < float(old_read):
        return "↘️"
    else:
        return "➡️"


def split_chunks(s, chunksize):
    """Split long message to fit within the mastodon limit"""
    length = len(s)
    pos = 0
    while pos != -1:
        if (length - pos) < chunksize:
            yield s[pos:length]
            pos = -1
        else:
            new_pos = s.rfind(" ", pos, pos + chunksize)
            if new_pos == pos:
                new_pos += chunksize  # force split in word
            yield s[pos:new_pos]
            pos = new_pos
