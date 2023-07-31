import itertools
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict
from urllib.request import urlopen

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

from .common import fissure_parser, sol_nodes, fissure_types, SORT_ORDER, logger


@dataclass
class Fissure:
    node: str
    mission: str
    planet: str
    tileset: str
    enemy: str
    era: str
    tier: int
    expiry: datetime
    fissure_type: str


class FissureEngine:
    FISSURE_TYPE_VOID_STORMS = 'Void Storms'
    FISSURE_TYPE_STEEL_PATH = 'Steel Path'
    FISSURE_TYPE_NORMAL = 'Normal'
    DISPLAY_TYPE_DISCORD = 'Discord'
    DISPLAY_TYPE_TIME_LEFT = 'Time Left'

    def __init__(self):
        self.fissure_lists = {
            self.FISSURE_TYPE_VOID_STORMS: [],
            self.FISSURE_TYPE_STEEL_PATH: [],
            self.FISSURE_TYPE_NORMAL: [],
        }

    @staticmethod
    def parse_fissure(type, data, data2="N/A"):
        if isinstance(fissure_parser[type][data], str) or isinstance(fissure_parser[type][data], int):
            return fissure_parser[type][data]
        else:
            return fissure_parser[type][data][data2]

    @staticmethod
    async def get_world_state():
        """
        Asynchronously fetch data from the given URL.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use for making the request.
        Returns:
            dict: The JSON data fetched from the URL.

        Raises:
            aiohttp.ClientResponseError: If the request to the URL results in an HTTP error.
        """

        @retry(stop=stop_after_attempt(5), wait=wait_exponential(max=60))
        async def make_request():
            async with session.get("http://content.warframe.com/dynamic/worldState.php") as res:
                res.raise_for_status()
                logger.debug(f"Fetched data for http://content.warframe.com/dynamic/worldState.php")
                return await res.text()

        # Makes the API request, retrying up to 5 times if it fails, waiting 1 second between each attempt
        async with aiohttp.ClientSession() as session:
            data = await make_request()

        return json.loads(data)

    @staticmethod
    def get_node_data(node_name):
        node = sol_nodes[node_name]
        if node_name in fissure_parser['mission_overrides']:
            mission = fissure_parser['mission_overrides'][node_name]
        else:
            mission = node['type']

        if 'tileset' not in node:
            tileset = "Space"
        else:
            tileset = node['tileset']

        return mission, node['node'], node['planet'], tileset, node['enemy']

    @staticmethod
    def get_fissure_data(fissure, fissure_type, mission):
        era = fissure_parser['era'][fissure[fissure_parser["era_key"][fissure_type]]]
        tier = fissure_parser['tier'][mission]

        return era, tier

    def classify_fissure_type(self, fissure):
        if 'ActiveMissionTier' in fissure:
            return self.FISSURE_TYPE_VOID_STORMS
        elif 'Hard' in fissure:
            return self.FISSURE_TYPE_STEEL_PATH
        else:
            return self.FISSURE_TYPE_NORMAL

    @staticmethod
    def get_expiry_datetime(fissure):
        return datetime.utcfromtimestamp(int(fissure['Expiry']['$date']['$numberLong']) // 1000)

    def add_fissure(self, fissure, fissure_type):
        self.fissure_lists[fissure_type].append(fissure)

    async def build_fissure_list(self):
        world_state = await self.get_world_state()

        fissures = []
        for fissure in world_state["ActiveMissions"] + world_state['VoidStorms']:
            fissure_type = self.classify_fissure_type(fissure)
            expiry = self.get_expiry_datetime(fissure)
            mission, location, planet, tileset, enemy = self.get_node_data(fissure['Node'])
            era, tier = self.get_fissure_data(fissure, fissure_type, mission)

            fissure = Fissure(location, mission, planet, tileset, enemy, era, tier, expiry, fissure_type)
            self.add_fissure(fissure, fissure_type)

        for fissure_list in self.fissure_lists.values():
            fissure_list.sort(key=lambda fissure: (SORT_ORDER[fissure.era], fissure.expiry))

    def get_soonest_expiry(self):
        soonest_expiries = defaultdict(dict)

        for fissure_type, fissures in self.fissure_lists.items():
            for era, group in itertools.groupby(fissures, key=lambda fissure: fissure.era):
                soonest_expiries[fissure_type][era] = next(group).expiry

        return soonest_expiries

    def get_last_expiry(self):
        last_expiries = defaultdict(dict)

        for fissure_type, fissures in self.fissure_lists.items():
            for era, group in itertools.groupby(fissures, key=lambda fissure: fissure.era):
                sorted_expiries = sorted(group, key=lambda fissure: fissure.expiry, reverse=True)
                last_expiries[fissure_type][era] = sorted_expiries[0].expiry - timedelta(minutes=3)

        return last_expiries

    def format_time_remaining(self, expiry: datetime, display_type: str = DISPLAY_TYPE_TIME_LEFT):
        expiry_timestamp = expiry.timestamp()
        if display_type == self.DISPLAY_TYPE_TIME_LEFT:
            now = datetime.utcnow()
            time_remaining = expiry - now

            hours, remainder = divmod(time_remaining.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            # Return time remaining and prepend it with " in "
            return f"in {f'{int(hours)} hour ' if hours > 0 else ''}{int(minutes)} min"
        elif display_type == self.DISPLAY_TYPE_DISCORD:
            return f"<t:{int(expiry_timestamp)}:R>"

    def get_reset_string(self, fissure_type: str = FISSURE_TYPE_NORMAL,
                         display_type: str = DISPLAY_TYPE_TIME_LEFT,
                         emoji_dict: Dict[str, str] = None):
        last_expiries = self.get_last_expiry()

        # If emoji_dict is None, use a "do-nothing" lambda function as default
        get_emoji = (lambda x: emoji_dict.get(x, x)) if emoji_dict else (lambda x: x)

        expiries = [f"{get_emoji(era)} {self.format_time_remaining(expiry, display_type=display_type)}"
                    for era, expiry in last_expiries[fissure_type].items()]

        return '\n'.join(expiries)

