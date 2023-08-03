import itertools
import json
import time
from collections import defaultdict, deque
from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
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
    FISSURE_TYPES = [FISSURE_TYPE_VOID_STORMS, FISSURE_TYPE_STEEL_PATH, FISSURE_TYPE_NORMAL]
    DISPLAY_TYPE_DISCORD = 'Discord'
    DISPLAY_TYPE_TIME_LEFT = 'Time Left'
    ERA_LITH = 'Lith'
    ERA_MESO = 'Meso'
    ERA_NEO = 'Neo'
    ERA_AXI = 'Axi'
    ERA_REQUIEM = 'Requiem'
    ERA_LIST = [ERA_LITH, ERA_MESO, ERA_NEO, ERA_AXI, ERA_REQUIEM]
    ERA_LIST_VOID_STORMS = [ERA_LITH, ERA_MESO, ERA_NEO, ERA_AXI]
    MAX_STORED_UPDATES = 10
    ALIASES = {'sp': FISSURE_TYPE_STEEL_PATH,
               'vs': FISSURE_TYPE_VOID_STORMS,
               'normal': FISSURE_TYPE_NORMAL,
               'n': FISSURE_TYPE_NORMAL,
               'steel': FISSURE_TYPE_STEEL_PATH,
               'void': FISSURE_TYPE_VOID_STORMS,
               'voidstorm': FISSURE_TYPE_VOID_STORMS,
               'voidstorms': FISSURE_TYPE_VOID_STORMS,
               'void storm': FISSURE_TYPE_VOID_STORMS,
               'void storms': FISSURE_TYPE_VOID_STORMS,
               'rj': FISSURE_TYPE_VOID_STORMS,
               'railjack': FISSURE_TYPE_VOID_STORMS,
               'rail jack': FISSURE_TYPE_VOID_STORMS}

    def __init__(self):
        self.fissure_lists = {
            self.FISSURE_TYPE_VOID_STORMS: [],
            self.FISSURE_TYPE_STEEL_PATH: [],
            self.FISSURE_TYPE_NORMAL: [],
        }
        self.update_log = deque(maxlen=self.MAX_STORED_UPDATES)
        self.last_update = None

    def get_fields(self, fissures: List[Fissure], field_formats: List[Tuple[str, str]],
                   display_type: str = DISPLAY_TYPE_TIME_LEFT, emoji_dict: Dict[str, str] = None):
        """
        Function to retrieve specified fields from a list of Fissure objects.

        Args:
            fissures (list): List of fissures.
            field_formats (list): List of tuples. Each tuple contains a field name and a format string.
            display_type (str): Type of display for the expiry field.
            emoji_dict (dict): A dictionary mapping eras to emojis.

        Returns:
            dict: A dictionary where each key is a field name and the corresponding value is a list of all values for that field from the list of Fissures, formatted according to the format string.
        """

        preprocess_functions = {
            "{era}": lambda fissure: f"{emoji_dict.get(fissure.era, '') if emoji_dict else ''} {fissure.era}",
            "{expiry}": lambda fissure: self.format_time_remaining(fissure.expiry, display_type)
        }

        def preprocess_format_string(format_string, fissure):
            for key, preprocess in preprocess_functions.items():
                if key in format_string:
                    format_string = format_string.replace(key, preprocess(fissure))
            return format_string

        return {
            field_name: [
                preprocess_format_string(format_string, fissure).format(**fissure.__dict__)
                for fissure in fissures
            ]
            for field_name, format_string in field_formats
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
        return datetime.fromtimestamp(int(fissure['Expiry']['$date']['$numberLong']) // 1000)

    def add_fissure(self, fissure, fissure_type):
        self.fissure_lists[fissure_type].append(fissure)

    def clear_fissure_lists(self):
        for fissure_list in self.fissure_lists.values():
            fissure_list.clear()

    async def build_fissure_list(self):
        old_fissure_lists = deepcopy(self.fissure_lists)
        self.clear_fissure_lists()
        world_state = await self.get_world_state()

        for fissure in world_state["ActiveMissions"] + world_state['VoidStorms']:
            fissure_type = self.classify_fissure_type(fissure)
            expiry = self.get_expiry_datetime(fissure)
            mission, location, planet, tileset, enemy = self.get_node_data(fissure['Node'])
            era, tier = self.get_fissure_data(fissure, fissure_type, mission)

            fissure = Fissure(location, mission, planet, tileset, enemy, era, tier, expiry, fissure_type)
            self.add_fissure(fissure, fissure_type)

        for fissure_list in self.fissure_lists.values():
            fissure_list.sort(key=lambda fissure: (SORT_ORDER[fissure.era], fissure.expiry))

        new_fissures = []
        if self.last_update is not None:
            for fissure_type in self.fissure_lists:
                new_fissures += [fissure for fissure in self.fissure_lists[fissure_type]
                                 if fissure not in old_fissure_lists[fissure_type]]

        timestamp = datetime.now()
        self.last_update = timestamp

        if new_fissures:
            self.update_log.append((timestamp, new_fissures))

        return new_fissures

    def get_updates_since(self, client_timestamp):
        return self.last_update, [fissure for (timestamp, fissures) in self.update_log
                                  if timestamp > client_timestamp for fissure in fissures]

    def get_soonest_expiry(self):
        soonest_expiries = defaultdict(dict)

        for fissure_type, fissures in self.fissure_lists.items():
            for era, group in itertools.groupby(fissures, key=lambda fissure: fissure.era):
                soonest_expiries[fissure_type][era] = next(group).expiry

        return soonest_expiries

    def get_era_list(self, fissure_type: str = FISSURE_TYPE_NORMAL):
        if fissure_type == self.FISSURE_TYPE_VOID_STORMS:
            return self.ERA_LIST_VOID_STORMS

        return self.ERA_LIST

    def get_last_expiry(self, era_list: List[str] = None):
        if era_list is None:
            era_list = self.get_era_list()

        last_expiries = defaultdict(dict)

        for fissure_type, fissures in self.fissure_lists.items():
            # Pre-filter fissures based on era_list
            fissures = filter(lambda fissure: fissure.era in era_list, fissures)
            for era, group in itertools.groupby(fissures, key=lambda fissure: fissure.era):
                sorted_expiries = sorted(group, key=lambda fissure: fissure.expiry, reverse=True)
                last_expiries[fissure_type][era] = sorted_expiries[0].expiry - timedelta(minutes=3)

        return last_expiries

    def format_time_remaining(self, expiry: datetime, display_type: str = DISPLAY_TYPE_TIME_LEFT):
        expiry_timestamp = expiry.timestamp()
        if display_type == self.DISPLAY_TYPE_TIME_LEFT:
            now = datetime.now()
            time_remaining = expiry - now

            hours, remainder = divmod(time_remaining.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            # Return time remaining and prepend it with " in "
            return f"in {f'{int(hours)} hour ' if hours > 0 else ''}{int(minutes)} minute{'' if minutes == 1 else 's'}"
        elif display_type == self.DISPLAY_TYPE_DISCORD:
            return f"<t:{int(expiry_timestamp)}:R>"

    def get_resets(self, fissure_type: str = FISSURE_TYPE_NORMAL,
                   display_type: str = DISPLAY_TYPE_TIME_LEFT,
                   emoji_dict: Dict[str, str] = None,
                   era_list: List[str] = None):
        if era_list is None:
            era_list = self.get_era_list()

        if fissure_type not in self.FISSURE_TYPES:
            raise ValueError(f"Invalid fissure type: {fissure_type}")

        last_expiries = self.get_last_expiry(era_list)

        # If emoji_dict is None, use a "do-nothing" lambda function as default
        get_emoji = (lambda x: emoji_dict.get(x, x)) if emoji_dict else (lambda x: x)

        expiries = []

        # Find the soonest expiry and append it to expiries list
        soonest_expiry = min(last_expiries[fissure_type].items(), key=lambda x: x[1])
        next_reset_string = (f"Next reset is {soonest_expiry[0]} "
                             f"{self.format_time_remaining(soonest_expiry[1], display_type=display_type)}")
        expiries.append(next_reset_string)

        expiries += [f"{get_emoji(era)} {self.format_time_remaining(expiry, display_type=display_type)}"
                     for era, expiry in last_expiries[fissure_type].items()]

        return expiries

    def get_fissures(self, fissure_type: str, **kwargs) -> List[Fissure]:
        """
        Returns a list of Fissure objects filtered by fissure_type and additional keyword arguments.

        Args:
            fissure_type (str): Type of the fissure. Can be 'Normal', 'Steel Path', or 'Void Storms'.
            **kwargs: Filtering options. Can include era, node, mission, planet, tileset, and tier.
                      Each argument can be a string or a list of strings.
                      For example, era='Lith', node=['Node1', 'Node2'], mission='SomeMission'.

        Raises:
            ValueError: If the fissure_type or era is invalid.

        Returns:
            List[Fissure]: A list of Fissure objects that meet all the specified conditions.
        """
        if fissure_type not in self.FISSURE_TYPES:
            raise ValueError(f"Invalid fissure type: {fissure_type}")

        # Filter out fissures that match all non-None conditions
        fissures = [fissure for fissure in self.fissure_lists[fissure_type]
                    if all(self._filter_condition(fissure, attr, value)
                           for attr, value in kwargs.items() if value is not None)]

        return fissures

    @staticmethod
    def _ensure_list(value):
        """Ensures that the value is a list. If not, converts it into a list with a single element."""
        return value if isinstance(value, list) else [value]

    def _filter_condition(self, fissure, attribute, value):
        """Checks whether the fissure attribute matches the value (or one of the values if it's a list)."""
        actual_value = getattr(fissure, attribute)
        expected_values = self._ensure_list(value)

        return actual_value in expected_values
