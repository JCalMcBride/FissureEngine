import itertools
import json
import time
from collections import defaultdict, deque, OrderedDict
from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Union
from urllib.request import urlopen
from ordered_set import OrderedSet


import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

from .common import fissure_parser, sol_nodes, fissure_types, SORT_ORDER, logger


class BoundedOrderedSet:
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.set = OrderedSet()

    def add(self, item):
        if item not in self.set:
            if len(self.set) >= self.maxsize:
                self.set.pop(0)  # Remove the oldest item
            self.set.add(item)

    def __contains__(self, item):
        return item in self.set


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
    activation: datetime
    duration: timedelta

    @property
    def time_left(self):
        now = datetime.now()
        time_remaining = self.expiry - now
        return int(time_remaining.total_seconds())

    def __eq__(self, other):
        if isinstance(other, Fissure):
            return (self.node == other.node and
                    self.mission == other.mission and
                    self.planet == other.planet and
                    self.tileset == other.tileset and
                    self.enemy == other.enemy and
                    self.era == other.era and
                    self.tier == other.tier and
                    self.expiry == other.expiry and
                    self.fissure_type == other.fissure_type and
                    self.activation == other.activation and
                    self.duration == other.duration)
        return False

    def __hash__(self):
        return hash((self.node, self.mission, self.planet, self.tileset, self.enemy,
                     self.era, self.tier, self.expiry, self.fissure_type, self.activation, self.duration))

def get_fissure_type_identifier(fissure_type, image_dict):
    if fissure_type != FissureEngine.FISSURE_TYPE_NORMAL:
        short_identifier = ''.join(word[0] for word in fissure_type.split())
        return f"{image_dict.get(short_identifier, short_identifier) if image_dict else short_identifier} "
    return ""


class FissureEngine:
    FISSURE_TYPE_VOID_STORMS = 'Void Storms'
    FISSURE_TYPE_STEEL_PATH = 'Steel Path'
    FISSURE_TYPE_NORMAL = 'Normal'
    FISSURE_TYPES = [FISSURE_TYPE_VOID_STORMS, FISSURE_TYPE_STEEL_PATH, FISSURE_TYPE_NORMAL]
    DISPLAY_TYPE_DISCORD = 'Discord'
    DISPLAY_TYPE_TIME_LEFT = 'Time Left'
    DISPLAY_TYPE_TIMESTAMP = 'Timestamp'  # Added new display type
    ERA_LITH = 'Lith'
    ERA_MESO = 'Meso'
    ERA_NEO = 'Neo'
    ERA_AXI = 'Axi'
    ERA_REQUIEM = 'Requiem'
    ERA_OMNIA = 'Omnia'
    ERA_LIST = [ERA_LITH, ERA_MESO, ERA_NEO, ERA_AXI, ERA_REQUIEM, ERA_OMNIA]
    ERA_LIST_VOID_STORMS = [ERA_LITH, ERA_MESO, ERA_NEO, ERA_AXI]
    MAX_STORED_UPDATES = 10
    MAX_SEEN_FISSURES = 1000  # Maximum number of seen fissures to store in the cache
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
        self.seen_fissures = BoundedOrderedSet(maxsize=self.MAX_SEEN_FISSURES)
        self.update_log = deque(maxlen=self.MAX_STORED_UPDATES)
        self.last_update = None

    def get_fields(self, fissures: List[Fissure], field_formats: List[Tuple[str, str]],
                   display_type: str = DISPLAY_TYPE_TIME_LEFT, image_dict: Dict[str, str] = None):
        """
        Function to retrieve specified fields from a list of Fissure objects.

        Args:
            fissures (list): List of fissures.
            field_formats (list): List of tuples. Each tuple contains a field name and a format string.
            display_type (str): Type of display for the expiry field.
            image_dict (dict): A dictionary mapping fissure type short identifiers and eras to images or emojis.

        Returns:
            dict: A dictionary where each key is a field name and the corresponding value is a list of all values for that field from the list of Fissures, formatted according to the format string.
        """
        preprocess_functions = {
            "{era}": lambda
                fissure: f"{get_fissure_type_identifier(fissure.fissure_type, image_dict)}{f'{image_dict.get(fissure.era, '')} ' if image_dict else ''}{fissure.era}",
            "{expiry}": lambda fissure: self.format_time_remaining(fissure.expiry, display_type)
        }

        def preprocess_format_string(format_string, fissure):
            for key, preprocess in preprocess_functions.items():
                if key in format_string:
                    format_string = format_string.replace(key, preprocess(fissure))
            return format_string

        # Sort fissures by fissure type in the order of normal, steel path, void storms
        sorted_fissures = sorted(fissures, key=lambda f: (f.fissure_type != self.FISSURE_TYPE_NORMAL,
                                                          f.fissure_type == self.FISSURE_TYPE_STEEL_PATH,
                                                          f.fissure_type == self.FISSURE_TYPE_VOID_STORMS))

        result = {}
        for field_name, _ in field_formats:
            result[field_name] = [[]]  # Initialize with a list that contains an empty list for the first fissure type

        prev_fissure_type = None

        for fissure in sorted_fissures:
            if fissure.fissure_type != prev_fissure_type:
                if prev_fissure_type is not None:
                    for field in result.values():
                        field.append([])  # Start a new list for the new fissure type
                prev_fissure_type = fissure.fissure_type

            for field_name, format_string in field_formats:
                formatted_value = preprocess_format_string(format_string, fissure).format(**fissure.__dict__)
                result[field_name][-1].append(formatted_value)  # Append to the last list for the current fissure type

        return result

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

        if f"{fissure_type} {mission}" in fissure_parser['tier']:
            tier = fissure_parser['tier'][f"{fissure_type} {mission}"]
        else:
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

    @staticmethod
    def get_activation_datetime(fissure):
        return datetime.fromtimestamp(int(fissure['Activation']['$date']['$numberLong']) // 1000)

    def add_fissure(self, fissure, fissure_type):
        self.fissure_lists[fissure_type].append(fissure)

    def clear_fissure_lists(self):
        for fissure_list in self.fissure_lists.values():
            fissure_list.clear()

    async def build_fissure_list(self):
        old_fissures = deepcopy(self.fissure_lists)
        world_state = await self.get_world_state()
        self.clear_fissure_lists()
        new_fissures = []
        changed_fissure_types = []

        for fissure in world_state["ActiveMissions"] + world_state['VoidStorms']:
            fissure_type = self.classify_fissure_type(fissure)
            expiry = self.get_expiry_datetime(fissure)
            activation = self.get_activation_datetime(fissure)
            duration = expiry - activation

            if expiry < datetime.now():
                continue

            mission, location, planet, tileset, enemy = self.get_node_data(fissure['Node'])
            era, tier = self.get_fissure_data(fissure, fissure_type, mission)

            fissure_obj = Fissure(location, mission, planet, tileset, enemy, era, tier, expiry, fissure_type, activation, duration)

            self.add_fissure(fissure_obj, fissure_type)

            fissure_hash = hash(fissure_obj)
            if fissure_hash not in self.seen_fissures:
                new_fissures.append(fissure_obj)
                self.seen_fissures.add(fissure_hash)

        for fissure_type, fissure_list in self.fissure_lists.items():
            fissure_list.sort(key=lambda fissure: (SORT_ORDER[fissure.era], fissure.expiry))

        if self.last_update is not None:
            for fissure_type, fissure_list in self.fissure_lists.items():
                old_fissure_hashes = set(hash(fissure) for fissure in old_fissures[fissure_type])
                new_fissure_hashes = set(hash(fissure) for fissure in fissure_list)
                if old_fissure_hashes != new_fissure_hashes:
                    changed_fissure_types.append(fissure_type)

        if self.last_update is None:
            new_fissures.clear()

        timestamp = datetime.now()
        self.last_update = timestamp

        if new_fissures:
            self.update_log.append((timestamp, new_fissures))

        return new_fissures, changed_fissure_types

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
        elif display_type == self.DISPLAY_TYPE_TIMESTAMP:  # Added new display type handling
            return f"{int(expiry_timestamp)}"

    def get_single_reset(self, fissure_type: str = FISSURE_TYPE_NORMAL,
                         display_type: str = DISPLAY_TYPE_TIME_LEFT,
                         image_dict: Dict[str, str] = None,
                         era_list: List[str] = None):
        if era_list is None:
            era_list = self.get_era_list()

        if fissure_type not in self.FISSURE_TYPES:
            raise ValueError(f"Invalid fissure type: {fissure_type}")

        last_expiries = self.get_last_expiry(era_list)

        # If image_dict is None, use a "do-nothing" lambda function as default
        get_image = (lambda x: image_dict.get(x, x)) if image_dict else (lambda x: x)

        expiries = []

        # Find the soonest expiry and append it to expiries list
        try:
            soonest_expiry = min(last_expiries[fissure_type].items(), key=lambda x: x[1])
            next_reset_string = (f"Next reset: {soonest_expiry[0]} "
                                 f"{self.format_time_remaining(soonest_expiry[1], display_type=display_type)}")
        except ValueError:
            next_reset_string = "No fissures found."

        expiries.append(next_reset_string)

        expiries += [f"{get_image(era)} {self.format_time_remaining(expiry, display_type=display_type)}"
                     for era, expiry in last_expiries[fissure_type].items()]

        return expiries


    def get_resets(self, fissure_type: Union[str, List[str]] = FISSURE_TYPE_NORMAL,
                   display_type: str = DISPLAY_TYPE_TIME_LEFT,
                   image_dict: Dict[str, str] = None,
                   era_list: List[str] = None):
        if isinstance(fissure_type, str):
            return self.get_single_reset(fissure_type, display_type, image_dict, era_list)
        elif isinstance(fissure_type, list):
            return {ft: self.get_single_reset(ft, display_type, image_dict, era_list) for ft in fissure_type}
        else:
            raise ValueError(f"Invalid fissure type: {fissure_type}")

    def get_fissures(self, **kwargs) -> List[Fissure]:
        """
        Returns a list of Fissure objects filtered by keyword arguments.

        Args:
            **kwargs: Filtering options. Can include fissure_type, era, node, mission, planet, tileset, and tier.
                      Each argument can be a single value or a list of values of any type.
                      To blacklist a value, prefix the value with a "-".
                      For example, fissure_type=['Normal', 'Steel Path'], era='Lith', node=['-Node1', 'Node2'], mission='SomeMission'.

        Returns:
            List[Fissure]: A list of Fissure objects that meet all the specified conditions.
        """

        # Filter out fissures that match all non-None conditions
        fissures = [fissure for fissure_type in self.FISSURE_TYPES for fissure in self.fissure_lists[fissure_type]
                    if all(self._filter_condition(fissure, attr, value)
                           for attr, value in kwargs.items() if value is not None)]

        return fissures

    @staticmethod
    def _ensure_list(value):
        """Ensures that the value is a list. If not, converts it into a list with a single element."""
        return value if isinstance(value, list) else [value]

    def _filter_condition(self, fissure, attribute, values):
        """
        Checks whether the fissure attribute matches the specified values.
        Values prefixed with a "-" are treated as blacklist conditions.
        """
        actual_value = getattr(fissure, attribute)
        expected_values = self._ensure_list(values)

        blacklist_values = [self._strip_prefix(value) for value in expected_values if self._is_blacklist(value)]
        whitelist_values = [value for value in expected_values if not self._is_blacklist(value)]

        if blacklist_values and actual_value in blacklist_values:
            return False
        elif whitelist_values and actual_value not in whitelist_values:
            return False
        else:
            return True

    @staticmethod
    def _is_blacklist(value):
        """Checks if a value is a blacklist condition."""
        return isinstance(value, str) and value.startswith('-')

    @staticmethod
    def _strip_prefix(value):
        """Removes the blacklist prefix ("-") from a value if it exists."""
        return value.lstrip('-') if isinstance(value, str) else value

