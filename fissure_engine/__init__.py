import json
import time
from urllib.request import urlopen

from .common import fissure_parser, sol_nodes, fissure_types, SORT_ORDER

def parse_fissure(type, data, data2="N/A"):
    if isinstance(fissure_parser[type][data], str) or isinstance(fissure_parser[type][data], int):
        return fissure_parser[type][data]
    else:
        return fissure_parser[type][data][data2]

def get_world_state():
    return json.load(urlopen("http://content.warframe.com/dynamic/worldState.php"))


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


def get_fissure_data(fissure, fissure_type, mission):
    era = fissure_parser['era'][fissure[fissure_parser["era_key"][fissure_type]]]
    tier = fissure_parser['tier'][mission]

    return era, tier


def build_fissure_list(world_state=None):
    if world_state is None:
        world_state = get_world_state()

    fissures = {}
    resets = {}
    refresh_time = {}
    for fissure_type in fissure_types:
        fissures[fissure_type] = []
        resets[fissure_type] = {}
        refresh_time[fissure_type] = []

    next_fetch_time = []
    fissure_type = None
    expiry = None
    era = None
    mission = None
    location = None
    planet = None

    current_time = time.time()
    for fissure in world_state["ActiveMissions"] + world_state['VoidStorms']:
        if 'ActiveMissionTier' in fissure:
            fissure_type = 'vs'
        elif 'Hard' in fissure:
            fissure_type = 'sp'
        else:
            fissure_type = 'normal'

        expiry = int(fissure['Expiry']['$date']['$numberLong']) // 1000
        if time.time() > expiry:
            continue

        mission, location, planet, tileset, enemy = get_node_data(fissure['Node'])
        era, tier = get_fissure_data(fissure, fissure_type, mission)

        if era == "Requiem" and fissure_type == 'normal':
            fissure_type = "requiem"

        resets[fissure_type].setdefault(era, []).append(expiry)
        fissures[fissure_type].append(
            {"mission": mission,
             "location": location,
             "planet": planet,
             "expiry": expiry,
             "tier": tier,
             'era': era})

    for fissure_type in fissures:
        fissures[fissure_type] = sorted(fissures[fissure_type], key=lambda val: SORT_ORDER[val['era']])

    for fissure_type in resets:
        if fissure_type != 'vs':
            type_modifier = 180
        else:
            type_modifier = 1920

        for era in resets[fissure_type]:
            era_resets = resets[fissure_type][era]

            refresh_time[fissure_type].append(min(era_resets))
            next_fetch_time.append(max(era_resets) - 200)

            resets[fissure_type][era] = max(era_resets) - type_modifier

        refresh_time[fissure_type] = min(refresh_time[fissure_type])

        resets[fissure_type] = dict(sorted(resets[fissure_type].items(), key=lambda val: SORT_ORDER[val[0]]))

    return fissures, resets, min(next_fetch_time), refresh_time


def get_fissures(fissure_list=None, resets=None, *, fissure_type='normal', era=None, tier=5):
    if fissure_list is None or resets is None:
        fissure_list, resets, _, _ = build_fissure_list()

    fissure_list = fissure_list[fissure_type]
    resets = resets[fissure_type]

    if era is not None:
        if isinstance(era, str):
            era = [era.title()]
        elif isinstance(era, list):
            era = [x.title() for x in era]

        fissure_list = [x for x in fissure_list if x['era'] in era and x['tier'] <= tier]
        resets = {k: v for k, v in resets.items() if k in era}
    elif tier < 5:
        fissure_list = [x for x in fissure_list if x['tier'] <= tier]

    min_era = min(resets, key=resets.get)
    min_reset_time = resets[min_era]

    return fissure_list, resets, min_era, min_reset_time
