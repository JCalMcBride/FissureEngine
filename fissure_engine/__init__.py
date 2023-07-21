import json
import time
from pprint import pprint
from urllib.request import urlopen


def parse_fissure(type, data, data2="N/A"):
    if isinstance(fissure_parser[type][data], str) or isinstance(fissure_parser[type][data], int):
        return fissure_parser[type][data]
    else:
        return fissure_parser[type][data][data2]


sol_nodes = {
    "SolNode0": {
        "node": "SolNode0",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "SolNode1": {
        "node": "Galatea",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Outpost"
    },
    "SolNode2": {
        "node": "Aphrodite",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Outpost"
    },
    "SolNode3": {
        "node": "Cordelia",
        "planet": "Uranus",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "SolNode4": {
        "node": "Acheron",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Ship"
    },
    "SolNode5": {
        "node": "Perdita",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode6": {
        "node": "Despina",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Excavation",
        "tileset": "Corpus Outpost"
    },
    "SolNode7": {
        "node": "Epimetheus",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode8": {
        "node": "Nix",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode9": {
        "node": "Rosalind",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Sealab"
    },
    "SolNode10": {
        "node": "Thebe",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Gas City"
    },
    "SolNode11": {
        "node": "Tharsis",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Settlement"
    },
    "SolNode12": {
        "node": "Elion",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Asteroid"
    },
    "SolNode13": {
        "node": "Bianca",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode14": {
        "node": "Ultor",
        "planet": "Mars",
        "enemy": "Crossfire",
        "type": "Exterminate",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode15": {
        "node": "Pacific",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Galleon"
    },
    "SolNode16": {
        "node": "Augustus",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Excavation",
        "tileset": "Grineer Settlement"
    },
    "SolNode17": {
        "node": "Proteus",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Ship"
    },
    "SolNode18": {
        "node": "Rhea",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Asteroid"
    },
    "SolNode19": {
        "node": "Enceladus",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Asteroid"
    },
    "SolNode20": {
        "node": "Telesto",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Galleon"
    },
    "SolNode21": {
        "node": "Narcissus",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Outpost"
    },
    "SolNode22": {
        "node": "Tessera",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Outpost"
    },
    "SolNode23": {
        "node": "Cytherean",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Interception",
        "tileset": "Corpus Ship"
    },
    "SolNode24": {
        "node": "Oro",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Assassination",
        "tileset": "Grineer Forest"
    },
    "SolNode25": {
        "node": "Callisto",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Interception",
        "tileset": "Corpus Gas City"
    },
    "SolNode26": {
        "node": "Lith",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Forest"
    },
    "SolNode27": {
        "node": "E Prime",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Forest"
    },
    "SolNode28": {
        "node": "M Prime",
        "planet": "Mercury",
        "enemy": "Crossfire",
        "type": "Exterminate",
        "tileset": "Grineer Asteroid"
    },
    "SolNode29": {
        "node": "Oberon",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode30": {
        "node": "Olympus",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Disruption",
        "tileset": "Grineer Settlement"
    },
    "SolNode31": {
        "node": "Anthe",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Galleon"
    },
    "SolNode32": {
        "node": "Tethys",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Assassination",
        "tileset": "Grineer Galleon"
    },
    "SolNode33": {
        "node": "Ariel",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Sealab"
    },
    "SolNode34": {
        "node": "Sycorax",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Sealab"
    },
    "SolNode35": {
        "node": "Arcadia",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode36": {
        "node": "Martialis",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Settlement"
    },
    "SolNode37": {
        "node": "Pallene",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode38": {
        "node": "Minthe",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Outpost"
    },
    "SolNode39": {
        "node": "Everest",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Excavation",
        "tileset": "Grineer Forest"
    },
    "SolNode40": {
        "node": "Prospero",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode41": {
        "node": "Arval",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Settlement"
    },
    "SolNode42": {
        "node": "Helene",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Galleon"
    },
    "SolNode43": {
        "node": "Cerberus",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Interception",
        "tileset": "Corpus Outpost"
    },
    "SolNode44": {
        "node": "Mimas",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode45": {
        "node": "Ara",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Settlement"
    },
    "SolNode46": {
        "node": "Spear",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Settlement"
    },
    "SolNode47": {
        "node": "Janus",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode48": {
        "node": "Regna",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Outpost"
    },
    "SolNode49": {
        "node": "Larissa",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Ship"
    },
    "SolNode50": {
        "node": "Numa",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Asteroid"
    },
    "SolNode51": {
        "node": "Hades",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Assassination",
        "tileset": "Corpus Outpost"
    },
    "SolNode52": {
        "node": "Portia",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode53": {
        "node": "Themisto",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Assassination",
        "tileset": "Corpus Gas City"
    },
    "SolNode54": {
        "node": "Silvanus",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode55": {
        "node": "Methone",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode56": {
        "node": "Cypress",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Ship"
    },
    "SolNode57": {
        "node": "Sao",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Outpost"
    },
    "SolNode58": {
        "node": "Hellas",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Settlement"
    },
    "SolNode59": {
        "node": "Eurasia",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Forest"
    },
    "SolNode60": {
        "node": "Caliban",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Sealab"
    },
    "SolNode61": {
        "node": "Ishtar",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Ship"
    },
    "SolNode62": {
        "node": "Neso",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode63": {
        "node": "Mantle",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Forest"
    },
    "SolNode64": {
        "node": "Umbriel",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Sealab"
    },
    "SolNode65": {
        "node": "Gradivus",
        "planet": "Mars",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Ship"
    },
    "SolNode66": {
        "node": "Unda",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Outpost"
    },
    "SolNode67": {
        "node": "Dione",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Asteroid"
    },
    "SolNode68": {
        "node": "Vallis",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Galleon"
    },
    "SolNode69": {
        "node": "Ophelia",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Survival",
        "tileset": "Grineer Sealab"
    },
    "SolNode70": {
        "node": "Cassini",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Galleon"
    },
    "SolNode71": {
        "node": "Vesper",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Spy"
    },
    "SolNode72": {
        "node": "Outer Terminus",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Outpost"
    },
    "SolNode73": {
        "node": "Ananke",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Gas City"
    },
    "SolNode74": {
        "node": "Carme",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Gas City"
    },
    "SolNode75": {
        "node": "Cervantes",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Forest"
    },
    "SolNode76": {
        "node": "Hydra",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Ship"
    },
    "SolNode77": {
        "node": "Cupid",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode78": {
        "node": "Triton",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Ship"
    },
    "SolNode79": {
        "node": "Cambria",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Forest"
    },
    "SolNode80": {
        "node": "Phoebe",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode81": {
        "node": "Palus",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Survival",
        "tileset": "Corpus Ship"
    },
    "SolNode82": {
        "node": "Calypso",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Galleon"
    },
    "SolNode83": {
        "node": "Cressida",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Sealab"
    },
    "SolNode84": {
        "node": "Nereid",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Outpost"
    },
    "SolNode85": {
        "node": "Gaia",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Forest"
    },
    "SolNode86": {
        "node": "Aegaeon",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode87": {
        "node": "Ganymede",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Gas City"
    },
    "SolNode88": {
        "node": "Adrastea",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Gas City"
    },
    "SolNode89": {
        "node": "Mariana",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Sealab"
    },
    "SolNode90": {
        "node": "Miranda",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "SolNode91": {
        "node": "Iapetus",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode92": {
        "node": "Charon",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode93": {
        "node": "Keeler",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Asteroid"
    },
    "SolNode94": {
        "node": "Apollodorus",
        "planet": "Mercury",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Galleon"
    },
    "SolNode95": {
        "node": "Thalassa",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode96": {
        "node": "Titan",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Survival",
        "tileset": "Grineer Galleon"
    },
    "SolNode97": {
        "node": "Amalthea",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Gas City"
    },
    "SolNode98": {
        "node": "Desdemona",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Sealab"
    },
    "SolNode99": {
        "node": "War",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Assassinate",
        "tileset": "Grineer Settlement"
    },
    "SolNode100": {
        "node": "Elara",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Survival",
        "tileset": "Corpus Gas City"
    },
    "SolNode101": {
        "node": "Kiliken",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Excavation",
        "tileset": "Corpus Outpost"
    },
    "SolNode102": {
        "node": "Oceanum",
        "planet": "Pluto",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Ship"
    },
    "SolNode103": {
        "node": "Terminus",
        "planet": "Mercury",
        "enemy": "Crossfire",
        "type": "Sabotage",
        "tileset": "Grineer Galleon"
    },
    "SolNode104": {
        "node": "Fossa",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Assassinate",
        "tileset": "Corpus Ship"
    },
    "SolNode105": {
        "node": "Titania",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Assassination",
        "tileset": "Grineer Sealab"
    },
    "SolNode106": {
        "node": "Alator",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Settlement"
    },
    "SolNode107": {
        "node": "Venera",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Outpost"
    },
    "SolNode108": {
        "node": "Tolstoj",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Assassinate",
        "tileset": "Grineer Asteroid"
    },
    "SolNode109": {
        "node": "Linea",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Outpost"
    },
    "SolNode110": {
        "node": "Hyperion",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode111": {
        "node": "Juliet",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode112": {
        "node": "Setebos",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode113": {
        "node": "Ares",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Settlement"
    },
    "SolNode114": {
        "node": "Puck",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Sealab"
    },
    "SolNode115": {
        "node": "Quirinus",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode116": {
        "node": "Mab",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode117": {
        "node": "Naiad",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode118": {
        "node": "Laomedeia",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Outpost"
    },
    "SolNode119": {
        "node": "Caloris",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Rescue",
        "tilset": "Grineer Asteroid"
    },
    "SolNode120": {
        "node": "Halimede",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode121": {
        "node": "Carpo",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Gas City"
    },
    "SolNode122": {
        "node": "Stephano",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Sealab"
    },
    "SolNode123": {
        "node": "V Prime",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Survival",
        "tileset": "Corpus Ship"
    },
    "SolNode124": {
        "node": "Trinculo",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode125": {
        "node": "Io",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Gas City"
    },
    "SolNode126": {
        "node": "Metis",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Gas City"
    },
    "SolNode127": {
        "node": "Psamathe",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Assassination",
        "tileset": "Corpus Ship"
    },
    "SolNode128": {
        "node": "E Gate",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Outpost"
    },
    "SolNode129": {
        "node": "Orb Vallis",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Free Roam",
        "tileset": "Orb Vallis"
    },
    "SolNode130": {
        "node": "Lares",
        "planet": "Mercury",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Grineer Asteroid"
    },
    "SolNode131": {
        "node": "Pallas",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Shipyard"
    },
    "SolNode132": {
        "node": "Bode",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Shipyard"
    },
    "SolNode133": {
        "node": "Vedic",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode134": {
        "node": "Varro",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode135": {
        "node": "Thon",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Galleon"
    },
    "SolNode136": {
        "node": "Olla",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode137": {
        "node": "Nuovo",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Shipyard"
    },
    "SolNode138": {
        "node": "Ludi",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Hijack",
        "tileset": "Grineer Shipyard"
    },
    "SolNode139": {
        "node": "Lex",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Shipyard"
    },
    "SolNode140": {
        "node": "Kiste",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Shipyard"
    },
    "SolNode141": {
        "node": "Ker",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Sabotage",
        "tileset": "Grineer Shipyard"
    },
    "SolNode142": {
        "node": "Hapke",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode143": {
        "node": "Gefion",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode144": {
        "node": "Exta",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Assassination",
        "tileset": "Grineer Shipyard"
    },
    "SolNode145": {
        "node": "Egeria",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode146": {
        "node": "Draco",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Survival",
        "tileset": "Grineer Asteroid"
    },
    "SolNode147": {
        "node": "Cinxia",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Shipyard"
    },
    "SolNode148": {
        "node": "Cerium",
        "planet": "Ceres"
    },
    "SolNode149": {
        "node": "Casta",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Shipyard"
    },
    "SolNode150": {
        "node": "Albedo",
        "planet": "Ceres",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode151": {
        "node": "Acanth",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode152": {
        "node": "Ascar",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode153": {
        "node": "Brugia",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Rescue",
        "tileset": "Infested Ship"
    },
    "SolNode154": {
        "node": "Candiru",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode155": {
        "node": "Cosis",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode156": {
        "node": "Cyath",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode157": {
        "node": "Giardia",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode158": {
        "node": "Gnathos",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode159": {
        "node": "Lepis",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode160": {
        "node": "Histo",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode161": {
        "node": "Hymeno",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode162": {
        "node": "Isos",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Capture",
        "tileset": "Infested Ship"
    },
    "SolNode163": {
        "node": "Ixodes",
        "planet": "Eris"
    },
    "SolNode164": {
        "node": "Kala-azar",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Infested Ship"
    },
    "SolNode165": {
        "node": "Sporid",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Hive Sabotage"
    },
    "SolNode166": {
        "node": "Nimus",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Infested Ship"
    },
    "SolNode167": {
        "node": "Oestrus",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Salvage",
        "tileset": "Infested Ship"
    },
    "SolNode168": {
        "node": "Phalan",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode169": {
        "node": "Psoro",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode170": {
        "node": "Ranova",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode171": {
        "node": "Saxis",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Exterminate",
        "tileset": "Infested Ship"
    },
    "SolNode172": {
        "node": "Xini",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Interception",
        "tileset": "Corpus Ship"
    },
    "SolNode173": {
        "node": "Solium",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Mobile Defense",
        "tileset": "Infested Ship"
    },
    "SolNode174": {
        "node": "Sparga",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode175": {
        "node": "Naeglar",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Hive",
        "tileset": "Infested Ship"
    },
    "SolNode176": {
        "node": "Viver",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Ancient Retribution"
    },
    "SolNode177": {
        "node": "Kappa",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Disruption",
        "tileset": "Grineer Galleon"
    },
    "SolNode178": {
        "node": "Hyosube",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode179": {
        "node": "Jengu",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode180": {
        "node": "Undine",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode181": {
        "node": "Adaro",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Asteroid"
    },
    "SolNode182": {
        "node": "Camenae",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode183": {
        "node": "Vodyanoi",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Arena",
        "tileset": "Grineer Sealab"
    },
    "SolNode184": {
        "node": "Rusalka",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Galleon"
    },
    "SolNode185": {
        "node": "Berehynia",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Shipyard"
    },
    "SolNode186": {
        "node": "Phithale",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Sabotage"
    },
    "SolNode187": {
        "node": "Selkie",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Survival",
        "tileset": "Grineer Asteroid"
    },
    "SolNode188": {
        "node": "Kelpie",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Galleon"
    },
    "SolNode189": {
        "node": "Naga",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Galleon"
    },
    "SolNode190": {
        "node": "Nakki",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Arena",
        "tileset": "Grineer Shipyard"
    },
    "SolNode191": {
        "node": "Marid",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Hijack",
        "tileset": "Grineer Shipyard"
    },
    "SolNode192": {
        "node": "Tikoloshe",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Spy"
    },
    "SolNode193": {
        "node": "Merrow",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Assassination",
        "tileset": "Grineer Asteroid"
    },
    "SolNode194": {
        "node": "Ponaturi",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode195": {
        "node": "Hydron",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Grineer Galleon"
    },
    "SolNode196": {
        "node": "Charybdis",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Galleon"
    },
    "SolNode197": {
        "node": "Graeae",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode198": {
        "node": "Scylla",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode199": {
        "node": "Yam",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Arena",
        "tileset": "Grineer Sealab"
    },
    "SolNode200": {
        "node": "Veles",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode201": {
        "node": "Tiamat",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode202": {
        "node": "Yemaja",
        "planet": "Sedna",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode203": {
        "node": "Abaddon",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode204": {
        "node": "Armaros",
        "planet": "Europa",
        "enemy": "Infested",
        "type": "Exterminate",
        "tileset": "Infested Ship"
    },
    "SolNode205": {
        "node": "Baal",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode206": {
        "node": "Eligor",
        "planet": "Europa"
    },
    "SolNode207": {
        "node": "Gamygyn",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode208": {
        "node": "Lillith",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode209": {
        "node": "Morax",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode210": {
        "node": "Naamah",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Assassination",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode211": {
        "node": "Ose",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Interception",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode212": {
        "node": "Paimon",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode213": {
        "node": "Shax",
        "planet": "Europa"
    },
    "SolNode214": {
        "node": "Sorath",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Hijack",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode215": {
        "node": "Valac",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Ship"
    },
    "SolNode216": {
        "node": "Valefor",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Excavation",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode217": {
        "node": "Orias",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode218": {
        "node": "Zagan",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode219": {
        "node": "Beleth",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SolNode220": {
        "node": "Kokabiel",
        "planet": "Europa",
        "enemy": "Corpus",
        "type": "Sabotage",
        "tileset": "Corpus Ice Planet"
    },
    "SolNode221": {
        "node": "Neruda",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode222": {
        "node": "Eminescu",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Ancient Retribution"
    },
    "SolNode223": {
        "node": "Boethius",
        "planet": "Mercury",
        "enemy": "Infested",
        "type": "Mobile Defense",
        "tileset": "Grineer Asteroid"
    },
    "SolNode224": {
        "node": "Odin",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Interception",
        "tileset": "Grineer Galleon"
    },
    "SolNode225": {
        "node": "Suisei",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Galleon"
    },
    "SolNode226": {
        "node": "Pantheon",
        "planet": "Mercury",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Galleon"
    },
    "SolNode227": {
        "node": "Verdi",
        "planet": "Mercury"
    },
    "SolNode228": {
        "node": "Plains of Eidolon",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Free Roam",
        "tileset": "Plains of Eidolon"
    },
    "SolNode230": {
        "node": "Everview Arc",
        "planet": "Zariman Ten Zero",
        "enemy": "Grineer/Corpus",
        "type": "Void Flood",
        "tileset": "Zariman"
    },
    "SolNode231": {
        "node": "Halako Perimeter",
        "planet": "Zariman Ten Zero",
        "enemy": "Grineer/Corpus",
        "type": "Exterminate",
        "tileset": "Zariman"
    },
    "SolNode232": {
        "node": "Tuvul Commons",
        "planet": "Zariman Ten Zero",
        "enemy": "Grineer/Corpus",
        "type": "Void Cascade",
        "tileset": "Zariman"
    },
    "SolNode233": {
        "node": "Oro Works",
        "planet": "Zariman Ten Zero",
        "enemy": "Grineer/Corpus",
        "type": "Void Armageddon",
        "tileset": "Zariman"
    },
    "SolNode234": {
        "node": "Dormizone",
        "planet": "Zariman Ten Zero",
        "enemy": "Tenno",
        "type": "Relay",
        "tileset": "Zariman"
    },
    "SolNode235": {
        "node": "The Greenway",
        "planet": "Zariman Ten Zero",
        "enemy": "Tenno",
        "type": "Mobile Defense",
        "tileset": "Zariman"
    },
    "SolNode400": {
        "node": "Teshub",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Exterminate",
        "tileset": "Orokin Tower"
    },
    "SolNode401": {
        "node": "Hepit",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Capture",
        "tileset": "Orokin Tower"
    },
    "SolNode402": {
        "node": "Taranis",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Defense",
        "tileset": "Orokin Tower"
    },
    "SolNode403": {
        "node": "Tiwaz",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Mobile Defense",
        "tileset": "Orokin Tower"
    },
    "SolNode404": {
        "node": "Stribog",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Orokin Sabotage",
        "tileset": "Orokin Tower"
    },
    "SolNode405": {
        "node": "Ani",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Survival",
        "tileset": "Orokin Tower"
    },
    "SolNode406": {
        "node": "Ukko",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Capture",
        "tileset": "Orokin Tower"
    },
    "SolNode407": {
        "node": "Oxomoco",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Exterminate",
        "tileset": "Orokin Tower"
    },
    "SolNode408": {
        "node": "Belenus",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Defense",
        "tileset": "Orokin Tower"
    },
    "SolNode409": {
        "node": "Mot",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Survival",
        "tileset": "Orokin Tower"
    },
    "SolNode410": {
        "node": "Aten",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Mobile Defense",
        "tileset": "Orokin Tower"
    },
    "SolNode411": {
        "node": "SolNode411",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Ancient Retribution",
        "tileset": "Orokin Tower"
    },
    "SolNode412": {
        "node": "Mithra",
        "planet": "Void",
        "enemy": "Orokin",
        "type": "Interception",
        "tileset": "Orokin Tower"
    },
    "SolNode413": {
        "node": "SolNode413",
        "planet": "Void",
        "enemy": "Corrupted",
        "type": "Ancient Retribution",
        "tileset": "Orokin Tower"
    },
    "SolNode740": {
        "node": "The Ropalolyst",
        "planet": "Jupiter",
        "enemy": "Sentient",
        "type": "Assassination",
        "tileset": "Corpus Gas City"
    },
    "SolNode741": {
        "node": "Koro",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Assault",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode742": {
        "node": "Nabuk",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Capture",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode743": {
        "node": "Rotuma",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode744": {
        "node": "Taveuni",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Survival",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode745": {
        "node": "Tamu",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Disruption",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode746": {
        "node": "Dakata",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode747": {
        "node": "Pago",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Spy",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode748": {
        "node": "Garus",
        "planet": "Kuva Fortress",
        "enemy": "Grineer",
        "type": "Rescue",
        "tileset": "Grineer Asteroid Fortress"
    },
    "SolNode901": {
        "node": "Caduceus",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "SolNode902": {
        "node": "Montes",
        "planet": "Venus",
        "enemy": "Corpus",
        "type": "Exterminate (Archwing)",
        "tileset": "Corpus Ship (Archwing)"
    },
    "SolNode903": {
        "node": "Erpo",
        "planet": "Earth",
        "enemy": "Grineer",
        "type": "Mobile Defense (Archwing)",
        "tileset": "Free Space"
    },
    "SolNode904": {
        "node": "Syrtis",
        "planet": "Mars",
        "enemy": "Grineer",
        "type": "Exterminate (Archwing)",
        "tileset": "Free Space"
    },
    "SolNode905": {
        "node": "Galilea ",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Sabotage (Archwing)",
        "tileset": "Corpus Ship (Archwing)"
    },
    "SolNode906": {
        "node": "Pandora",
        "planet": "Saturn",
        "enemy": "Grineer",
        "type": "Pursuit (Archwing)",
        "tileset": "Free Space"
    },
    "SolNode907": {
        "node": "Caelus",
        "planet": "Uranus",
        "enemy": "Grineer",
        "type": "Interception (Archwing)",
        "tileset": "Free Space"
    },
    "SolNode908": {
        "node": "Salacia",
        "planet": "Neptune",
        "enemy": "Corpus",
        "type": "Mobile Defense (Archwing)",
        "tileset": "Corpus Ship (Archwing)"
    },
    "SolNode300": {
        "node": "Plato",
        "planet": "Lua",
        "enemy": "Grineer",
        "type": "Exterminate",
        "tileset": "Orokin Moon"
    },
    "SolNode301": {
        "node": "Grimaldi",
        "planet": "Lua",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Orokin Moon"
    },
    "SolNode302": {
        "node": "Tycho",
        "planet": "Lua",
        "enemy": "Corpus",
        "type": "Survival",
        "tileset": "Orokin Moon"
    },
    "SolNode304": {
        "node": "Copernicus",
        "planet": "Lua",
        "enemy": "Grineer",
        "type": "Mobile Defense",
        "tileset": "Orokin Moon"
    },
    "SolNode305": {
        "node": "Stöfler",
        "planet": "Lua",
        "enemy": "Grineer",
        "type": "Defense",
        "tileset": "Orokin Moon"
    },
    "SolNode306": {
        "node": "Pavlov",
        "planet": "Lua",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Orokin Moon"
    },
    "SolNode307": {
        "node": "Zeipel",
        "planet": "Lua",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Orokin Moon"
    },
    "SolNode308": {
        "node": "Apollo",
        "planet": "Lua",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Orokin Moon"
    },
    "SettlementNode1": {
        "node": "Roche",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Exterminate",
        "tileset": "Corpus Ship"
    },
    "SettlementNode2": {
        "node": "Skyresh",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Capture",
        "tileset": "Corpus Ship"
    },
    "SettlementNode3": {
        "node": "Stickney",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Survival",
        "tileset": "Corpus Ship"
    },
    "SettlementNode4": {
        "node": "Drunlo",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode5": {
        "node": "Grildrig",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode6": {
        "node": "Limtoc",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode7": {
        "node": "Hall",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode8": {
        "node": "Reldresal",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode9": {
        "node": "Clustril",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode10": {
        "node": "Kepler",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Rush (Archwing)",
        "tileset": "Corpus Ship (Archwing)"
    },
    "SettlementNode11": {
        "node": "Gulliver",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Defense",
        "tileset": "Corpus Ship"
    },
    "SettlementNode12": {
        "node": "Monolith",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Rescue",
        "tileset": "Corpus Ship"
    },
    "SettlementNode13": {
        "node": "D'Arrest",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode14": {
        "node": "Shklovsky",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Spy",
        "tileset": "Corpus Ship"
    },
    "SettlementNode15": {
        "node": "Sharpless",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Mobile Defense",
        "tileset": "Corpus Ship"
    },
    "SettlementNode16": {
        "node": "Wendell",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode17": {
        "node": "Flimnap",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode18": {
        "node": "Opik",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode19": {
        "node": "Todd",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Ancient Retribution"
    },
    "SettlementNode20": {
        "node": "Iliad",
        "planet": "Phobos",
        "enemy": "Corpus",
        "type": "Assassination",
        "tileset": "Corpus Ship"
    },
    "MercuryHUB": {
        "node": "Larunda Relay",
        "planet": "Mercury",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "VenusHUB": {
        "node": "Vesper Relay",
        "planet": "Venus",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "EarthHUB": {
        "node": "Strata Relay",
        "planet": "Earth",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "SaturnHUB": {
        "node": "Kronia Relay",
        "planet": "Saturn",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "ErisHUB": {
        "node": "Kuiper Relay",
        "planet": "Eris",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "EuropaHUB": {
        "node": "Leonov Relay",
        "planet": "Europa",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "PlutoHUB": {
        "node": "Orcus Relay",
        "planet": "Pluto",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "ZarimanHub": {
        "node": "Chrysalith",
        "planet": "Zariman Ten Zero",
        "enemy": "Tenno",
        "type": "Relay"
    },
    "EventNode0": {
        "node": "Balor",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode1": {
        "node": "Tethra",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode2": {
        "node": "Operation Gate Crash",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode3": {
        "node": "Elatha",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode4": {
        "node": "Proxy Rebellion",
        "enemy": "Corpus",
        "type": "Survival"
    },
    "EventNode5": {
        "node": "Birog",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode6": {
        "node": "Tyl Reygor Seal Lab",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode7": {
        "node": "Proxy Rebellion",
        "enemy": "Corpus",
        "type": "Interception"
    },
    "EventNode8": {
        "node": "Corb",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode9": {
        "node": "Operation Gate Crash Pt. 2",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode10": {
        "node": "Lugh",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode11": {
        "node": "Nemed",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode12": {
        "node": "Operation Cryotic Front",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode13": {
        "node": "Shifting Sands",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode14": {
        "node": "Gate Crash",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode15": {
        "node": "Operation Cryotic Front",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode16": {
        "node": "Operation Cryotic Front",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode17": {
        "node": "Proxy Rebellion",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "EventNode18": {
        "node": "Proxy Rebellion",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "EventNode19": {
        "node": "Mars",
        "enemy": "Grineer",
        "type": "Defense"
    },
    "EventNode20": {
        "node": "Tyl Regor Sea Lab",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode22": {
        "node": "Tyl Regor Sea Lab",
        "enemy": "Sentient",
        "type": "Ancient Retribution"
    },
    "EventNode24": {
        "node": "Earth",
        "enemy": "Grineer",
        "type": "Arena"
    },
    "EventNode25": {
        "node": "Earth",
        "enemy": "Grineer",
        "type": "Arena"
    },
    "EventNode26": {
        "node": "Earth",
        "enemy": "Grineer",
        "type": "Exterminate"
    },
    "EventNode27": {
        "node": "Void",
        "enemy": "Corrupted",
        "type": "Survival"
    },
    "EventNode28": {
        "node": "Saturn",
        "enemy": "Grineer",
        "type": "Assassination"
    },
    "EventNode29": {
        "node": "Saturn",
        "enemy": "Grineer",
        "type": "Assassination"
    },
    "EventNode30": {
        "node": "Ganymede",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Gas City"
    },
    "EventNode31": {
        "node": "Ganymede",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Gas City"
    },
    "EventNode32": {
        "node": "Ganymede",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Gas City"
    },
    "EventNode33": {
        "node": "Ganymede",
        "planet": "Jupiter",
        "enemy": "Corpus",
        "type": "Disruption",
        "tileset": "Corpus Gas City"
    },
    "EventNode34": {
        "node": "Earth",
        "enemy": "Grineer",
        "type": "Arena"
    },
    "EventNode35": {
        "node": "Earth",
        "enemy": "Grineer",
        "type": "Arena"
    },
    "EventNode761": {
        "node": "The Index",
        "enemy": "Corpus",
        "type": "Arena"
    },
    "EventNode762": {
        "node": "The Index pt 2",
        "enemy": "Corpus",
        "type": "Arena"
    },
    "EventNode763": {
        "node": "The Index Endurance",
        "enemy": "Corpus",
        "type": "Arena"
    },
    "PvpNode0": {
        "node": "Conclave Capture the Cephalon",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode1": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode2": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode3": {
        "node": "Conclave Capture the Cephalon",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode4": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode5": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode6": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode7": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode8": {
        "node": "Conclave",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode9": {
        "node": "Conclave Team Domination",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode10": {
        "node": "Conclave Domination",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode11": {
        "node": "Conclave Domination",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode12": {
        "node": "Conclave Domination",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode13": {
        "node": "Tactical Alert: Snoball Fight!",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "PvpNode14": {
        "node": "Conclave: Quick Steel",
        "enemy": "Tenno",
        "type": "Conclave"
    },
    "ClanNode0": {
        "node": "Romula",
        "planet": "Venus",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Corpus Ship"
    },
    "ClanNode1": {
        "node": "Malva",
        "planet": "Venus",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Corpus Ship"
    },
    "ClanNode2": {
        "node": "Coba",
        "planet": "Earth",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Grineer Forest"
    },
    "ClanNode3": {
        "node": "Tikal",
        "planet": "Earth",
        "enemy": "Infested",
        "type": "Excavation",
        "tileset": "Grineer Forest"
    },
    "ClanNode4": {
        "node": "Sinai",
        "planet": "Jupiter",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Corpus Gas City"
    },
    "ClanNode5": {
        "node": "Cameria",
        "planet": "Jupiter",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Corpus Gas City"
    },
    "ClanNode6": {
        "node": "Larzac",
        "planet": "Europa",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Corpus Ice Planet"
    },
    "ClanNode7": {
        "node": "Cholistan",
        "planet": "Europa",
        "enemy": "Infested",
        "type": "Excavation",
        "tileset": "Corpus Ice Planet"
    },
    "ClanNode8": {
        "node": "Kadesh",
        "planet": "Mars",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Grineer Settlement"
    },
    "ClanNode9": {
        "node": "Wahiba",
        "planet": "Mars",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Corpus Ship"
    },
    "ClanNode10": {
        "node": "Memphis",
        "planet": "Phobos",
        "enemy": "Infested",
        "type": "Defection",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode11": {
        "node": "Zeugma",
        "planet": "Phobos",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode12": {
        "node": "Caracol",
        "planet": "Saturn",
        "enemy": "Infested",
        "type": "Defection",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode13": {
        "node": "Piscinas",
        "planet": "Saturn",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode14": {
        "node": "Amarna",
        "planet": "Sedna",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode15": {
        "node": "Sangeru",
        "planet": "Sedna",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Grineer Asteroid"
    },
    "ClanNode16": {
        "node": "Ur",
        "planet": "Uranus",
        "enemy": "Infested",
        "type": "Disruption",
        "tileset": "Grineer Galleon"
    },
    "ClanNode17": {
        "node": "Assur",
        "planet": "Uranus",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Galleon"
    },
    "ClanNode18": {
        "node": "Akkad",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Infested Ship"
    },
    "ClanNode19": {
        "node": "Zabala",
        "planet": "Eris",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Infested Ship"
    },
    "ClanNode20": {
        "node": "Yursa",
        "planet": "Neptune",
        "enemy": "Infested",
        "type": "Defection",
        "tileset": "Infested Ship"
    },
    "ClanNode21": {
        "node": "Kelashin",
        "planet": "Neptune",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Infested Ship"
    },
    "ClanNode22": {
        "node": "Seimeni",
        "planet": "Ceres",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Grineer Shipyard"
    },
    "ClanNode23": {
        "node": "Gabii",
        "planet": "Ceres",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Grineer Galleon"
    },
    "ClanNode24": {
        "node": "Sechura",
        "planet": "Pluto",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Corpus Outpost"
    },
    "ClanNode25": {
        "node": "Hieracon",
        "planet": "Pluto",
        "enemy": "Infested",
        "type": "Excavation",
        "tileset": "Corpus Outpost"
    },
    "/Lotus/Types/Keys/SortieBossKeyPhorid": {
        "node": "Sortie Boss: Phorid",
        "enemy": "Infested",
        "type": "Assassination",
        "tileset": "Grineer Asteroid"
    },
    "SolNode706": {
        "node": "Horend",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Capture",
        "tileset": "Orokin Derelict"
    },
    "SolNode707": {
        "node": "Hyf",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Defense",
        "tileset": "Orokin Derelict"
    },
    "SolNode708": {
        "node": "Phlegyas",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Exterminate",
        "tileset": "Orokin Derelict"
    },
    "SolNode709": {
        "node": "Dirus",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Mobile Defense",
        "tileset": "Orokin Derelict"
    },
    "SolNode710": {
        "node": "Formido",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Sabotage",
        "tileset": "Orokin Derelict"
    },
    "SolNode711": {
        "node": "Terrorem",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Survival",
        "tileset": "Orokin Derelict"
    },
    "SolNode712": {
        "node": "Magnacidium",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Assassinate",
        "tileset": "Orokin Derelict"
    },
    "SolNode713": {
        "node": "Exequias",
        "planet": "Deimos",
        "enemy": "Infested",
        "type": "Assassinate",
        "tileset": "Orokin Derelict"
    },
    "CrewBattleNode501": {
        "node": "Mordo Cluster",
        "planet": "Saturn Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode502": {
        "node": "Sover Strait",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode503": {
        "node": "Bifrost Echo",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Exterminate"
    },
    "CrewBattleNode504": {
        "node": "Arva Vector",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "CrewBattleNode505": {
        "node": "Ruse War Field",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode509": {
        "node": "Iota Temple",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode510": {
        "node": "Gian Point",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode511": {
        "node": "Beacon Shield Ring",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Volatile"
    },
    "CrewBattleNode512": {
        "node": "Orvin-Haarc",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Spy"
    },
    "CrewBattleNode513": {
        "node": "Vesper Strait",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Orphix"
    },
    "CrewBattleNode514": {
        "node": "Falling Glory",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "CrewBattleNode515": {
        "node": "Luckless Expanse",
        "planet": "Venus Proxima",
        "enemy": "Corpus",
        "type": "Survival"
    },
    "CrewBattleNode516": {
        "node": "Nu-Gua Mines",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Exterminate"
    },
    "CrewBattleNode518": {
        "node": "Ogal Cluster",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode519": {
        "node": "Korm's Belt",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode521": {
        "node": "Enkidu Ice Drifts",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Survival"
    },
    "CrewBattleNode522": {
        "node": "Bendar Cluster",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode523": {
        "node": "Mammon's Prospect",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Orphix"
    },
    "CrewBattleNode524": {
        "node": "Sovereign Grasp",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Volatile"
    },
    "CrewBattleNode525": {
        "node": "Brom Cluster",
        "planet": "Neptune Proxima",
        "enemy": "Corpus",
        "type": "Spy"
    },
    "CrewBattleNode526": {
        "node": "Khufu Envoy",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Orphix"
    },
    "CrewBattleNode527": {
        "node": "Seven Sirens",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Exterminate"
    },
    "CrewBattleNode528": {
        "node": "Obol Crossing",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "CrewBattleNode529": {
        "node": "Profit Margin",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Volatile"
    },
    "CrewBattleNode530": {
        "node": "Kasio's Rest",
        "planet": "Saturn Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode531": {
        "node": "Fenton's Field",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Survival"
    },
    "CrewBattleNode533": {
        "node": "Nodo Gap",
        "planet": "Saturn Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode534": {
        "node": "Lupal Pass",
        "planet": "Saturn Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode535": {
        "node": "Vand Cluster",
        "planet": "Saturn Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode536": {
        "node": "Peregrine Axis",
        "planet": "Pluto Proxima",
        "enemy": "Corpus",
        "type": "Spy"
    },
    "CrewBattleNode538": {
        "node": "Calabash",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Exterminate"
    },
    "CrewBattleNode539": {
        "node": "Numina",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Volatile"
    },
    "CrewBattleNode540": {
        "node": "Arc Silver",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Defense"
    },
    "CrewBattleNode541": {
        "node": "Erato",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Orphix"
    },
    "CrewBattleNode542": {
        "node": "Lu-yan",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Survival"
    },
    "CrewBattleNode543": {
        "node": "SABMIR CLOUD",
        "planet": "Veil Proxima",
        "enemy": "Corpus",
        "type": "Spy"
    },
    "CrewBattleNode550": {
        "node": "Nsu Grid",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode551": {
        "node": "Ganalen's Grave",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode552": {
        "node": "Rya",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode553": {
        "node": "Flexa",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode554": {
        "node": "H-2 Cloud",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode555": {
        "node": "R-9 Cloud",
        "planet": "Veil Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    },
    "CrewBattleNode556": {
        "node": "Free Flight",
        "planet": "Earth Proxima",
        "enemy": "Grineer",
        "type": "Skirmish"
    }
}

fissure_parser = {
    "era": {
        "VoidT1": "Lith",
        "VoidT2": "Meso",
        "VoidT3": "Neo",
        "VoidT4": "Axi",
        "VoidT5": "Requiem"
    },
    "era_key": {
        "normal": "Modifier",
        "sp": "Modifier",
        "vs": "ActiveMissionTier"
    },
    "mission_overrides": {
        "SolNode10": "Gas City Sabotage",
        "SolNode19": "Machine Sabotage",
        "SolNode56": "Ship Sabotage",
        "SolNode57": "Core Sabotage",
        "SolNode61": "Ship Sabotage",
        "SolNode75": "Injector Sabotage",
        "SolNode82": "Ship Sabotage",
        "SolNode88": "Ship Sabotage",
        "SolNode113": "Core Sabotage",
        "SolNode135": "Ship Sabotage",
        "SolNode141": "Core Sabotage",
        "SolNode184": "Ship Sabotage",
        "SolNode220": "Core Sabotage",
        "SolNode404": "Orokin Sabotage"
    },
    "tier": {
        "Capture": 1,
        "Defense": 5,
        "Excavation": 3,
        "Exterminate": 1,
        "Hive": 4,
        "Spy": 4,
        "Mobile Defense": 5,
        "Rescue": 2,
        "Survival": 5,
        "Interception": 5,
        "Disruption": 3,
        "Assault": 5,
        "Skirmish": 2,
        "Volatile": 2,
        "Core Sabotage": 1,
        "Machine Sabotage": 1,
        "Ship Sabotage": 2,
        "Gas City Sabotage": 4,
        "Orokin Sabotage": 4,
        "Injector Sabotage": 4,
    }
}


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


SORT_ORDER = {'Lith': 0,
              'Meso': 1,
              'Neo': 2,
              'Axi': 3,
              'Requiem': 4}

fissure_types = ['normal', 'sp', 'vs', 'requiem']


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
