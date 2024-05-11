import logging

sol_nodes = {
  "SolNode0": {
    "node": "SolNode0",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode1": {
    "node": "Galatea",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode2": {
    "node": "Aphrodite",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode3": {
    "node": "Cordelia",
    "planet": "Uranus",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode4": {
    "node": "Acheron",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode5": {
    "node": "Perdita",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode6": {
    "node": "Despina",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Excavation",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode7": {
    "node": "Epimetheus",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode8": {
    "node": "Nix",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode9": {
    "node": "Rosalind",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode10": {
    "node": "Thebe",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode11": {
    "node": "Tharsis",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode12": {
    "node": "Elion",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode13": {
    "node": "Bianca",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode14": {
    "node": "Ultor",
    "planet": "Mars",
    "enemy": "Crossfire",
    "type": "Exterminate",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode15": {
    "node": "Pacific",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode16": {
    "node": "Augustus",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Excavation",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode17": {
    "node": "Proteus",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode18": {
    "node": "Rhea",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode19": {
    "node": "Enceladus",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode20": {
    "node": "Telesto",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode21": {
    "node": "Narcissus",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode22": {
    "node": "Tessera",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode23": {
    "node": "Cytherean",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Interception",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode24": {
    "node": "Oro",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Assassination",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode25": {
    "node": "Callisto",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Interception",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode26": {
    "node": "Lith",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode27": {
    "node": "E Prime",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode28": {
    "node": "M Prime",
    "planet": "Mercury",
    "enemy": "Crossfire",
    "type": "Exterminate",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode29": {
    "node": "Oberon",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode30": {
    "node": "Olympus",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Disruption",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode31": {
    "node": "Anthe",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode32": {
    "node": "Tethys",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Assassination",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode33": {
    "node": "Ariel",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode34": {
    "node": "Sycorax",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode35": {
    "node": "Arcadia",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode36": {
    "node": "Martialis",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode37": {
    "node": "Pallene",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode38": {
    "node": "Minthe",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode39": {
    "node": "Everest",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Excavation",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode40": {
    "node": "Prospero",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode41": {
    "node": "Arval",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode42": {
    "node": "Helene",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode43": {
    "node": "Cerberus",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Interception",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode44": {
    "node": "Mimas",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode45": {
    "node": "Ara",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode46": {
    "node": "Spear",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode47": {
    "node": "Janus",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode48": {
    "node": "Regna",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode49": {
    "node": "Larissa",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode50": {
    "node": "Numa",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode51": {
    "node": "Hades",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Assassination",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode52": {
    "node": "Portia",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode53": {
    "node": "Themisto",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Assassination",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode54": {
    "node": "Silvanus",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode55": {
    "node": "Methone",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode56": {
    "node": "Cypress",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode57": {
    "node": "Sao",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode58": {
    "node": "Hellas",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode59": {
    "node": "Eurasia",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode60": {
    "node": "Caliban",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode61": {
    "node": "Ishtar",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode62": {
    "node": "Neso",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode63": {
    "node": "Mantle",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode64": {
    "node": "Umbriel",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode65": {
    "node": "Gradivus",
    "planet": "Mars",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode66": {
    "node": "Unda",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode67": {
    "node": "Dione",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode68": {
    "node": "Vallis",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode69": {
    "node": "Ophelia",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Survival",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode70": {
    "node": "Cassini",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode71": {
    "node": "Vesper",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Spy",
    "dark_sector": False
  },
  "SolNode72": {
    "node": "Outer Terminus",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode73": {
    "node": "Ananke",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode74": {
    "node": "Carme",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode75": {
    "node": "Cervantes",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode76": {
    "node": "Hydra",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode77": {
    "node": "Cupid",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode78": {
    "node": "Triton",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode79": {
    "node": "Cambria",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode80": {
    "node": "Phoebe",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode81": {
    "node": "Palus",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Survival",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode82": {
    "node": "Calypso",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode83": {
    "node": "Cressida",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode84": {
    "node": "Nereid",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode85": {
    "node": "Gaia",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Forest",
    "dark_sector": False
  },
  "SolNode86": {
    "node": "Aegaeon",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode87": {
    "node": "Ganymede",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode88": {
    "node": "Adrastea",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode89": {
    "node": "Mariana",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode90": {
    "node": "Miranda",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode91": {
    "node": "Iapetus",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode92": {
    "node": "Charon",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode93": {
    "node": "Keeler",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode94": {
    "node": "Apollodorus",
    "planet": "Mercury",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode95": {
    "node": "Thalassa",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode96": {
    "node": "Titan",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Survival",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode97": {
    "node": "Amalthea",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode98": {
    "node": "Desdemona",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode99": {
    "node": "War",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Assassinate",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode100": {
    "node": "Elara",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Survival",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode101": {
    "node": "Kiliken",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Excavation",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode102": {
    "node": "Oceanum",
    "planet": "Pluto",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode103": {
    "node": "Terminus",
    "planet": "Mercury",
    "enemy": "Crossfire",
    "type": "Sabotage",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode104": {
    "node": "Fossa",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Assassinate",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode105": {
    "node": "Titania",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Assassination",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode106": {
    "node": "Alator",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode107": {
    "node": "Venera",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode108": {
    "node": "Tolstoj",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Assassinate",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode109": {
    "node": "Linea",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode110": {
    "node": "Hyperion",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode111": {
    "node": "Juliet",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode112": {
    "node": "Setebos",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode113": {
    "node": "Ares",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Settlement",
    "dark_sector": False
  },
  "SolNode114": {
    "node": "Puck",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode115": {
    "node": "Quirinus",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode116": {
    "node": "Mab",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode117": {
    "node": "Naiad",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode118": {
    "node": "Laomedeia",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode119": {
    "node": "Caloris",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Rescue",
    "tilset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode120": {
    "node": "Halimede",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode121": {
    "node": "Carpo",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode122": {
    "node": "Stephano",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode123": {
    "node": "V Prime",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Survival",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode124": {
    "node": "Trinculo",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode125": {
    "node": "Io",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode126": {
    "node": "Metis",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode127": {
    "node": "Psamathe",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Assassination",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode128": {
    "node": "E Gate",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Outpost",
    "dark_sector": False
  },
  "SolNode129": {
    "node": "Orb Vallis",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Free Roam",
    "tileset": "Orb Vallis",
    "dark_sector": False
  },
  "SolNode130": {
    "node": "Lares",
    "planet": "Mercury",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode131": {
    "node": "Pallas",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode132": {
    "node": "Bode",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode133": {
    "node": "Vedic",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode134": {
    "node": "Varro",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode135": {
    "node": "Thon",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode136": {
    "node": "Olla",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode137": {
    "node": "Nuovo",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode138": {
    "node": "Ludi",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Hijack",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode139": {
    "node": "Lex",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode140": {
    "node": "Kiste",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode141": {
    "node": "Ker",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Sabotage",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode142": {
    "node": "Hapke",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode143": {
    "node": "Gefion",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode144": {
    "node": "Exta",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Assassination",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode145": {
    "node": "Egeria",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode146": {
    "node": "Draco",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Survival",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode147": {
    "node": "Cinxia",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode148": {
    "node": "Cerium",
    "planet": "Ceres",
    "dark_sector": False
  },
  "SolNode149": {
    "node": "Casta",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode150": {
    "node": "Albedo",
    "planet": "Ceres",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode151": {
    "node": "Acanth",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode152": {
    "node": "Ascar",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode153": {
    "node": "Brugia",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Rescue",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode154": {
    "node": "Candiru",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode155": {
    "node": "Cosis",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode156": {
    "node": "Cyath",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode157": {
    "node": "Giardia",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode158": {
    "node": "Gnathos",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode159": {
    "node": "Lepis",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode160": {
    "node": "Histo",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode161": {
    "node": "Hymeno",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode162": {
    "node": "Isos",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Capture",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode163": {
    "node": "Ixodes",
    "planet": "Eris",
    "dark_sector": False
  },
  "SolNode164": {
    "node": "Kala-azar",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode165": {
    "node": "Sporid",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Hive Sabotage",
    "dark_sector": False
  },
  "SolNode166": {
    "node": "Nimus",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode167": {
    "node": "Oestrus",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Salvage",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode168": {
    "node": "Phalan",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode169": {
    "node": "Psoro",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode170": {
    "node": "Ranova",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode171": {
    "node": "Saxis",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Exterminate",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode172": {
    "node": "Xini",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Interception",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode173": {
    "node": "Solium",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Mobile Defense",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode174": {
    "node": "Sparga",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode175": {
    "node": "Naeglar",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Hive",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode176": {
    "node": "Viver",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode177": {
    "node": "Kappa",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Disruption",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode178": {
    "node": "Hyosube",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode179": {
    "node": "Jengu",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode180": {
    "node": "Undine",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode181": {
    "node": "Adaro",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode182": {
    "node": "Camenae",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode183": {
    "node": "Vodyanoi",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Arena",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode184": {
    "node": "Rusalka",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode185": {
    "node": "Berehynia",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode186": {
    "node": "Phithale",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Sabotage",
    "dark_sector": False
  },
  "SolNode187": {
    "node": "Selkie",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Survival",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode188": {
    "node": "Kelpie",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode189": {
    "node": "Naga",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode190": {
    "node": "Nakki",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Arena",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode191": {
    "node": "Marid",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Hijack",
    "tileset": "Grineer Shipyard",
    "dark_sector": False
  },
  "SolNode192": {
    "node": "Tikoloshe",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Spy",
    "dark_sector": False
  },
  "SolNode193": {
    "node": "Merrow",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Assassination",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode194": {
    "node": "Ponaturi",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode195": {
    "node": "Hydron",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode196": {
    "node": "Charybdis",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode197": {
    "node": "Graeae",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode198": {
    "node": "Scylla",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode199": {
    "node": "Yam",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Arena",
    "tileset": "Grineer Sealab",
    "dark_sector": False
  },
  "SolNode200": {
    "node": "Veles",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode201": {
    "node": "Tiamat",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode202": {
    "node": "Yemaja",
    "planet": "Sedna",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode203": {
    "node": "Abaddon",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode204": {
    "node": "Armaros",
    "planet": "Europa",
    "enemy": "Infested",
    "type": "Exterminate",
    "tileset": "Infested Ship",
    "dark_sector": False
  },
  "SolNode205": {
    "node": "Baal",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode206": {
    "node": "Eligor",
    "planet": "Europa",
    "dark_sector": False
  },
  "SolNode207": {
    "node": "Gamygyn",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode208": {
    "node": "Lillith",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode209": {
    "node": "Morax",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode210": {
    "node": "Naamah",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Assassination",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode211": {
    "node": "Ose",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Interception",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode212": {
    "node": "Paimon",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode213": {
    "node": "Shax",
    "planet": "Europa",
    "dark_sector": False
  },
  "SolNode214": {
    "node": "Sorath",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Hijack",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode215": {
    "node": "Valac",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SolNode216": {
    "node": "Valefor",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Excavation",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode217": {
    "node": "Orias",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode218": {
    "node": "Zagan",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode219": {
    "node": "Beleth",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode220": {
    "node": "Kokabiel",
    "planet": "Europa",
    "enemy": "Corpus",
    "type": "Sabotage",
    "tileset": "Corpus Ice Planet",
    "dark_sector": False
  },
  "SolNode221": {
    "node": "Neruda",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode222": {
    "node": "Eminescu",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode223": {
    "node": "Boethius",
    "planet": "Mercury",
    "enemy": "Infested",
    "type": "Mobile Defense",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode224": {
    "node": "Odin",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Interception",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode225": {
    "node": "Suisei",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode226": {
    "node": "Pantheon",
    "planet": "Mercury",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Galleon",
    "dark_sector": False
  },
  "SolNode227": {
    "node": "Verdi",
    "planet": "Mercury",
    "dark_sector": False
  },
  "SolNode228": {
    "node": "Plains of Eidolon",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Free Roam",
    "tileset": "Plains of Eidolon",
    "dark_sector": False
  },
  "SolNode230": {
    "node": "Everview Arc",
    "planet": "Zariman Ten Zero",
    "enemy": "Grineer/Corpus",
    "type": "Void Flood",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode231": {
    "node": "Halako Perimeter",
    "planet": "Zariman Ten Zero",
    "enemy": "Grineer/Corpus",
    "type": "Exterminate",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode232": {
    "node": "Tuvul Commons",
    "planet": "Zariman Ten Zero",
    "enemy": "Grineer/Corpus",
    "type": "Void Cascade",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode233": {
    "node": "Oro Works",
    "planet": "Zariman Ten Zero",
    "enemy": "Grineer/Corpus",
    "type": "Void Armageddon",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode234": {
    "node": "Dormizone",
    "planet": "Zariman Ten Zero",
    "enemy": "Tenno",
    "type": "Relay",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode235": {
    "node": "The Greenway",
    "planet": "Zariman Ten Zero",
    "enemy": "Tenno",
    "type": "Mobile Defense",
    "tileset": "Zariman",
    "dark_sector": False
  },
  "SolNode400": {
    "node": "Teshub",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Exterminate",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode401": {
    "node": "Hepit",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Capture",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode402": {
    "node": "Taranis",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Defense",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode403": {
    "node": "Tiwaz",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Mobile Defense",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode404": {
    "node": "Stribog",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Orokin Sabotage",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode405": {
    "node": "Ani",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Survival",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode406": {
    "node": "Ukko",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Capture",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode407": {
    "node": "Oxomoco",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Exterminate",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode408": {
    "node": "Belenus",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Defense",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode409": {
    "node": "Mot",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Survival",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode410": {
    "node": "Aten",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Mobile Defense",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode411": {
    "node": "Marduk",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Sabotage",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode412": {
    "node": "Mithra",
    "planet": "Void",
    "enemy": "Orokin",
    "type": "Interception",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode413": {
    "node": "SolNode413",
    "planet": "Void",
    "enemy": "Corrupted",
    "type": "Ancient Retribution",
    "tileset": "Orokin Tower",
    "dark_sector": False
  },
  "SolNode740": {
    "node": "The Ropalolyst",
    "planet": "Jupiter",
    "enemy": "Sentient",
    "type": "Assassination",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "SolNode741": {
    "node": "Koro",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Assault",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode742": {
    "node": "Nabuk",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Capture",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode743": {
    "node": "Rotuma",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode744": {
    "node": "Taveuni",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Survival",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode745": {
    "node": "Tamu",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Disruption",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode746": {
    "node": "Dakata",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode747": {
    "node": "Pago",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Spy",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode748": {
    "node": "Garus",
    "planet": "Kuva Fortress",
    "enemy": "Grineer",
    "type": "Rescue",
    "tileset": "Grineer Asteroid Fortress",
    "dark_sector": False
  },
  "SolNode901": {
    "node": "Caduceus",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SolNode902": {
    "node": "Montes",
    "planet": "Venus",
    "enemy": "Corpus",
    "type": "Exterminate (Archwing)",
    "tileset": "Corpus Ship (Archwing)",
    "dark_sector": False
  },
  "SolNode903": {
    "node": "Erpo",
    "planet": "Earth",
    "enemy": "Grineer",
    "type": "Mobile Defense (Archwing)",
    "tileset": "Free Space",
    "dark_sector": False
  },
  "SolNode904": {
    "node": "Syrtis",
    "planet": "Mars",
    "enemy": "Grineer",
    "type": "Exterminate (Archwing)",
    "tileset": "Free Space",
    "dark_sector": False
  },
  "SolNode905": {
    "node": "Galilea ",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Sabotage (Archwing)",
    "tileset": "Corpus Ship (Archwing)",
    "dark_sector": False
  },
  "SolNode906": {
    "node": "Pandora",
    "planet": "Saturn",
    "enemy": "Grineer",
    "type": "Pursuit (Archwing)",
    "tileset": "Free Space",
    "dark_sector": False
  },
  "SolNode907": {
    "node": "Caelus",
    "planet": "Uranus",
    "enemy": "Grineer",
    "type": "Interception (Archwing)",
    "tileset": "Free Space",
    "dark_sector": False
  },
  "SolNode908": {
    "node": "Salacia",
    "planet": "Neptune",
    "enemy": "Corpus",
    "type": "Mobile Defense (Archwing)",
    "tileset": "Corpus Ship (Archwing)",
    "dark_sector": False
  },
  "SolNode300": {
    "node": "Plato",
    "planet": "Lua",
    "enemy": "Grineer",
    "type": "Exterminate",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode301": {
    "node": "Grimaldi",
    "planet": "Lua",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode302": {
    "node": "Tycho",
    "planet": "Lua",
    "enemy": "Corpus",
    "type": "Survival",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode304": {
    "node": "Copernicus",
    "planet": "Lua",
    "enemy": "Grineer",
    "type": "Mobile Defense",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode305": {
    "node": "Stöfler",
    "planet": "Lua",
    "enemy": "Grineer",
    "type": "Defense",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode306": {
    "node": "Pavlov",
    "planet": "Lua",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode307": {
    "node": "Zeipel",
    "planet": "Lua",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode308": {
    "node": "Apollo",
    "planet": "Lua",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode309": {
    "node": "Yuvarium",
    "planet": "Lua",
    "enemy": "Corrupted",
    "type": "Survival",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SolNode310": {
    "node": "Circulus",
    "planet": "Lua",
    "enemy": "Corrupted",
    "type": "Survival",
    "tileset": "Orokin Moon",
    "dark_sector": False
  },
  "SettlementNode1": {
    "node": "Roche",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Exterminate",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode2": {
    "node": "Skyresh",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Capture",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode3": {
    "node": "Stickney",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Survival",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode4": {
    "node": "Drunlo",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode5": {
    "node": "Grildrig",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode6": {
    "node": "Limtoc",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode7": {
    "node": "Hall",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode8": {
    "node": "Reldresal",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode9": {
    "node": "Clustril",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode10": {
    "node": "Kepler",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Rush (Archwing)",
    "tileset": "Corpus Ship (Archwing)",
    "dark_sector": False
  },
  "SettlementNode11": {
    "node": "Gulliver",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Defense",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode12": {
    "node": "Monolith",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Rescue",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode13": {
    "node": "D'Arrest",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode14": {
    "node": "Shklovsky",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Spy",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode15": {
    "node": "Sharpless",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Mobile Defense",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "SettlementNode16": {
    "node": "Wendell",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode17": {
    "node": "Flimnap",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode18": {
    "node": "Opik",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode19": {
    "node": "Todd",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "SettlementNode20": {
    "node": "Iliad",
    "planet": "Phobos",
    "enemy": "Corpus",
    "type": "Assassination",
    "tileset": "Corpus Ship",
    "dark_sector": False
  },
  "MercuryHUB": {
    "node": "Larunda Relay",
    "planet": "Mercury",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "VenusHUB": {
    "node": "Vesper Relay",
    "planet": "Venus",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "EarthHUB": {
    "node": "Strata Relay",
    "planet": "Earth",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "SaturnHUB": {
    "node": "Kronia Relay",
    "planet": "Saturn",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "ErisHUB": {
    "node": "Kuiper Relay",
    "planet": "Eris",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "EuropaHUB": {
    "node": "Leonov Relay",
    "planet": "Europa",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "PlutoHUB": {
    "node": "Orcus Relay",
    "planet": "Pluto",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "ZarimanHub": {
	"node": "Chrysalith",
    "planet": "Zariman Ten Zero",
    "enemy": "Tenno",
    "type": "Relay",
    "dark_sector": False
  },
  "EventNode0": {
    "node": "Balor",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode1": {
    "node": "Tethra",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode2": {
    "node": "Operation Gate Crash",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode3": {
    "node": "Elatha",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode4": {
    "node": "Proxy Rebellion",
    "enemy": "Corpus",
    "type": "Survival",
    "dark_sector": False
  },
  "EventNode5": {
    "node": "Birog",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode6": {
    "node": "Tyl Reygor Seal Lab",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode7": {
    "node": "Proxy Rebellion",
    "enemy": "Corpus",
    "type": "Interception",
    "dark_sector": False
  },
  "EventNode8": {
    "node": "Corb",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode9": {
    "node": "Operation Gate Crash Pt. 2",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode10": {
    "node": "Lugh",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode11": {
    "node": "Nemed",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode12": {
    "node": "Operation Cryotic Front",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode13": {
    "node": "Shifting Sands",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode14": {
    "node": "Gate Crash",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode15": {
    "node": "Operation Cryotic Front",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode16": {
    "node": "Operation Cryotic Front",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode17": {
    "node": "Proxy Rebellion",
    "enemy": "Corpus",
    "type": "Defense",
    "dark_sector": False
  },
  "EventNode18": {
    "node": "Proxy Rebellion",
    "enemy": "Corpus",
    "type": "Defense",
    "dark_sector": False
  },
  "EventNode19": {
    "node": "Mars",
    "enemy": "Grineer",
    "type": "Defense",
    "dark_sector": False
  },
  "EventNode20": {
    "node": "Tyl Regor Sea Lab",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode22": {
    "node": "Tyl Regor Sea Lab",
    "enemy": "Sentient",
    "type": "Ancient Retribution",
    "dark_sector": False
  },
  "EventNode24": {
    "node": "Earth",
    "enemy": "Grineer",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode25": {
    "node": "Earth",
    "enemy": "Grineer",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode26": {
    "node": "Earth",
    "enemy": "Grineer",
    "type": "Exterminate",
    "dark_sector": False
  },
  "EventNode27": {
    "node": "Void",
    "enemy": "Corrupted",
    "type": "Survival",
    "dark_sector": False
  },
  "EventNode28": {
    "node": "Saturn",
    "enemy": "Grineer",
    "type": "Assassination",
    "dark_sector": False
  },
  "EventNode29": {
    "node": "Saturn",
    "enemy": "Grineer",
    "type": "Assassination",
    "dark_sector": False
  },
  "EventNode30": {
    "node": "Ganymede",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "EventNode31": {
    "node": "Ganymede",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "EventNode32": {
    "node": "Ganymede",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "EventNode33": {
    "node": "Ganymede",
    "planet": "Jupiter",
    "enemy": "Corpus",
    "type": "Disruption",
    "tileset": "Corpus Gas City",
    "dark_sector": False
  },
  "EventNode34": {
    "node": "Earth",
    "enemy": "Grineer",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode35": {
    "node": "Earth",
    "enemy": "Grineer",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode761": {
    "node": "The Index",
    "enemy": "Corpus",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode762": {
    "node": "The Index pt 2",
    "enemy": "Corpus",
    "type": "Arena",
    "dark_sector": False
  },
  "EventNode763": {
    "node": "The Index Endurance",
    "enemy": "Corpus",
    "type": "Arena",
    "dark_sector": False
  },
  "PvpNode0": {
    "node": "Conclave Capture the Cephalon",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode1": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode2": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode3": {
    "node": "Conclave Capture the Cephalon",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode4": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode5": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode6": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode7": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode8": {
    "node": "Conclave",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode9": {
    "node": "Conclave Team Domination",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode10": {
    "node": "Conclave Domination",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode11": {
    "node": "Conclave Domination",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode12": {
    "node": "Conclave Domination",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode13": {
    "node": "Tactical Alert: Snoball Fight!",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "PvpNode14": {
    "node": "Conclave: Quick Steel",
    "enemy": "Tenno",
    "type": "Conclave",
    "dark_sector": False
  },
  "ClanNode0": {
    "node": "Romula",
    "planet": "Venus",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Corpus Ship",
    "dark_sector": True,
    "bonus": "10%"
  },
  "ClanNode1": {
    "node": "Malva",
    "planet": "Venus",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Corpus Ship",
    "dark_sector": True,
    "bonus": "10%"
  },
  "ClanNode2": {
    "node": "Coba",
    "planet": "Earth",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Grineer Forest",
    "dark_sector": True,
    "bonus": "15%"
  },
  "ClanNode3": {
    "node": "Tikal",
    "planet": "Earth",
    "enemy": "Infested",
    "type": "Excavation",
    "tileset": "Grineer Forest",
    "dark_sector": True,
    "bonus": "15%"
  },
  "ClanNode4": {
    "node": "Sinai",
    "planet": "Jupiter",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Corpus Gas City",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode5": {
    "node": "Cameria",
    "planet": "Jupiter",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Corpus Gas City",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode6": {
    "node": "Larzac",
    "planet": "Europa",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Corpus Ice Planet",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode7": {
    "node": "Cholistan",
    "planet": "Europa",
    "enemy": "Infested",
    "type": "Excavation",
    "tileset": "Corpus Ice Planet",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode8": {
    "node": "Kadesh",
    "planet": "Mars",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Grineer Settlement",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode9": {
    "node": "Wahiba",
    "planet": "Mars",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Corpus Ship",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode10": {
    "node": "Memphis",
    "planet": "Phobos",
    "enemy": "Infested",
    "type": "Defection",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode11": {
    "node": "Zeugma",
    "planet": "Phobos",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode12": {
    "node": "Caracol",
    "planet": "Saturn",
    "enemy": "Infested",
    "type": "Defection",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode13": {
    "node": "Piscinas",
    "planet": "Saturn",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "20%"
  },
  "ClanNode14": {
    "node": "Amarna",
    "planet": "Sedna",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode15": {
    "node": "Sangeru",
    "planet": "Sedna",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Grineer Asteroid",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode16": {
    "node": "Ur",
    "planet": "Uranus",
    "enemy": "Infested",
    "type": "Disruption",
    "tileset": "Grineer Galleon",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode17": {
    "node": "Assur",
    "planet": "Uranus",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Galleon",
    "dark_sector": True,
    "bonus": "25%"
  },
  "ClanNode18": {
    "node": "Akkad",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Infested Ship",
    "dark_sector": True,
    "bonus": "30%"
  },
  "ClanNode19": {
    "node": "Zabala",
    "planet": "Eris",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Infested Ship",
    "dark_sector": True,
    "bonus": "30%"
  },
  "ClanNode20": {
    "node": "Yursa",
    "planet": "Neptune",
    "enemy": "Infested",
    "type": "Defection",
    "tileset": "Infested Ship",
    "dark_sector": True,
    "bonus": "30%"
  },
  "ClanNode21": {
    "node": "Kelashin",
    "planet": "Neptune",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Infested Ship",
    "dark_sector": True,
    "bonus": "30%"
  },
  "ClanNode22": {
    "node": "Seimeni",
    "planet": "Ceres",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Grineer Shipyard",
    "dark_sector": True,
    "bonus": "35%"
  },
  "ClanNode23": {
    "node": "Gabii",
    "planet": "Ceres",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Grineer Galleon",
    "dark_sector": True,
    "bonus": "35%"
  },
  "ClanNode24": {
    "node": "Sechura",
    "planet": "Pluto",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Corpus Outpost",
    "dark_sector": True,
    "bonus": "35%"
  },
  "ClanNode25": {
    "node": "Hieracon",
    "planet": "Pluto",
    "enemy": "Infested",
    "type": "Excavation",
    "tileset": "Corpus Outpost",
    "dark_sector": True,
    "bonus": "35%"
  },
  "/Lotus/Types/Keys/SortieBossKeyPhorid": {
    "node": "Sortie Boss: Phorid",
    "enemy": "Infested",
    "type": "Assassination",
    "tileset": "Grineer Asteroid",
    "dark_sector": False
  },
  "SolNode706": {
    "node": "Horend",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Capture",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode707": {
    "node": "Hyf",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Defense",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode708": {
    "node": "Phlegyas",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Exterminate",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode709": {
    "node": "Dirus",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Mobile Defense",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode710": {
    "node": "Formido",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Sabotage",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode711": {
    "node": "Terrorem",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Survival",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode712": {
    "node": "Magnacidium",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Assassinate",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode713": {
    "node": "Exequias",
    "planet": "Deimos",
    "enemy": "Infested",
    "type": "Assassinate",
    "tileset": "Orokin Derelict",
    "dark_sector": False
  },
  "SolNode717": {
    "node": "Persto",
    "planet": "Deimos",
    "enemy": "Murmur",
    "type": "Survival",
    "tileset": "Albrecht's Laboratories",
    "dark_sector": False
  },
  "SolNode718": {
    "node": "Cambire",
    "planet": "Deimos",
    "enemy": "Murmur",
    "type": "Alchemy",
    "tileset": "Albrecht's Laboratories",
    "dark_sector": False
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
        "VoidT5": "Requiem",
        "VoidT6": "Omnia"
    },
    "era_key": {
        "Normal": "Modifier",
        "Steel Path": "Modifier",
        "Void Storms": "ActiveMissionTier"
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
        "Steel Path Void Cascade": 3,
        "Void Cascade": 5,
        "Void Flood": 5,
        "Alchemy": 5
    }
}

SORT_ORDER = {'Lith': 0,
              'Meso': 1,
              'Neo': 2,
              'Axi': 3,
              'Requiem': 4,
              'Omnia': 5}

fissure_types = ['normal', 'sp', 'vs', 'requiem']

# ------------------------------
# Logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# ------------------------------
