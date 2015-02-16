from dejavu import Dejavu
import warnings
import json
warnings.filterwarnings("ignore")

# load config from a JSON file (or anything outputting a python dictionary)
with open("dejavu.cnf") as f:
    config = json.load(f)

# create a Dejavu instance
djv = Dejavu(config)
djv.fingerprint_directory("mp3", [".wav"], 3)
