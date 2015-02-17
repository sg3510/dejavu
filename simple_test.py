from dejavu import Dejavu
import json
import sys
sys.path.append('../../Server/')
import models
import warnings
warnings.filterwarnings("ignore")

# load config from a JSON file (or anything outputting a python dictionary)
with open("dejavu.cnf") as f:
    config = json.load(f)

bundles = models.Training_DB.objects(user = 'sebgrubb', bundle = 'dining_room')
print bundles

# create a Dejavu instance
djv = Dejavu(config)
djv.fingerprint_directory("mp3", [".wav"],'seb','kitchen', False, 3)
