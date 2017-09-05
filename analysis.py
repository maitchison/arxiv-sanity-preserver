"""
Statistical analysis of papers.
"""


import pickle
import dateutil.parser
import matplotlib.pyplot as plt
from os import path

from sqlite3 import dbapi2 as sqlite3
from utils import safe_pickle_dump, Config

KEY_WORDS = ["machine learning", "tensorflow", "caffe", "pytorch"]

sqldb = sqlite3.connect(Config.database_path)
sqldb.row_factory = sqlite3.Row # to return dicts rather than tuples

def word_search(pids, words):
    """ Returns list of document PIDs for each given word. """

    result = {}

    for pid in pids:

        result[pid] = []

        filename = "./data/txt/"+pid+".pdf.txt"
        if not path.exists(filename):
            print("missing: ",filename)
            continue
        print("reading: ", filename)
        f = open(filename, "r", encoding="utf8")

        s = str(f.read())

        for word in words:
            if word.upper() in s:
                result[pid].append(word)
        f.close()

CACHE = {}

print('loading the paper database', Config.db_path)
db = pickle.load(open(Config.db_path, 'rb'))

documents_by_month = {}
pids = []

# process data

print('decorating the database with additional information...')
for pid,p in db.items():
    timestruct = dateutil.parser.parse(p['published'])
    id = p['id']
    filename = id.split("/")[-1]
    pids.append(filename)
    date_code = timestruct.strftime("%Y-%m") # store in struct for future convenience
    documents_by_month[date_code] = [pid] if date_code not in documents_by_month else documents_by_month[date_code] + [pid]


print("Scanning files...")
word_search(pids, KEY_WORDS)
print("Done.")


# show articles per month

keys = list(documents_by_month.keys())
keys.sort()

values = []

for key in keys:
    values.append(len(documents_by_month[key]))

plt.plot(values)

print(keys,values)

plt.show()