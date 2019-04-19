# Charles Blackmon-Luca, ccb2158

import sys
from time import time

sys.path.append("../src/")

from CSVDataTable import CSVDataTable

with open("../test_output/index_test.txt", "w+") as file:

    # open csv as table
    db = CSVDataTable("People", key_columns=["playerID"], loadit=True)
    file.write("Loading CSVDataTable:\n\n%s\n" % db)
    
    # add year index
    db.add_index("birthYear", "INDEX", ["birthYear"])
    file.write("New index: %s\n" % db._indexes["birthYear"])
    
    # compare FBT with and without index
    tmp = {"birthYear" : "1971", "birthCity" : "Denver"}
    file.write("FBT using template: %s\n" % tmp)
    start = time()
    for i in range(1000):
        db.find_by_template(tmp, index_allowed=False)
    end = time()
    file.write("Time to execute 1000 FBTs without indexes: %s s\n" % (end - start))
    start = time()
    for i in range(1000):
        db.find_by_template(tmp, index_allowed=True)
    end = time()
    file.write("Time to execute 1000 FBTs with indexes: %s s\n\n" % (end - start))
    
    # try to add bad index
    file.write("Bad UNIQUE index: \n")
    try:
        db.add_index("birthState", "UNIQUE", ["birthState"])
    except Exception as e:
        file.write("%s\n" % str(e))
        
    # check if indexes persist through save/load
    file.write("Saving DB to JSON file ...\n")
    db.save()
    file.write("Loading JSON file ...\n\n")
    new_db = CSVDataTable("People", loadit=True)
    
    file.write("Indexes of loaded table: %s" % new_db._indexes)