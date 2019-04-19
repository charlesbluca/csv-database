# Charles Blackmon-Luca, ccb2158

import sys

sys.path.append("../src/")

from CSVDataTable import CSVDataTable

with open("../test_output/fbt_test.txt", "w+") as file:

    # open csv as table
    db = CSVDataTable("People", key_columns=["playerID"], loadit=True)
    file.write("Loading CSVDataTable:\n\n%s\n" % db)
    
    # find all 1871ers
    tmp = {"birthYear" : "1871"}
    file.write("Find by template : %s\n\n" % tmp)
    db = db.find_by_template(tmp)
    file.write("Result:\n\n%s\n" % db)
    
    # limit our fields list
    fields = ["nameFirst", "nameLast", "deathYear"]
    file.write("Limit to fields : %s\n\n" % fields)
    db = db.find_by_template({}, field_list=fields)
    file.write("Result:\n\n%s\n" % db)