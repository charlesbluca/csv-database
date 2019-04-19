# Charles Blackmon-Luca, ccb2158

import sys

sys.path.append("../src/")

from CSVDataTable import CSVDataTable

with open("../test_output/insert_test.txt", "w+") as file:

    # open csv as table
    db = CSVDataTable("ssol_dummy", key_columns=["Uni"], loadit=True)
    file.write("Loading CSVDataTable:\n\n%s\n" % db)
    
    # add new entry and find it
    row = {"Uni" : "ccb2158",
           "First_Name" : "Charles",
           "Last_Name": "Luca"}
    db.insert(row)
    file.write("Insert row: %s\n\n" % row)
    file.write("Row in table:\n\n%s\n" % db.find_by_template({"Uni" : "ccb2158"}))
    
    # try to insert duplicate entry
    file.write("Insert row: %s\n\n" % {"Uni" : "bn0523"})
    try:
        db.insert(row)
    except Exception as e:
        file.write("%s\n" % str(e))
        
    # remove rows added
    file.write("Delete by template: %s\n\n" % {"Uni" : "ccb2158"})
    db.delete({"Uni" : "ccb2158"})
    file.write("Row in table:\n\n%s\n" % db.find_by_template({"Uni" : "ccb2158"}))