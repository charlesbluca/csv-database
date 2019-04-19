# Charles Blackmon-Luca, ccb2158

import sys
from time import time

sys.path.append("../src/")

from CSVDataTable import CSVDataTable

with open("../test_output/advanced_test.txt", "w+") as file:

    # open csv as table
    people = CSVDataTable("People", key_columns=["playerID"], loadit=True)
    file.write("Loading CSVDataTable:\n\n%s\n" % people)
    
    # add tons of indexes
    people.add_index("birthYear", "INDEX", ["birthYear"])
    people.add_index("birthState", "INDEX", ["birthState"])
    people.add_index("birthCity", "INDEX", ["birthCity"])
    
    file.write("Added indexes:\n\n")
    for idx in people._indexes.values():
        file.write("%s\n" % idx)
    
    # open second csv as table
    batting = CSVDataTable("BattingSmall", key_columns=["playerID", "teamID", "yearID", "stint"], loadit=True)
    file.write("Loading CSVDataTable:\n\n%s\n" % batting)
        
    # join the two tables and get complicated fields
    columns = ["playerID"]
    tmp = {"People.birthYear" : "1850"}
    fields = ['BattingSmall.teamID', 'BattingSmall.yearID', 'BattingSmall.stint', 'BattingSmall.playerID']
    file.write("Joining on columns %s, where %s with fields %s\n\n" % (columns, tmp, fields))
    file.write("Result : %s\n\n" % people.join(batting, columns, tmp, fields))

    