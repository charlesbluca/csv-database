# Charles Blackmon-Luca, ccb2158

# base class for all errors
class Error(Exception):
    pass
    
# specifically for errors in Index class
class CSVIndexError(Error):
    pass
    
# specifically for errors in CSVDataTable class
class CSVTableError(Error):
    pass