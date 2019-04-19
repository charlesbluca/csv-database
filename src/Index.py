# Charles Blackmon-Luca, ccb2158

from Exceptions import CSVIndexError

class Index:

    def __init__(self, name=None, table=None, kind=None, columns=None, json=None):
    
        self._name = name
        self._table = table
        self._columns = columns
        self._kind = kind
        self._index_data = {}
        
        if json is not None:
            self.from_json(json)
        else:
            self.build()
        
    def __str__(self):
        
        return ("%s\n"
                "name : %s\n"
                "table : %s\n"
                "columns : %s\n"
                "kind : %s\n") % (type(self),self._name, self._table._table_name,
                                  self._columns, self._kind)
        
    def compute_index_value(self, row):
        
        return "_".join([row[k] for k in row if k in self._columns])
        
    def add_to_index(self, row, rid):
        
        value = self.compute_index_value(row)
        rows = self._index_data.get(value, {})
        
        # make sure we are not creating duplicate entry in unique index
        if self._kind in ["PRIMARY", "UNIQUE"] and rows != {}:
            raise CSVIndexError("Insert has created a duplicate entry:\n %s" % self)
        else:
            rows[rid] = row
            self._index_data[value] = rows

    def remove_from_index(self, row, rid):
        
        value = self.compute_index_value(row)
        del self._index_data[value][rid]
        
    def build(self):
        
        for rid, row in self._table._rows.items():
            self.add_to_index(row, rid)
        
    def matches_index(self, tmp):
        
        return set(self._columns) <= set(tmp.keys())
        
    def find_rows(self, tmp):

        value = self.compute_index_value(tmp)         
        result = {}
        
        # check index of template for results
        rows = self._index_data.get(value, {})
        for rid, row in rows.items():
           if tmp.items() <= row.items():
               result[rid] = row
                
        return result
         
    def to_json(self):
    
        return {
            "name" : self._name,
            "columns" : self._columns,
            "kind" : self._kind,
            "index_data" : self._index_data
        }
        
    def from_json(self, json):
        
        self._name = json["name"]
        self._columns = json["columns"]
        self._kind = json["kind"]
        self._index_data = json["index_data"]
