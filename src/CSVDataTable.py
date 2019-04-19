# Charles Blackmon-Luca, ccb2158

import json
from csv import DictReader
from Index import Index
from Exceptions import CSVTableError, CSVIndexError

csv_path = "../CSVFile/"
json_path = "../DB/"
       
class CSVDataTable:
 
    def __init__(self, table_name, columns=[], key_columns=[], loadit=False):
    
        # initialize based on inputs
        self._table_name = table_name
        self._columns = columns
        self._key_columns = key_columns
        self._next_rid = "0"
        self._rows = {}
        self._indexes = {}
                
        # load in from json or csv if possible
        if loadit:
            self.load()
            
        # make default primary index if it doesn't exist
        if "PRIMARY" not in self._indexes and key_columns != []:
            self.add_index("PRIMARY", "PRIMARY", self._key_columns)
            
    def __str__(self):
        result = ("%s\n"
                  "table name : %s\n"
                  "key columns : %s\n") % (type(self), self._table_name,
                                            self._key_columns)

        # Find out how many rows are in the table.
        rows = len(self._rows)
        result += "number of rows : %s\n" % rows

        # Get the first five rows and print to show sample data.
        result += "first five rows:\n"
        limit = 5 if len(self._rows) >= 5 else len(self._rows)
        i = 0
        for v in self._rows.values():
            if i == 5:
                break
            else:
                result += "%s\n" % v
                i += 1
        return result
                  
    def insert(self, row):
        
        # check that row contains all primary keys
        for key in self._key_columns:
            if key not in row.keys():
                raise CSVTableError("Field '%s' doesn't have a default value" % key)
                
        # make full record for row, insert into table
        rec = {k : "" if k not in row else row[k] for k in self._columns}
        self._rows[self._next_rid] = rec
        
        # insert into all indexes
        for idx in self._indexes.values():
            idx.add_to_index(rec, self._next_rid)
            
        # update next rid
        self._next_rid = str(int(self._next_rid)+1)
        
    def import_data(self, rows):
        
        for row in rows:
            self.insert(row)
        
    def find_by_template(self, tmp, field_list=None, index_allowed=True):
    
        # input validation
        if not tmp.keys() <= set(self._columns):
            raise CSVTableError("Template contains foreign column(s)")
        
        # get fields from field list
        if field_list is not None:
            if not set(field_list) <= set(self._columns):
                raise CSVTableError("Field list contains foreign column(s)")
            else:
                fields = field_list
        else:
            fields = self._columns
        
        # try indexing if allowed
        if index_allowed:
            best_idx = self.best_query_index(tmp)
            if best_idx is not None:
                rows = {k : {c : t for c, t in v.items() if c in fields}
                        for k, v in best_idx.find_rows(tmp).items()}
                index_failed = False
            else:
                index_failed = True
                    
        # otherwise use list comprehension alone
        if not index_allowed or index_failed:
            rows = {}
            for rid, row in self._rows.items():
                if tmp.items() <= row.items():
                    rows[rid] = {k : v for k, v in row.items() if k in fields}
                   
        # form new CSVDataTable
        new_keys = [v for v in self._key_columns if v in set(fields)]
        result = CSVDataTable(self._table_name + "_fbt", fields, new_keys)
        result._rows = rows
        result._next_rid = self._next_rid
        for idx in self._indexes.values():
            if set(idx._columns) <= set(fields):
                result.add_index(idx._name, idx._kind, idx._columns)
                
        return result
        
    def delete(self, tmp):
        
        # find rows to delete
        delete = self.find_by_template(tmp)
        result = len(delete._rows)
        
        # delete rows from table
        for rid in delete._rows:
            del self._rows[rid]
            
        # delete rows from indexes
        for name, idx in delete._indexes.items():
            for value in idx._index_data:
                for rid in idx._index_data[value]:
                    del self._indexes[name]._index_data[value][rid]
            
        return result
        
    def add_index(self, name=None, kind=None, columns=None, json=None):
    
        # input validation
        if kind is not None and kind not in ["PRIMARY", "UNIQUE", "INDEX"]:
            raise CSVTableError("Index must be of the following: "
                                "PRIMARY,UNIQUE,INDEX")
        if columns is not None and not set(columns) <= set(self._columns):
            raise CSVTableError("Index must use columns of the "
                                "containing database")
            
        # make sure primary index is kept singular
        idx = Index(name, self, kind, columns, json)
        if kind == "PRIMARY":
            self._indexes["PRIMARY"] = idx
        else:
            self._indexes[name] = idx
            
    def remove_index(self, name):
        
        # check that we are not removing primary index
        if self._indexes[name]._kind == "PRIMARY":
            raise CSVTableError("Cannot remove PRIMARY index")
        else:
            del self._indexes[name]
            
    def best_query_index(self, tmp):
    
        min = int(self._next_rid)
        min_idx = None
    
        # find index with least rows for template
        for idx in self._indexes.values():
            if idx.matches_index(tmp):
                value = idx.compute_index_value(tmp)
                rows = idx._index_data.get(value, {})
                if len(rows) < min:
                    min = len(rows)
                    min_idx = idx
                
        return min_idx
        
    def best_join_index(self, other_table, on_columns):
    
        tmp = {k : "" for k in on_columns}
        
        min = int(self._next_rid) + int(other_table._next_rid)
        min_idx = None
        
        # find index with smallest average number of rows
        for idx in list(self._indexes.values()) + list(other_table._indexes.values()):
            if idx.matches_index(tmp):
                rows = [len(v) for v in idx._index_data.values()]
                avg = sum(rows) / len(rows)
                if avg < min:
                    min = avg
                    min_idx = idx
                    
        return min_idx
        
    def save(self):
    
        # make json object of metadata
        d = {
            "state" : {
                "table_name" : self._table_name,
                "columns" : self._columns,
                "key_columns" : self._key_columns,
                "next_rid" : self._next_rid
            },
            "rows" : self._rows,
            "indexes" : { k : self._indexes[k].to_json() for k in self._indexes}
        }
        
        # save to file
        with open(json_path + self._table_name + ".json", "w+") as file:
            json.dump(d, file, indent=4)
            
    def load(self):
    
        # check if we can load in json file
        try:
            with open(json_path + self._table_name + ".json", "r") as file:
                d = json.load(file)
                self._columns = d["state"]["columns"]
                self._key_columns = d["state"]["key_columns"]
                self._next_rid = d["state"]["next_rid"]
                self._rows = d["rows"]
                
                # add indexes
                for k,v in d["indexes"].items():
                    self.add_index(name=k, json=v)
                
        # if not try to load in csv file
        except FileNotFoundError:
            try:
                with open(csv_path + self._table_name + ".csv", "r") as file:
                    reader = DictReader(file)
                    self._columns = reader.fieldnames
                    self.import_data([dict(row) for row in reader])
                    
            # if not throw error
            except FileNotFoundError:
                raise
                
    def join(self, other_table, on_columns, where_template=None,
             field_list=None):
        
        # input validation
        if not (set(on_columns) <= set(self._columns) 
            and set(on_columns) <= set(other_table._columns)):
            raise CSVTableError("On condition must match columns of " 
                            "left and right tables")       
        if field_list is not None:
            fields = [s.split(".")[1] for s in field_list]
            if not set(fields) <= set(self._columns) | set(other_table._columns):
                raise CSVTableError("Field list contains foreign column(s)")
                
        # perform select operations first if needed
        if where_template is not None:
            self_tmp = {}
            other_tmp = {}
            for k, v in where_template.items():
                name, column = k.split(".")
                if name == self._table_name:
                    self_tmp[column] = v
                if name == other_table._table_name:
                    other_tmp[column] = v
            return (self.find_by_template(self_tmp)
                        .join(other_table.find_by_template(other_tmp),
                              on_columns, field_list=field_list))
                              
        # check which table has best index for probing
        join_idx = self.best_join_index(other_table, on_columns)
        if join_idx is None:
            swap = False
        elif join_idx._table is other_table:
            swap = False
        else:
            swap = True
        
        # create resulting table
        result = CSVDataTable(self._table_name + "_" + other_table._table_name,
                               list(set(self._columns + other_table._columns)),
                       list(set(self._key_columns + other_table._key_columns)))
            
        # perform scan and probe operations
        if swap:
            for scan_row in other_table._rows.values():
                tmp = {k : v for k, v in scan_row.items() if k in on_columns}
                for probe_row in self.find_by_template(tmp)._rows.values():
                    result.insert({**scan_row, **probe_row})
        else:
            for scan_row in self._rows.values():
                tmp = {k : v for k, v in scan_row.items() if k in on_columns}
                for probe_row in other_table.find_by_template(tmp)._rows.values():
                    result.insert({**scan_row, **probe_row})

        # add indexes to the populated table
        for idx in list(self._indexes.values()) + list(other_table._indexes.values()):
            if idx._kind != "PRIMARY":
                try:
                    result.add_index(idx._name, idx._kind, idx._columns)
                except CSVIndexError:
                    pass
                    
        if field_list is None:
            return result
        else:
            return result.find_by_template({}, field_list=fields)
                    