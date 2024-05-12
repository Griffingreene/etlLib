"""
 etllib.py -- Module for ETL processes in python

 functions designed to be easily applied in other scripts to access data files, parse content and reformat

 Author: Griffin Greene (griffincgreene@gmail.com)
 """

import io
import csv
import sqlite3
import json


                                                                        # Accesses SQL table and returns a CSV string of all info
def sql2csv(query, conn):                                               # Takes query as string and sqlite data connection object
    cursor = conn.cursor()
    cursor.execute(query)

    headers = [cols[0] for cols in cursor.description]                  # Extract column names from SQL table specified in query
  
    writestring = io.StringIO()
    writer = csv.writer(writestring)                                    # Uses StringIO object to maintain create string in CSV format

    writer.writerow(headers)
    for line in cursor:                                                 # Writes all rows of quried table to CSV stile string
        writer.writerow(line)

    return writestring.getvalue()

                                                                        # Converts SQL table to JSON string
def sql2json(query, conn, format='lod',primary_key=None):               # Query as string, sqlite data connection object
                                                                        # Format of JSON string returned, column to used as key for dict of dicts
    cursor = conn.cursor()
    cursor.execute(query)
    headers = [cols[0] for cols in cursor.description]                  # Get SQL column names

    if format == 'lod':                                                 # List of dictionaries option
        lod = []
        for row in cursor:                                              # Create dict for each row with column name as key
            inner_dict = dict(zip(headers, row))                        
            lod.append(inner_dict)

        return json.dumps(lod)                                          # Return as JSON string


    if format == 'dod':                                                 # Dictionary of dictionaries option
        try:
            key_idx = headers.index(primary_key)                        # Check existence of specified column for key and stores index
        except ValueError:
            print(f'A valid primary key is required for fromat dod, {primary_key} is not a column')
            exit()
        
        headers.pop(key_idx)                                            # Remove primary key from header
        dod = {}
        for row in cursor:                                              # Iterates of rows
            row = list(row)
            outer_key = row.pop(key_idx)                                # Removes primary key from row
            inner_dict = dict(zip(headers,row))                         # Creates dict of other values
            dod[outer_key] = inner_dict


        return json.dumps(dod)



                                                                       # Read CSV file and write to SQL table
def csv2sql(filename, conn, table):                                    # Path to file, sqlite connection, SQL table to write to
    fh = open(filename)
    reader = csv.reader(fh)
    
    cursor = conn.cursor()
    
    headers = next(reader)                                             # Get number of columns in CSV
    q_marks = '?' * len(headers)
    parameter_tokens = ', '.join(q_marks)                              # Create SQL token sring with tokens to match number of columns
    query = f"INSERT INTO {table} VALUES({parameter_tokens})"          # Create query to insert row into specified table
    
    for row in reader:                                                 # Execute insert query for each row in CSV file
        cursor.execute(query, row)

    conn.commit()
    conn.close()
    fh.close()

                                                                       # Read JSON file contents and insert into SQL table
def json2sql(filename, conn, table):                                   # Path JSON file to read, sqlite connection, SQL table to insert
    j_file = open(filename)
    parsed_json = json.load(j_file)                                    # Store JSON contents

    num_values = len(next(iter(parsed_json.values())))                 # Gets number of key value pairs that will become columns
    
    for i in parsed_json.keys():                                       # Checks each JSON dictionary to confirm they are the same length                                
        in_dict_len = len(parsed_json[i])                              # terminates code if they ar not
        if in_dict_len != num_values:
            print("Dictionaries in JSON file must be the same lenghth.")
            exit()


    cursor = conn.cursor()

    q_marks = '?' * num_values                                         # Create sting for VALUES command with same number of tokens as columns
    parameter_tokens = ', '.join(q_marks)
    query = f"INSERT INTO {table} VALUES({parameter_tokens})"

    for idict in parsed_json:                                          # Extract values from each dictionary
        values = list(parsed_json[idict].values())                     # Executes insert command for resulting list of values
        cursor.execute(query, values)

    conn.commit()
    conn.close()
    j_file.close()




