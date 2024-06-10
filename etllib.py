"""
 etllib.py -- Module for ETL processes in python

 functions designed to be easily applied in other scripts to access data files, parse content and reformat

 Author: Griffin Greene
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
                                                                       # assumes table exists and column order matches

def csv2sql(filename, conn, table, unique_col=None):                   # Path to file, sqlite connection, SQL table to write to, col name tocheck duplicate
    fh = open(filename)
    reader = csv.reader(fh)

    cursor = conn.cursor()

    headers = next(reader)                                              # Get name of columns in CSV

    q_marks = '?' * len(headers)                                        # Create '?' string equal to number of columns in CSV
    parameter_tokens = ', '.join(q_marks)                               # Create SQL token sring with tokens to match number of columns
    insert = f"INSERT INTO {table} VALUES({parameter_tokens})"          # Query to insert row into specified table


    if unique_col:
        unique_col_idx = headers.index(unique_col)

        cursor.execute(f"SELECT {unique_col} FROM {table}")

        key_col_vals = [item for sublist in cursor.fetchall() for item in sublist]



        for row in reader:                                              # Execute insert query for each row in CSV file
            if row[unique_col_idx] not in key_col_vals:                 # that is not in the list of values in the header
                cursor.execute(insert, row)
    else:
        for row in reader:                                               # Execute insert query for each row in CSV file
            cursor.execute(insert, row)

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




