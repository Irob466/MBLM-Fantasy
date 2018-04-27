# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 01:07:55 2018

@author: Irob4
"""

import getpass
import goldsberry
import pandas as pd
import psycopg2
import itertools
import json
import csv
import io
import re
from timeit import default_timer as timer
from sqlconversion import *

class PostgresConnection():
  """
  Handles the Postgres connection I/O with a class-level connection and cursor
  """
  def __init__(self):
    self.conn = self._authenticate()
    self.cur = self._get_cursor()
    self.time_format = '(?:\d:|[01]\d:|2[0-3]:|[01]\d|2[0-3])[0-5]\d(?: AM| PM|AM|PM|)'
    
  def __enter__(self):
    return self
    
  def __exit__(self, exception_type, exception_value, traceback):
    if not self.cur.closed:
      # close cursor
      self.cur.close()
    if not self.conn.closed:
      # rollback any uncommitted transactions and close
      self.conn.rollback()
      self.conn.close()
    
    print('Goodbye!')
    
  def _authenticate(self):
    """ 
    Prompt for database name, user, and password for the psycopg2 connection.
    If invalid data is input, prompt for information until valid.
    
    Returns a psycopg2 connection to the database named in the input.
    """
    
    while 1:
      try:
        conn = psycopg2.connect("dbname=" + input("Database: ") + " user=" + input("User: ") + " password=" + getpass.getpass("Password: "))
      except psycopg2.Error as e:
        print(e)
      else:
        break
    return conn
  
  def _get_cursor(self):
    """
    Takes pyscopg2 connection input, checks to see if the connection is open, and 
    opens a cursor.
    
    Returns psycopg2 connection cursor
    """
    
    try:
      self.conn
    except NameError:
      self.conn = _authenticate()
      
    if self.conn.closed:
      self.conn = _authenticate()
    else:
      cur = self.conn.cursor()
    return cur
  
  def _check_connection(self):
    """
    Checks to see that the cursor and connection are both open. The get_cursor
    function already validates the connection, so only the get_cursor function
    is called. 

    Returns nothing.
    """
  
    try:
      if self.cur.closed:
        self.cur = self._get_cursor()
    except NameError:
      self.cur = self._get_cursor()
    
    return
  
  def _check_constraints(self, df, table_name, schema='current'):
    """
    Updates the dataframe to remove values that fail foreign key constraints
    on the table being updated. Uses the foreign_keys_view as a shorthand
    for the information_schema query.
    """
    self._check_connection()
    
    foreign_ids = {}
    
    try:
      # Query for all columns that have a foreign key in the current table
      self.cur.execute("SELECT source_column, target_table, target_column FROM {schema}.foreign_keys_view WHERE source_table = '{schema}.{table_name}'::regClass".format(
          schema=schema,table_name=table_name))
      results = self.cur.fetchall()
      # Create a dict of column names as keys and a tuple of (schema.table, column name) for foreign keys
      result_dict = {key: None for (key,ftab,fcol) in results}
      for key in result_dict.keys():
        result_dict[key] = [result[1:] for result in results if result[0]==key]
      # Set of unique table, column tuples to query to check contstraints for
      foreign_id_set = set([value for value in itertools.chain.from_iterable(result_dict.values())])
      # Get the ids for all table, column tuples
      for id_set in foreign_id_set:
        self.cur.execute("SELECT {id} FROM {table}".format(id=id_set[1], table=id_set[0]))
        foreign_ids[id_set] = [id for (id,) in self.cur.fetchall()]
      # Restrict the results for only ids that satisfy the foreign key restraints
      for key in result_dict.keys():
        df = df[df[key].isin(itertools.chain.from_iterable(foreign_ids[n] for n in result_dict[key]))]
      
    except psycopg2.Error as e:
      self.conn.rollback()
      
    return df
  
  def _clean_json(self,data):
      return json.dumps(data)
    
  def _clean_time(self,data):
    if re.search(self.time_format, data) != None:
      return re.search(self.time_format, data).group(0)
    else:
      return ''
  
  def _clean_numeric(self,data):
    return pd.to_numeric(data, errors='coerce')
    
  def _clean(self,data,method,dt):
    """
    If data type from Postgres is ARRAY, then apply method to all members of the
    list and then mogrify the list, otherwise apply the method to the data
    """
    
    if dt=='ARRAY':
      try:
        return self.cur.mogrify('%s',([method(x) for x in data],))
      except TypeError as e:
        print('Data type in Postgres is an ARRAY')
        raise(e)
    else:
      return method(data)

  def _clean_dataframe(self, df, table_name, schema='current'):
    """
    Replaces all blank strings and NaN for ID columns and then updates dtype to int.
    Also applies cleaning functions to data based on data type in postgres.
    """
    
    # Clean IDs
    for id in [x for x in df.columns if x[-2:] == 'id']:   
      df[id] = df[id].replace('',0).fillna(0).astype(int)
    
    df.fillna('', inplace=True)
    
    # Clean rest of columns based on Postgres data types
    self._check_connection()
    self.cur.execute(self.cur.mogrify("SELECT column_name, udt_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                                 (schema,table_name)))
    res = self.cur.fetchall()
    
    for col in res:
      if col[1].endswith('json'):
        df[col[0]] = df[col[0]].apply(self._clean,method=self._clean_json, dt=col[2])
      elif col[1].endswith('time'):
        df[col[0]] = df[col[0]].apply(self._clean,method=self._clean_time, dt=col[2])
      # elif col[1].endswith('numeric'):
        # df[col[0]].apply(_clean,method=_clean_numeric)
  
    return df
      
  def to_pg(self, df, table_name, schema='current'):
    """
    Inserts data from dataframe into Postgres connection using StringIO
    """
    
    start = timer()
    
    self._check_connection()
    
    df = self._check_constraints(self._clean_dataframe(df, table_name, schema), table_name, schema)
    
    """
    Clean the dataframe by dropping all IDs already in the target table, and
    also rearranging column headers to match the target table. Assumes that the 
    NBA data does not have any NaN values.
    """
    
    try:
      # check current IDs in the table, assumes that ID column is named TABLENAME_ID
      self.cur.execute('SELECT {id} FROM {schema}.{table}'.format(id=table_name + '_id', schema=schema, table=table_name))
      existing_ids = [id for (id,) in self.cur.fetchall()]
      # if the ID is already in the table, remove it from the import data
      df = df[~df[table_name + '_id'].isin(existing_ids)]
    except psycopg2.Error as e:
      # rollback the failed transaction
      self.conn.rollback()

    self.cur.execute('SELECT * FROM {schema}.{table}'.format(schema=schema, table=table_name))
    table_headers = [desc[0] for desc in self.cur.description]
    df = df[table_headers]
    
    output = io.StringIO()
    # create CSV from dataframe ignoring the index
    df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    output.getvalue()
    # jump to start of stream
    output.seek(0)
    
    self.cur.copy_from(output, '{schema}.{table}'.format(schema=schema, table=table_name), null='')
    self.conn.commit()
    self.cur.close()
    end = timer()
    
    print('Imported ' + str(df.shape[0]) + ' rows in table ' + table_name + ' using schema ' + schema + ' in ' + str((end-start)))
    
    return
  
  def from_pg(self, table_name, schema='current'):
    """
    Executes a SELECT * query for the table specified in the parameters
    """
    
    start = timer()
    self._check_connection()
    self.cur.execute("SELECT * FROM {schema}.{table}".format(schema=schema, table=table_name))
    results = self.cur.fetchall()
    rowcount = self.cur.rowcount
    self.cur.close()
    end = timer()
    
    print('Returned ' + str(rowcount) + ' rows in ' + str((end-start)))
    
    return results

with PostgresConnection() as c:
  d = goldsberry.masterclass.MiscDataProvider('2017')
  # Get teams that are listed as NBA franchises
  df = pd.DataFrame(d.teams()).rename(columns=team_columns)
  df = df[df.is_nba_franchise]
  c.to_pg(df, 'team')
    
  # Get all players
  df = pd.DataFrame(d.players()).rename(columns=player_columns)
  # Since the datatype of the teams column is a list, convert it to a dict for JSON
  c.to_pg(df, 'player')
    
  # Get all coaches
  df = pd.DataFrame(d.coaches()).rename(columns=coach_columns)
  c.to_pg(df, 'coach')
    
  # Get all regular season games, drop the playoff column
  df = pd.DataFrame(d.schedule()).rename(columns=game_columns)
  df.drop(labels='playoffs', axis=1, inplace=True)
  df = df[df.season_stage_id == 2]
  # Since the json comes over as objects, convert the IDs to int64
  df['game_id'] = df['game_id'].astype(int)
  # Turn the period, h_team and v_team JSONs into columns for the dataframe
  period = df['period'].apply(pd.Series).rename(columns=period_columns)
  period.drop(labels='type', axis=1, inplace=True)
  h_team = df['hTeam'].apply(pd.Series).rename(columns={'teamId': 'id'})
  h_team.rename(columns=dict(zip(list(h_team.columns.values),
                                 ['h_team_' + team_col for team_col in h_team.columns.values])), inplace=True)
  v_team = df['vTeam'].apply(pd.Series).rename(columns={'teamId': 'id'})
  v_team.rename(columns=dict(zip(list(v_team.columns.values),
                                 ['v_team_' + team_col for team_col in v_team.columns.values])), inplace=True)
  df = pd.concat([df.drop(columns=['period','hTeam','vTeam']),period,h_team,v_team],axis=1,join='inner')
  c.to_pg(df, 'game')