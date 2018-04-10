# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 01:07:55 2018

@author: Irob4
"""

import getpass
import os
import goldsberry
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import Json
import io
from timeit import default_timer as timer

class PostgresConnection():
  """
  Handles the Postgres connection I/O with a class-level connection and cursor
  """
  def __init__(self):
    self.conn = self.__authenticate()
    self.cur = self.__get_cursor()
    
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
  
  def to_pg(self, df, table_name, schema='current'):
    """
    Inserts data from dataframe into Postgres connection using StringIO
    """
    
    start = timer()
    
    self._check_connection()
    
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
      df = df[~df[str.upper(table_name) + '_ID'].isin(existing_ids)]
    except psycopg2.Error as e:
      # rollback the failed transaction
      self.conn.rollback()

    self.cur.execute('SELECT * FROM {schema}.{table}'.format(schema=schema, table=table_name))
    table_headers = [str.upper(desc[0]) for desc in self.cur.description]
    df = df[table_headers]
    
    output = io.StringIO()
    # create CSV from dataframe ignoring the index
    df.to_csv(output, sep='\t', header=False, index=False)
    output.getvalue()
    # jump to start of stream
    output.seek(0)
    
    self.cur.copy_from(output, '{schema}.{table}'.format(schema=schema, table=table_name))
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
  player = goldsberry.PlayerList()
  player2017 = pd.DataFrame(player.players())
  player2017.drop(labels=['DISPLAY_LAST_COMMA_FIRST','TEAM_ABBREVIATION','TEAM_CITY','TEAM_NAME','TO_YEAR'], axis=1, inplace=True)
  player2017.replace({'GAMES_PLAYED_FLAG':{'Y':True,'N':False}}, inplace=True)
  player2017.rename(columns={'PERSON_ID':'PLAYER_ID'}, inplace=True)
  c.to_pg(player2017, 'player')
  print(c.from_pg('player'))

