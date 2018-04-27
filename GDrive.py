# -*- coding: utf-8 -*-
"""
Created on Sat Mar 31 12:10:36 2018

@author: Irob4
"""

import goldsberry
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import io
from timeit import default_timer as timer
from goldsberry.masterclass import NbaDataProvider

class GDrive():
  def __init__(self,folder='MBLM Shared Files'):
    self.folder = folder
    self.gauth = GoogleAuth()
    self.gauth.LocalWebserverAuth()
    self.drive = GoogleDrive(self.gauth)
    self.file_list = self.drive.ListFile({'q':"'root' in parents and trashed=false"}).GetList()
    self.folder_list = {self.file_list[i]['title']:self.file_list[i]['id'] for i in range(len(self.file_list)) if \
        [f['mimeType']=='application/vnd.google-apps.folder' for f in self.file_list][i]}
    self.current_folder = self.folder_list[self.folder]
    
  def __enter__(self):
    return self
    
  def __exit__(self, exception_type, exception_value, traceback):
    return
  
  def _create_file(self,name):
    file = self.drive.CreateFile({'title':name + '.csv','mimeType':'text/csv','parents':[
        {'kind':'drive#fileLink','id':self.current_folder}]})
    return file
  
  def change_dir(self,folder):
    try:
      self.current_folder = folder_list[folder]
    except KeyError as e:
      print('KeyError: Folder %s does not exist',folder)
    return

  def run_func(self,section,func,*args):
    file = self._create_file(section.__class__.__name__ + '_' + func) 
    df = pd.DataFrame(getattr(section,func)())
    output = io.StringIO()
    df.to_csv(output,index=False,sep='\t')
    file.SetContentString(output.getvalue())
    file.Upload()
    output.close()
    
with GDrive() as d:
  for func in goldsberry.playtype.playtype.__all__:
    start = timer()
    pt = getattr(goldsberry.playtype,func)()
    d.run_func(pt,'defensive')
    end = timer()
    print(func,' loaded in ',str(end-start))
    #d.run_func(getattr(goldsberry.playtype.playtype,func)(),'season')
    