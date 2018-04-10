/ ............,,,,,,,"# -*- coding: utf-8 -*-
"""
Created on Sat Mar 31 12:10:36 2018

@author: Irob4
"""

import goldsberry
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import io

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

  def run_func(self,section,func,ID):
    file = self._create_file(section.__class__.__name__ + '_' + func + '_' + ID)
    df = pd.DataFrame(getattr(section,func)())
    output = io.StringIO()
    df.to_csv(output,index=False,sep='\t')
    file.SetContentString(output.getvalue())
    file.Upload()
    output.close()
    