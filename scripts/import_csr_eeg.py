import sys
sys.path.insert(0, '..')

import os
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing.pool import ThreadPool
import csv

from Utils.timer import Timer

from Eegdb.data_file import DataFile
from Eegdb.eegdb import Eegdb

import config.db_config_ibm as config_file

def test_data_import(eegdb,data_folder):
  data_file_dict = {}
  for sessionid in os.listdir(data_folder):
    session_folder = os.path.join(data_folder, sessionid)
    if os.path.isdir(session_folder):
      subjectid = sessionid[:-2]
      if subjectid[0]!="I":
        continue
      for filename in os.listdir(session_folder):
        file_ext = filename.split(".")[-1]
        fileid = filename.split("."+file_ext)[0]
        if file_ext == "edf":
          try:
            data_file_dict[fileid][2] = session_folder + "/" + filename
          except:
            data_file_dict[fileid] = [subjectid, sessionid, session_folder + "/" + filename, None]
        if file_ext == "txt":
          try:
            data_file_dict[fileid][3] = session_folder + "/" + filename
          except:
            data_file_dict[fileid] = [subjectid, sessionid, None, session_folder + "/" + filename]
  # for fileid,file_info in data_file_dict.items():
  #   if not file_info[2] or not file_info[3]:
  #     print(file_info)
  total_file_count = len(data_file_dict.keys())
  print(total_file_count, "files found!")

  imported_file_count = 0
  _sample_rate_set = set([])
  exclude_subjectid_set = set([])
  for fileid,file_info in data_file_dict.items():
    subjectid,sessionid,filepath,annotation_filepath = file_info
    if subjectid in exclude_subjectid_set:
      continue
    if not filepath:
      print("No edf found for",file_info)
    if not annotation_filepath:
      print("No edf found for",file_info)
    # eegdb.import_csr_eeg_file(subjectid,sessionid,filepath,max_segment_length=None,annotation_filepath=annotation_filepath,max_sample_rate=500)
    _code,_tmp_sr_set = eegdb.import_csr_eeg_file_v2(subjectid,sessionid,filepath,segment_duration=None,annotation_filepath=annotation_filepath,max_sample_rate=299)
    if _code == -3:
      exclude_subjectid_set.add(subjectid)
    if _code == 1:
      imported_file_count += 1
    if _tmp_sr_set:
      _sample_rate_set.update(_tmp_sr_set)
      print(_sample_rate_set)
    print(imported_file_count,"/",total_file_count, "imported.")

if __name__ == '__main__':
  my_timer = Timer()

  eegdb = Eegdb(config_file.mongo_url,config_file.eegdb_name,config_file.output_folder,config_file.data_folder)
  test_data_import(eegdb,config_file.data_folder)




  print(my_timer.stop())