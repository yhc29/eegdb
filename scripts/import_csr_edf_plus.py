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

def edf_plus_import(eegdb,data_folder):
  data_file_dict = {}
  for sessionid in os.listdir(data_folder):
    session_folder = os.path.join(data_folder, sessionid)
    if os.path.isdir(session_folder):
      subjectid = sessionid[:-2]
  
  subjectid = "WKQB28950888871511"
  sessionid = "WKQB2895088887151101"
  filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/csr/UH/EEG/WKQB2895088887151101/DA1167CT_1-1+.edf"
  _code,_tmp_sr_set = eegdb.import_csr_edf_plus(subjectid,sessionid,filepath,segment_duration=None,max_sample_rate=299)

if __name__ == '__main__':
  my_timer = Timer()

  eegdb = Eegdb(config_file.mongo_url,config_file.eegdb_name,config_file.output_folder,config_file.data_folder)
  edf_plus_import(eegdb,config_file.data_folder)




  print(my_timer.stop())