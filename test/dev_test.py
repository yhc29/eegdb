import sys
sys.path.insert(0, '..')

import os
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from Utils.timer import Timer

from Eegdb.data_file import DataFile
from Eegdb.eegdb import Eegdb

import config.db_config_ibm as config_file

def read_test():
  subjectid = "BJED03029788302444"
  sessionid = "BJED0302978830244401"
  filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/BJED0302978830244401-20160803-162846-21600.edf"
  file_type = "edf"
  datafile = DataFile(subjectid,filepath,file_type,sessionid)
  segments = datafile.segmentation(1)
  print(len(segments))
  segment_doc = segments[100000].get_doc()
  for key,value in segment_doc.items():
    print(key,value)

def import_test():
  n_test_subject = 2
  max_segment_length = 1
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  eegdb.drop_collections(["files","segments"])

  data_folder = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/"
  filepath_list = []
  for filename in os.listdir(data_folder):
    if os.path.isfile(os.path.join(data_folder, filename)):
      if filename.split(".")[-1] == "edf":
        filepath_list.append( data_folder + filename)
  print(filepath_list)

  file_type = "edf"
  random_offset_day_list = list(range(-365*5, 365*5))
  for test_subject_index in range(n_test_subject):
    random_offset_in_days = random.choice(random_offset_day_list)
    print("random_offset_in_days",random_offset_in_days)
    subjectid = "test_subject_"+str(test_subject_index)
    sessionid = subjectid+"_01"
    for filepath in filepath_list:
      # filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/BJED0302978830244401-20160803-162846-21600.edf"
      print("import",subjectid,sessionid,filepath)
      data_file = DataFile(subjectid,filepath,file_type,sessionid)
      new_start_datetime = data_file.get_doc()["start_datetime"] + relativedelta(days = random_offset_in_days)
      data_file.set_start_datetime(new_start_datetime)
      eegdb.import_data_file(data_file,max_segment_length=max_segment_length)
      # eegdb.import_csr_eeg_file(subjectid,sessionid,filepath)


def export_test():
  n_test_subject = 2
  max_segment_length = 1
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  
  subjectid = "test_subject_1"
  # ['Fp1', 'Fp2', 'F3', 'F4', 'C3', 'C4', 'P3', 'P4', 'O1', 'O2', 'F7', 'F8', 'T7', 'T8', 'P7', 'P8', 'Fz', 'Cz', 'Pz', 'E', 'FT9', 'FT10', 'A1', 'A2', 'EKG', 'EKG2', 'X3', 'X4', '-', '-', '-', 'DC01', 'DC02', 'DC03', 'DC04', '', '', 'BP1', 'BP2', 'BP3', 'BP4', 'N/A']
  channel_list = ['EKG', 'EKG2']
  dataset = eegdb.data_export(subjectid,channel_list,query_start_datetime=None,query_end_datetime=None)
  for channel_key, channel_signals_doc_list in dataset.items():
    print(channel_key)
    for section_signals_doc in channel_signals_doc_list:
      print(section_signals_doc["start_datetime"],section_signals_doc["end_datetime"])


if __name__ == '__main__':
  my_timer = Timer()

  # read_test()
  import_test()

  print(my_timer.stop())