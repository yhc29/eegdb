import sys
sys.path.insert(0, '..')

import os
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing.pool import ThreadPool

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

def import_test(n_test_subject,max_segment_length):
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  # eegdb.drop_collections(["files","segments"])

  data_folder = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/"
  filepath_list = []
  for filename in os.listdir(data_folder):
    if os.path.isfile(os.path.join(data_folder, filename)):
      if filename.split(".")[-1] == "edf":
        filepath_list.append( data_folder + filename)
  print(filepath_list)

  file_type = "edf"
  data_file_list = []
  # filepath_list = ["/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/BJED0302978830244401-20160805-102831-2922.edf"]
  for filepath in filepath_list:
    print(filepath)
    data_file = DataFile("",filepath,file_type,"")
    data_file_list.append(data_file)

  random_offset_day_list = list(range(-1*int(365*10/n_test_subject), int(365*10/n_test_subject)))
  # mp_input = []
  for test_subject_index in range(n_test_subject):
    random_offset_in_days = random.choice(random_offset_day_list)
    print("random_offset_in_days",random_offset_in_days)
    subjectid = "test_subject_"+str(test_subject_index)
    sessionid = subjectid+"_01"
    print("**************************",subjectid,"**************************")

    for data_file in data_file_list:
      data_file_doc = data_file.get_doc()
      fileid = data_file_doc["fileid"]
      print("import",subjectid,sessionid,fileid)
      new_start_datetime = data_file_doc["start_datetime"] + relativedelta(days = random_offset_in_days)
      # new_start_datetime = datetime(2016,1,1) + relativedelta(days = random_offset_in_days)
      data_file.set_start_datetime(new_start_datetime)
      data_file.set_subjectid(subjectid)
      data_file.set_sessionid(sessionid)
      eegdb.import_data_file(data_file,max_segment_length=max_segment_length)


    # mp_input.append([subjectid,sessionid,data_file_list,max_segment_length])

  # n_processes = 10
  # pool = ThreadPool(processes=n_processes)
  # print("import_subject, n_processes =",n_processes)
  # pool.starmap(import_subject,mp_input)

def import_subject(subjectid,sessionid,data_file_list,max_segment_length):
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_10_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)

  random_offset_day_list = list(range(-365*5, 365*5))
  random_offset_in_days = random.choice(random_offset_day_list)
  for data_file in data_file_list:
    print("import",subjectid,sessionid)
    new_start_datetime = data_file.get_doc()["start_datetime"] + relativedelta(days = random_offset_in_days)
    data_file.set_start_datetime(new_start_datetime)
    eegdb.import_data_file(data_file,max_segment_length=max_segment_length)


def export_test():
  n_test_subject = 10
  max_segment_length = 1
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  # eegdb.build_index()
  
  subjectid = "test_subject_8"
  # ['Fp1', 'Fp2', 'F3', 'F4', 'C3', 'C4', 'P3', 'P4', 'O1', 'O2', 'F7', 'F8', 'T7', 'T8', 'P7', 'P8', 'Fz', 'Cz', 'Pz', 'E', 'FT9', 'FT10', 'A1', 'A2', 'EKG', 'EKG2', 'X3', 'X4', '-', '-', '-', 'DC01', 'DC02', 'DC03', 'DC04', '', '', 'BP1', 'BP2', 'BP3', 'BP4', 'N/A']
  channel_list = ['EKG', 'EKG2']
  # query_start_datetime,query_end_datetime=None,None
  query_start_datetime,query_end_datetime= datetime(2015,4,9,21,0,0),datetime(2015,4,9,22,0,0)
  dataset = eegdb.data_export(subjectid,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
  for channel_key, channel_signals_doc_list in dataset.items():
    print(channel_key)
    for section_signals_doc in channel_signals_doc_list:
      print(section_signals_doc["start_datetime"],section_signals_doc["end_datetime"],len(section_signals_doc["signals"]))

def load_annotation_test():
  subjectid = "CCOU71670000211555"
  sessionid = "CCOU7167000021155502"
  edf_filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/UH/CCOU7167000021155502/CCOU7167000021155502_7_1.edf"
  annotation_filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/UH/CCOU7167000021155502/CCOU7167000021155502_7_1.txt"
  file_type = "edf"
  datafile = DataFile(subjectid,edf_filepath,file_type,sessionid)

  annotation_docs = datafile.load_annotations(annotation_filepath)
  for doc in annotation_docs:
    print(doc)

if __name__ == '__main__':
  my_timer = Timer()

  # read_test()
  n_test_subject = 10
  max_segment_length_list = [1,10,20,30,60,600]
  max_segment_length_list = [5,180,300,900]
  for max_segment_length in max_segment_length_list:
    import_test(n_test_subject,max_segment_length)
  # export_test()

  print(my_timer.stop())