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

def test_data_import(n_test_subject,max_segment_length):
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

def test_db_indexing(n_test_subject,max_segment_length):
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  eegdb.build_index()

def data_export_test(n_test_subject,max_segment_length):
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)

  dataset = eegdb.data_export(["test_subject_0"],["EKG"],query_start_datetime=None,query_end_datetime=None)

  subject_record_time_dict = {}
  # test1, single subject, 1 channel, all data
  print("test1")
  channel_list = ['EKG']
  test1_time = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime,query_end_datetime=None,None

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test1_time += tmp_timer.total_time

    for channel_key, channel_signals_doc_list in dataset.items():
      subject_record_time_dict[subjectid] = [channel_signals_doc_list[0]["start_datetime"],channel_signals_doc_list[0]["end_datetime"]]
      break
  test1_time_avg = round(test1_time/n_test_subject,2)
  print("test1_time_avg",test1_time_avg)

  # test2, single subject, 1 channel, 10s window
  print("test2")
  channel_list = ['EKG']
  window_length = 10
  test2_time = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime = subject_record_time_dict[subjectid][0] + relativedelta(seconds=20*3600-666)
    query_end_datetime = query_start_datetime + window_length

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test2_time += tmp_timer.total_time
  test2_time_avg = round(test2_time/n_test_subject,2)
  print("test2_time_avg",test2_time_avg)

  # test3, single subject, 1 channel, 1min window
  print("test3")
  channel_list = ['EKG']
  window_length = 60
  test3_time = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime = subject_record_time_dict[subjectid][0] + relativedelta(seconds=20*3600-666)
    query_end_datetime = query_start_datetime + window_length

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test3_time += tmp_timer.total_time
  test3_time_avg = round(test3_time/n_test_subject,2)
  print("test3_time_avg",test3_time_avg)

  # test4, single subject, 1 channel, 10min window
  print("test4")
  channel_list = ['EKG']
  window_length = 600
  test4_time = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime = subject_record_time_dict[subjectid][0] + relativedelta(seconds=20*3600-666)
    query_end_datetime = query_start_datetime + window_length

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test4_time += tmp_timer.total_time
  test4_time_avg = round(test4_time/n_test_subject,2)
  print("test4_time_avg",test4_time_avg)

  # test5, single subject, 1 channel, 1hour window
  print("test5")
  channel_list = ['EKG']
  window_length = 3600
  test5_time = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime = subject_record_time_dict[subjectid][0] + relativedelta(seconds=20*3600-666)
    query_end_datetime = query_start_datetime + window_length

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test5_time += tmp_timer.total_time
  test5_time_avg = round(test5_time/n_test_subject,2)
  print("test5_time_avg",test5_time_avg)









if __name__ == '__main__':
  my_timer = Timer()

  n_test_subject = 10
  # max_segment_length_list = [1,5,10,20,30,60,180,300,600,900]
  max_segment_length_list = [5,180,300,900]
  for max_segment_length in max_segment_length_list:
    test_data_import(n_test_subject,max_segment_length)


  print(my_timer.stop())