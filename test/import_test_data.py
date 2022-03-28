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

def export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=None,query_start_datetime=None,query_end_datetime=None):
  this_result_log = []
  test_time = 0
  n_export_data_points = 0
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    if subject_record_time_dict:
      query_start_datetime = subject_record_time_dict[subjectid][0] + relativedelta(seconds=20*3600-666)
      query_end_datetime = query_start_datetime + relativedelta(seconds=window_length)
    else:
      query_start_datetime = query_start_datetime
      query_end_datetime = query_end_datetime

    tmp_timer = Timer()
    dataset = eegdb.data_export(subjectid_list,query_channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    tmp_timer.stop()
    test_time += tmp_timer.total_time

    for channel_key, channel_signals_doc_list in dataset.items():
      for channel_signals_doc in channel_signals_doc_list:
        n_export_data_points += len(channel_signals_doc["signals"])

  test_time_avg = round(test_time/n_test_subject,2)
  print("test_time_avg",test_time_avg)
  this_result_log.append(test_time_avg)

  avg_export_data_points = round(n_export_data_points/n_test_subject,2)
  print("avg_export_data_points",avg_export_data_points)
  this_result_log.append(avg_export_data_points)

  return this_result_log

def test_db_indexing(n_test_subject,max_segment_length):
  print("building index",n_test_subject,max_segment_length)
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)
  eegdb.build_index()

def data_export_test(n_test_subject,max_segment_length):
  print("*****************************","data_export_test",n_test_subject,max_segment_length)
  eegdb = Eegdb(config_file.mongo_url,"eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s",config_file.output_folder,config_file.data_folder)

  result_list_1channel = []
  result_list_10channels = []

  # warm up query, get record time for each subject
  subject_record_time_dict = {}
  channel_list = ['EKG']
  for subject_index in range(n_test_subject):
    subjectid = "test_subject_"+str(subject_index)
    subjectid_list = [subjectid]
    query_start_datetime,query_end_datetime=None,None
    dataset = eegdb.data_export(subjectid_list,channel_list,query_start_datetime=query_start_datetime,query_end_datetime=query_end_datetime)
    for channel_key, channel_signals_doc_list in dataset.items():
      subject_record_time_dict[subjectid] = [channel_signals_doc_list[0]["start_datetime"],channel_signals_doc_list[0]["end_datetime"]]
      break

  # test1, single subject, 1/10 channel, 1s window
  test_name = "test1"
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  window_length = 1
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test2, single subject, 1 channel, 10s window
  test_name = "test2"
  window_length = 10
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test3, single subject, 1 channel, 1min window
  test_name = "test3"
  window_length = 60
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test4, single subject, 1 channel, 10min window
  test_name = "test4"
  window_length = 600
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test5, single subject, 1 channel, 1hour window
  test_name = "test5"
  window_length = 3600
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test6, single subject, 1 channel, 5hour window
  test_name = "test6"
  window_length = 3600*5
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test7, single subject, 1 channel, 10hour window
  test_name = "test7"
  window_length = 3600*10
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test8, single subject, 1 channel, 24hour window
  test_name = "test8"
  window_length = 3600*24
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=subject_record_time_dict)
  result_list_10channels.append(this_result_log)

  # test9, single subject, 1 channel, infinite window
  test_name = "test9"
  window_length = None
  this_result_log = [test_name]
  query_channel_list = ['EKG']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=None)
  result_list_1channel.append(this_result_log)

  test_name = test_name+"_10channels"
  this_result_log = [test_name]
  query_channel_list = ['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2', 'T7', 'T8', 'Fz', 'Cz']
  print(test_name,": 1 subject," + str(len(query_channel_list)) + "channel(s)," + str(window_length) + "second(s) window")
  this_result_log += export_test1(eegdb,query_channel_list,window_length,subject_record_time_dict=None)
  result_list_10channels.append(this_result_log)



  csv_file_path = config_file.output_folder + "eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s" + "_export_time.csv"
  output_file = csv.writer(open(csv_file_path, "w"))
  header1 = ["db_name"]
  header2 = ["db_name"]
  for result_info in result_list_1channel:
    header1.append(result_info[0])
  # for result_info in result_list_10channels:
  #   header2.append(result_info[0])
  time_row1 = ["eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s time"]
  data_point_row1 = ["eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s data_point"]
  time_row2 = ["eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s"+" 10 channels time"]
  data_point_row2 = ["eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s"+" 10 channels data_point"]
  for result_info in result_list_1channel:
    time_row1.append(result_info[1])
    data_point_row1.append(result_info[2])
  for result_info in result_list_10channels:
    time_row2.append(result_info[1])
    data_point_row2.append(result_info[2])
  output_file.writerow(header1)
  output_file.writerow(time_row1)
  output_file.writerow(data_point_row1)
  # output_file.writerow(header2)
  output_file.writerow(time_row2)
  output_file.writerow(data_point_row2)

def merge_result_files(result_folder,max_segment_length_list,n_test_subject=10):
  result_dict = {"time":[], "data_point":[], "time_10chn":[], "data_point_10chn":[]}
  for max_segment_length in max_segment_length_list:
    db_name = "eegdb_test_"+str(n_test_subject)+"_subjects_"+str(max_segment_length)+"s"
    csv_file_path = result_folder + db_name + "_export_time.csv"
    with open(csv_file_path,encoding='utf-8-sig') as f:
      csv_reader = csv.DictReader(f)
      i_row = 0
      for row in csv_reader:
        i_row += 1
        header = list(row.keys())
        if i_row == 1:
          result_dict["time"].append(list(row.values()))
        elif i_row == 2:
          result_dict["data_point"].append(list(row.values()))
        elif i_row == 3:
          result_dict["time_10chn"].append(list(row.values()))
        elif i_row == 4:
          result_dict["data_point_10chn"].append(list(row.values()))
  csv_file_path = config_file.output_folder + "eegdb_test_export_time.csv"
  output_file = csv.writer(open(csv_file_path, "w"))
  
  header[0] = "db_name/time"
  output_file.writerow(header)
  output_file.writerows(result_dict["time"])

  header[0] = "db_name/data_point"
  output_file.writerow(header)
  output_file.writerows(result_dict["data_point"])

  header[0] = "db_name/time_10chn"
  output_file.writerow(header)
  output_file.writerows(result_dict["time_10chn"])

  header[0] = "db_name/data_point_10chn"
  output_file.writerow(header)
  output_file.writerows(result_dict["data_point_10chn"])



if __name__ == '__main__':
  my_timer = Timer()

  n_test_subject = 10
  max_segment_length_list = [1,2,5,10,20,30,60,180,300,600,900]
  # max_segment_length_list = [5,180,300,900]

  # for max_segment_length in max_segment_length_list:
  #   test_data_import(n_test_subject,max_segment_length)

  for max_segment_length in max_segment_length_list:
    # test_db_indexing(n_test_subject,max_segment_length)
    data_export_test(n_test_subject,max_segment_length)
  
  merge_result_files()


  print(my_timer.stop())