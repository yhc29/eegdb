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

def signal_data_import(eegdb,data_folder):
  import_queue = []
  vendor = "samsung_wearable"
  for file_type in ["bppg","hribi","gyro"]:
    type_data_folder = data_folder+ file_type + "/1/"
    for subjectid in os.listdir(type_data_folder):
      subject_folder = type_data_folder+subjectid+"/"
      if os.path.isdir(subject_folder):
        subject_filepath_list = [ ]
        for original_segmentid in os.listdir(subject_folder):
          segment_folder = os.path.join(subject_folder, original_segmentid)
          if os.path.isdir(segment_folder):
            for filename in os.listdir(segment_folder):
              
              if os.path.isfile(os.path.join(segment_folder, filename)):
                if filename.split(".")[-1] == file_type:
                  subject_filepath_list.append( segment_folder + "/" + filename)
        import_queue.append([vendor,file_type,subjectid,subject_filepath_list])

  my_timer = Timer()
  total_task_count = len(import_queue)
  print(total_task_count,"tasks found!")
  finished_task_count = 0
  for import_task in import_queue:
    vendor,file_type,subjectid,subject_filepath_list = import_task[0],import_task[1],import_task[2],import_task[3]
    print("import subjectid:",subjectid,"file_type:",file_type,"subject_filepath_list:",len(subject_filepath_list))
    eegdb.import_samsung_wearable_data(vendor,file_type,subjectid,subject_filepath_list)
    finished_task_count += 1
    print(finished_task_count,"/",total_task_count, "imported.")
    my_timer.get_progress(finished_task_count,total_task_count,p=1,show=True)

def annotation_import(eegdb,annotation_folder):
  eegdb.import_samsung_wearable_annotation(annotation_folder)


if __name__ == '__main__':
  my_timer = Timer()

  eegdb = Eegdb(config_file.mongo_url,config_file.samsung_wearable_eegdb_name,config_file.output_folder,config_file.samsung_wearable_data_folder)
  # signal_data_import(eegdb,config_file.samsung_wearable_data_folder)
  annotation_import(eegdb,config_file.samsung_wearable_annotation_folder)




  print(my_timer.stop())