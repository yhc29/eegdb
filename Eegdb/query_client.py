import sys
sys.path.insert(0, '..')

import numpy as np
import os
import math
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymongo
import csv

from Eegdb.data_file import DataFile

class QueryClient:
  def __init__(self,mongo_url,db_name,output_folder):
    self.__mongo_client = pymongo.MongoClient(mongo_url)
    self.__db_name = db_name
    self.__database = self.__mongo_client[db_name]
    self.__output_folder = output_folder
    if not os.path.exists(output_folder):
      os.makedirs(output_folder)
  
  def get_db_name(self):
    return self.__db_name

  def get_segments_collection(self):
    return self.__database["segments"]

  
  '''
    Quey functions
  '''
  def segment_query(self,datetime1,datetime2=None,subjectid_list=None,channel_list=None):
    datetime2 = datetime1 if not datetime2 else datetime2

    segment_datetime1 = get_fixed_segment_start_datetime(datetime1,30)
    segment_datetime2 = get_fixed_segment_start_datetime(datetime2,30)
    if subjectid_list:
      _stmt = {"subjectid":{"$in":subjectid_list}}
    else:
      _stmt = {"subjectid":{"$exists":True}}
    if channel_list:
      _stmt["channel_label"] = {"$in":channel_list}
    _stmt["segment_datetime"] = {"$gte":segment_datetime1,"$lte":segment_datetime2}
    segment_docs = self.get_segments_collection().find(_stmt)

    result = {}
    for doc in segment_docs:
      _subjectid = doc["subjectid"]
      _channel_label = doc["channel_label"]
      try:
        result[_subjectid]
      except:
        result[_subjectid] = {}
      
      try:
        result[_subjectid][_channel_label].append(doc)
      except:
        result[_subjectid][_channel_label] = [doc]
    
    for subjectid,channels_data in result.items():
      for channel_label,signal_doc_array in channels_data.items():
        new_time_points = []
        new_signals = []
        for signal_doc in sorted(signal_doc_array,key=lambda x:x["segment_datetime"]):
          signals = signal_doc["signals"]
          sample_rate = int(signal_doc["sample_rate"]+0.5)
          granularity = 1/sample_rate
          signal_start_datetime = signal_doc["start_datetime"]
          signal_end_datetime = signal_doc["end_datetime"]
          for m in range(signal_start_datetime.minute,signal_end_datetime.minute+1):
            for s in range(60):
              try:
                new_signals += signals[str(m)][str(s)]
                new_time_points += [signal_start_datetime+relativedelta(minutes=m,seconds=s+x*granularity) for x in range(len(signals[str(m)][str(s)])) ]
              except:
                pass

        result[subjectid][channel_label] = (new_time_points,new_signals)




    return result



def get_fixed_segment_start_datetime(segment_start_datetime,segment_duration):
  _start_hour, _start_minute = segment_start_datetime.hour, segment_start_datetime.minute
  _total_minutes = (_start_hour*60 + _start_minute)//segment_duration*segment_duration
  _new_hour = _total_minutes//60
  _new_minute = _total_minutes%60
  _new_datetime = segment_start_datetime.replace(hour=_new_hour,minute=_new_minute,second=0)
  return _new_datetime
