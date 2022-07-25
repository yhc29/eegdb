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

from Utils.timer import Timer

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

  def get_files_collection(self):
    return self.__database["files"]
  def get_segments_collection(self):
    return self.__database["segments"]
  def get_annotations_collection(self):
    return self.__database["annotation_records"]
  def get_annotation_tii_collection(self):
    return self.__database["annotation_tii"]
  def get_annotation_relation_pt_timeline_collection(self):
    return self.__database["pannotation_relation_pt_timeline"]

  
  '''
    Quey functions
  '''
  def segment_query(self,segment_duration,datetime1,datetime2=None,subjectid_list=None,channel_list=None):
    _my_timer = Timer()
    datetime2 = datetime1 if not datetime2 else datetime2

    segment_datetime1 = get_fixed_segment_start_datetime(datetime1,segment_duration)
    segment_datetime2 = get_fixed_segment_start_datetime(datetime2,segment_duration)
    segment_datetime_list = [segment_datetime1]
    for i in range(1,int( (segment_datetime2-segment_datetime1).total_seconds()//(segment_duration*60)+1 )):
      segment_datetime_list.append(segment_datetime1+relativedelta(minutes=segment_duration*i))
    
    if subjectid_list:
      if len(subjectid_list)==1:
        _match_stmt = {"subjectid":subjectid_list[0]}
      else:
        _match_stmt = {"subjectid":{"$in":subjectid_list}}
    else:
      _match_stmt = {"subjectid":{"$exists":True}}
    if channel_list:
      if len(channel_list)==1:
        _match_stmt["channel_label"] = channel_list[0]
      else:
        _match_stmt["channel_label"] = {"$in":channel_list}
    _match_stmt["segment_datetime"] = {"$in":segment_datetime_list}
    # _match_stmt["segment_datetime"] = {"$gte":segment_datetime1,"$lte":segment_datetime2}

    _project_stmt = { "_id": 0, "subjectid": 1, "channel_label": 1, "start_datetime":1, "end_datetime":1, "sample_rate":1}
    _segment_start_minute, _segment_end_minute = datetime1.minute,datetime2.minute
    for i in range(60):
      _cond_list = []
      if i<_segment_start_minute:
        _cond_list.append(segment_datetime_list[0])
      if i>_segment_end_minute:
        _cond_list.append(segment_datetime_list[-1])
      _project_stmt["signals."+str(i)] = {"$cond": [{"$in": ['$segment_datetime', _cond_list.copy() ]}, 0, "$signals."+str(i) ]}
    _ap_stmt = [
      { "$match" : _match_stmt },
      { "$project": _project_stmt},
    ]
    # segment_docs = self.get_segments_collection().find(_match_stmt)
    segment_docs = self.get_segments_collection().aggregate(_ap_stmt,allowDiskUse=False)

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
    print("mongo query time",_my_timer.click())
    for subjectid,channels_data in result.items():
      for channel_label,signal_doc_array in channels_data.items():
        new_time_points_list = [[datetime1,datetime1,None]]
        new_signals_list = [[]]
        sample_rate_set = set([])
        for signal_doc in sorted(signal_doc_array,key=lambda x:x["start_datetime"],reverse=False):
          signals = signal_doc["signals"]
          sample_rate = int(signal_doc["sample_rate"]+0.5)
          sample_rate_set.add(sample_rate)
          if len(sample_rate_set)>1:
            print("inconsistant sample rate found:",sample_rate_set)
          # granularity = 1/sample_rate
          signal_start_datetime = signal_doc["start_datetime"]
          signal_end_datetime = signal_doc["end_datetime"]
          if signal_end_datetime>datetime2:
            signal_end_datetime = datetime2
          if signal_start_datetime<new_time_points_list[-1][1]:
            if signal_end_datetime<=new_time_points_list[-1][1]:
              # contained in the previous recording, skip
              continue
            else:
              # signal overlap
              signal_start_datetime = new_time_points_list[-1][1]
              new_time_points_list[-1][1] = signal_end_datetime
              new_time_points_list[-1][2] = sample_rate
          elif signal_start_datetime == new_time_points_list[-1][1]:
            # concatenate
            new_time_points_list[-1][1] = signal_end_datetime
            new_time_points_list[-1][2] = sample_rate
          else:
            # not continous, append a new segment
            if new_signals_list[-1] == []:
              new_time_points_list[-1] = [signal_start_datetime,signal_end_datetime,sample_rate]
            else:
              new_time_points_list.append([signal_start_datetime,signal_end_datetime,sample_rate])
              new_signals_list.append([])
          m1 = signal_start_datetime.minute
          s1 = signal_start_datetime.second
          m2 = 60 if signal_end_datetime.minute==0 else signal_end_datetime.minute
          s2 = signal_end_datetime.second
          for m in range(m1,m2+1):
            if m == m1:
              tmp_seconds = range(s1,60)
            elif m == m2:
              tmp_seconds = range(0,s2)
            else:
              tmp_seconds = range(60)
            for s in tmp_seconds:
              try:
                new_signals_list[-1] += signals[str(m)][str(s)]["values"]
              except:
                pass
        result[subjectid][channel_label] = (new_time_points_list,new_signals_list)
    print("post process time",_my_timer.click())
    return result

  def annotation_query(self,annotation_list):
    result = {}
    _match_stmt = {"annotation":{"$in":annotation_list}}
    annotation_docs = self.get_annotations_collection().find(_match_stmt)
    for doc in annotation_docs:
      subjectid = doc["subjectid"]
      time = doc["time"]
      try:
        result[subjectid].append(time)
      except:
        result[subjectid] = [time]
    
    for subjectid,time_list in result.items():
      result[subjectid] = sorted(time_list)
    return result

  def get_all_subjectid_with_annotation(self):
    try:
      subjectid_list = self.get_annotations_collection().distinct("subjectid")
    except Exception as e:
      print(str(e))
      subjectid_list = []
    return subjectid_list


  def get_subjectid_by_annotation(self, annotation_list = [], annotationid_list = [] ):
    # if input annotation is empty, then return all patients with annotation
    if annotation_list == [] and annotationid_list == []:
      return self.get_all_subjectid_with_annotation()
    
    _or_list = []
    if annotation_list:
      _or_list.append({"annotation": {"$in":annotation_list}})
    if annotationid_list:
      _or_list.append({"annotationid_partial": {"$in":annotationid_list}})
    _match_stmt = {"$or":_or_list}
    annotation_docs = self.get_annotation_tii_collection().find(_match_stmt)
    result = set([])
    for doc in annotation_docs:
      subjectid_list = doc["subjectid_list"]
      result = result.union(set(subjectid_list))
    return list(result)

  def get_subjectid_by_annotation_relation_v0(self, time_diff = [0,3600], annotationid1_list = [], annotationid2_list = [] ):
    # if input annotation is empty, then return all patients with annotation
    if annotationid1_list == [] or annotationid2_list == []:
      print("input error: annotation_list or annotationid_list is empty")
      return []

    annotationid_pt_time_dict = {}

    _ap_stmt = [
      { "$match" : { "annotationid_partial": {"$in":annotationid1_list}}},
      { "$project": {"_id": 0, "subjectid": 1, "time": 1} },
      { "$group": { 
        "_id": "$subjectid",
        "time_list":{"$addToSet":"$time"},
      } }
    ]
    docs = self.get_annotations_collection().aggregate(_ap_stmt,allowDiskUse=False)
    for doc in docs:
      annotationid_pt_time_dict[doc["_id"]] = [(1,x) for x in doc["time_list"]]
    _ap_stmt = [
      { "$match" : { "annotationid_partial": {"$in":annotationid2_list}}},
      { "$project": {"_id": 0, "subjectid": 1, "time": 1} },
      { "$group": { 
        "_id": "$subjectid",
        "time_list":{"$addToSet":"$time"},
      } }
    ]
    docs = self.get_annotations_collection().aggregate(_ap_stmt,allowDiskUse=False)
    for doc in docs:
      try:
        annotationid_pt_time_dict[doc["_id"]] += [(2,x) for x in doc["time_list"]]
      except:
        # annotationid_pt_time_dict.pop(doc["_id"])
        pass
    subjectid_list = []
    for subjectid,time_list in annotationid_pt_time_dict.items():
      cadidates = []
      pointer = 0
      matched_pattern = []
      for annotationid,time in sorted(time_list,key=lambda x:x[1]):
        if annotationid == 1:
          cadidates.append(time)
        elif annotationid == 2:
          if pointer<len(cadidates):
            for i in range(pointer,len(cadidates)):
              if (time-cadidates[i]).total_seconds()<time_diff[0] or (time-cadidates[i]).total_seconds()<=0:
                break
              elif (time-cadidates[i]).total_seconds()>time_diff[1]:
                pointer = i+1
              else:
                matched_pattern.append((cadidates[i],time))
                break
          if len(matched_pattern)>0:
            break
      if len(matched_pattern)>0:
        subjectid_list.append(subjectid)


    return subjectid_list

  def get_subjectid_by_annotation_relation(self, time_diff = [0,3600], annotationid1_list = [], annotationid2_list = [] ):
    # if input annotation is empty, then return all patients with annotation
    if annotationid1_list == [] or annotationid2_list == []:
      print("input error: annotation_list or annotationid_list is empty")
      return []

    _ap_stmt = [
      { "$match" : { "annotationid1": {"$in":annotationid1_list},"annotationid2": {"$in":annotationid2_list}, "time_diff": { "$elemMatch": { "$ne":0,"$gte": time_diff[0], "$lte": time_diff[1] } } } },
      { "$project": {"_id": 0, "subjectid": 1} },
      { "$group": { "_id": "$subjectid"} }
    ]
    docs = self.get_annotation_relation_pt_timeline_collection().aggregate(_ap_stmt,allowDiskUse=False)
    subjectid_list = [ doc["_id"] for doc in docs ]

    return subjectid_list

  def get_time_by_annotation(self, annotation_list = [], annotationid_list = [] ):
    # if input annotation is empty, then return all patients with annotation
    if annotation_list == [] and annotationid_list == []:
      print("input error: annotation_list and annotationid_list are both empty")
      return []
    
    _or_list = []
    if annotation_list:
      _or_list.append({"annotation": {"$in":annotation_list}})
    if annotationid_list:
      _or_list.append({"annotationid_partial": {"$in":annotationid_list}})
    _match_stmt = {"$or":_or_list}
    annotation_docs = self.get_annotations_collection().find(_match_stmt)
    result = {}
    for doc in annotation_docs:
      subjectid = doc["subjectid"]
      time = doc["time"]
      try:
        result[subjectid].append(time)
      except:
        result[subjectid] = [time]
    return result



def get_fixed_segment_start_datetime(segment_start_datetime,segment_duration):
  _start_hour, _start_minute = segment_start_datetime.hour, segment_start_datetime.minute
  _total_minutes = (_start_hour*60 + _start_minute)//segment_duration*segment_duration
  _new_hour = _total_minutes//60
  _new_minute = _total_minutes%60
  _new_datetime = segment_start_datetime.replace(hour=_new_hour,minute=_new_minute,second=0,microsecond=0)
  return _new_datetime
