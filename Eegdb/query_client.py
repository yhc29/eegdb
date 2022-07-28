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
    return self.__database["annotation_relation_pt_timeline"]
  def get_annotation_relation_pt_timeline_collection_v1(self):
    return self.__database["annotation_relation_pt_timeline_v1"]

  
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
  
  def get_time_by_annotation_relation_v1(self, time_diff = [0,3600], annotationid1_list = [], annotationid2_list = [], subjectid_list = None ):
    # if input annotation is empty, then return all patients with annotation
    if annotationid1_list == [] or annotationid2_list == []:
      print("input error: annotation_list or annotationid_list is empty")
      return []
    if subjectid_list:
      _match_stmt = { "subjectid":{"$in":subjectid_list}, "annotationid1": {"$in":annotationid1_list}, "annotationid2": {"$in":annotationid2_list}, "time_diff": { "$ne":0, "$gte": time_diff[0], "$lte": time_diff[1] } }
    else:
      _match_stmt = { "annotationid1": {"$in":annotationid1_list},"annotationid2": {"$in":annotationid2_list}, "time_diff": { "$ne":0, "$gte": time_diff[0], "$lte": time_diff[1] } }
    _ap_stmt = [
      { "$match" : _match_stmt },
      # { "$project": {"_id": 0, "subjectid": 1, "time_diff":1,"time":1 } },
      { "$group": {
          "_id": {"subjectid": "$subjectid", "annotationid1": "$annotationid1", "annotationid2": "$annotationid2"}, 
          "relations": {"$push": {"r":["$time_diff","$time"]}}
        } },
    ]
    docs = self.get_annotation_relation_pt_timeline_collection_v1().aggregate(_ap_stmt,allowDiskUse=False)
    results = {}
    for doc in docs:
      subjectid = doc["_id"]["subjectid"]
      annotationid1 = doc["_id"]["annotationid1"]
      annotationid2 = doc["_id"]["annotationid2"]
      try:
        results[subjectid].append( (annotationid1,annotationid2,[x["r"] for x in doc["relations"]]) )
      except:
        results[subjectid] = [ (annotationid1,annotationid2,[x["r"] for x in doc["relations"]]) ]
      # for relation_data in doc["relations"]:
      #   annotationid1 = relation_data["annotationid1"]
      #   annotationid2 = relation_data["annotationid2"]
      #   time_diff = relation_data["time_diff"]
      #   time = relation_data["time"]
      #   try:
      #     results[subjectid].append((annotationid1,annotationid2,time,time_diff))
      #   except:
      #     results[subjectid] = [(annotationid1,annotationid2,time,time_diff)]

    return results
    
  def get_time_by_annotation_relation(self, time_diff = [0,3600], annotationid1_list = [], annotationid2_list = [], subjectid_list = None ):
    # if input annotation is empty, then return all patients with annotation
    if annotationid1_list == [] or annotationid2_list == []:
      print("input error: annotation_list or annotationid_list is empty")
      return []
    if subjectid_list:
      _match_stmt = { "subjectid":{"$in":subjectid_list}, "annotationid1": {"$in":annotationid1_list}, "annotationid2": {"$in":annotationid2_list}, "time_diff": { "$elemMatch": { "$ne":0,"$gte": time_diff[0], "$lte": time_diff[1] } } }
    else:
      _match_stmt = { "annotationid1": {"$in":annotationid1_list},"annotationid2": {"$in":annotationid2_list}, "time_diff": { "$elemMatch": { "$ne":0,"$gte": time_diff[0], "$lte": time_diff[1] } } }
    _ap_stmt = [
      { "$match" : _match_stmt },
      { "$project": {
          "_id": 0, 
          "subjectid": 1,
          "annotationid1": 1,
          "annotationid2": 1,
          "time":{
            "$slice": [
              { "$zip": { "inputs": [ "$time1", "$time2" ] } },
              {"$indexOfArray":[
                "$time_diff",
                {"$first": {
                  "$filter": {
                    "input": "$time_diff",
                    "as": "item",
                    "cond": { "$and": [ {"$ne": ["$$item",0]}, {"$gte": ["$$item",time_diff[0]]}] }
                  }
                }}
              ]},
              { "$size": {
                "$filter": {
                  "input": "$time_diff",
                  "as": "item",
                  "cond": {"$and": [ {"$gte": ["$$item",time_diff[0]]},{"$lte": ["$$item",time_diff[1]]}]}
                }
              }}
            ]
          }
        } },
    ]
    docs = self.get_annotation_relation_pt_timeline_collection().aggregate(_ap_stmt,allowDiskUse=False)
    results = {}
    for doc in docs:
      subjectid = doc["subjectid"]
      annotationid1 = doc["annotationid1"]
      annotationid2 = doc["annotationid2"]
      time = doc["time"]
      try:
        results[subjectid].append((annotationid1,annotationid2,time))
      except:
        results[subjectid] = [(annotationid1,annotationid2,time)]

    return results

  def get_time_by_annotation_relation_v0(self, time_diff = [0,3600], annotationid1_list = [], annotationid2_list = [] ):
    # if input annotation is empty, then return all patients with annotation
    if annotationid1_list == [] or annotationid2_list == []:
      print("input error: annotation_list or annotationid_list is empty")
      return []

    annotationid_pt_time_dict = {}

    _ap_stmt = [
      { "$match" : { "annotationid_partial": {"$in":annotationid1_list}}},
      { "$project": {"_id": 0, "subjectid": 1, "annotationid_partial":1, "time": 1} },
      { "$group": { 
        "_id": {"subjectid": "$subjectid", "annotationid": "$annotationid_partial"},
        "time_list":{"$addToSet":"$time"},
      } }
    ]
    docs = self.get_annotations_collection().aggregate(_ap_stmt,allowDiskUse=False)
    for doc in docs:
      subjectid = doc["_id"]["subjectid"]
      annotationid = doc["_id"]["annotationid"]
      annotationid_pt_time_dict[subjectid] = [(1,x,annotationid) for x in doc["time_list"]]

    _ap_stmt = [
      { "$match" : { "annotationid_partial": {"$in":annotationid2_list}}},
      { "$project": {"_id": 0, "subjectid": 1, "annotationid_partial":1, "time": 1} },
      { "$group": { 
        "_id": {"subjectid": "$subjectid", "annotationid": "$annotationid_partial"},
        "time_list":{"$addToSet":"$time"},
      } }
    ]
    docs = self.get_annotations_collection().aggregate(_ap_stmt,allowDiskUse=False)
    for doc in docs:
      try:
        subjectid = doc["_id"]["subjectid"]
        annotationid = doc["_id"]["annotationid"]
        annotationid_pt_time_dict[subjectid] += [(2,x,annotationid) for x in doc["time_list"]]
      except:
        # annotationid_pt_time_dict.pop(doc["_id"])
        pass
    results = {}
    for subjectid,time_list in annotationid_pt_time_dict.items():
      cadidates = []
      pointer = 0
      matched_pattern = []
      for relation_flag,time,annotationid in sorted(time_list,key=lambda x:x[1]):
        if relation_flag == 1:
          cadidates.append((annotationid,time))
        elif relation_flag == 2:
          if pointer<len(cadidates):
            for i in range(pointer,len(cadidates)):
              if (time-cadidates[i][1]).total_seconds()<time_diff[0] or (time-cadidates[i][1]).total_seconds()<=0:
                break
              elif (time-cadidates[i][1]).total_seconds()>time_diff[1]:
                pointer = i+1
              else:
                matched_pattern.append((cadidates[i][0],annotationid,cadidates[i][1],time))
      if len(matched_pattern)>0:
        results[subjectid] = matched_pattern

    return results

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

  def temproal_query(self,input):
    # input: [ (point1), (point2), ...]
    # point: (annotation_list, exclude_annotation_list, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input:
      print("ERROR at temproal_query: input is empty")

    input_length = len(input)
    all_relations_by_points = [ [] for i in range(input_length)]
    all_relations_dict = {}
    for point_index, point_data in enumerate(input):
      if point_index == input_length-1 and not all_relations_by_points[point_index]:
        break
      annotation_list, exclude_annotation_list, relation_list = point_data
      # add a before relation to next point if not exists
      if not relation_list or sorted(relation_list,key=lambda x:x[0],reverse=True)[0][0]!=point_index+1:
        if point_index != input_length-1:
          relation_list.append( (point_index+1,None) )
      if relation_list:
        for relation_data in relation_list:
          print(relation_data)
          pt_relation_dict = {}
          point2_index = relation_data[0]
          relation_key = str(point_index)+"-"+str(point2_index)
          all_relations_by_points[point2_index].append(relation_key)
          annotationid2_list = input[point2_index][0]
          time_diff = relation_data[1]
          _relation_results = self.get_time_by_annotation_relation(time_diff=time_diff,annotationid1_list=annotation_list,annotationid2_list=annotationid2_list)
          for subjectid, _tmp_relation_list in _relation_results.items():
            _before_dict = {}
            _after_dict = {}
            for _tmp_relation_data in _tmp_relation_list:
              _tmp_anno1,_tmp_anno2,_tmp_time_list = _tmp_relation_data
              for _tmp_time in _tmp_time_list:
                _tmp_time1, _tmp_time2 = _tmp_time
                try:
                  _before_dict[_tmp_time1].append((_tmp_time2,_tmp_anno1,_tmp_anno2))
                except:
                  _before_dict[_tmp_time1] = [(_tmp_time2,_tmp_anno1,_tmp_anno2)]
                try:
                  _after_dict[_tmp_time2].append((_tmp_time1,_tmp_anno1,_tmp_anno2))
                except:
                  _after_dict[_tmp_time2] = [(_tmp_time1,_tmp_anno1,_tmp_anno2)]
            pt_relation_dict[subjectid] = [_before_dict, _after_dict]
          all_relations_dict[relation_key] = pt_relation_dict
      else:
        _point_result = self.get_time_by_annotation(annotationid_list = annotation_list)
        all_relations_dict[str(point_index)] = _point_result
    
    print("all_relations_by_points", all_relations_by_points)
    print("all_relations_dict",all_relations_dict.keys())

    cadicates_set = None
    cadicates_timeline = {}
    for relation_key, pt_relation_result in all_relations_dict.items():
      _tmp_subjectid_set = pt_relation_result.keys()
      if cadicates_set == None:
        cadicates_set = _tmp_subjectid_set
      else:
        cadicates_set = cadicates_set & _tmp_subjectid_set
      if len(cadicates_set) == 0:
        return {}
    for subjectid in cadicates_set:
      cadicates_timeline[subjectid] = [ None for i in range(input_length)]
    for point2_index in range(input_length-1,-1):
      relation_keys = all_relations_by_points[point2_index]
      removed_cadicates_set = set([])
      for subjectid in cadicates_set:
        removed_cadicate_flag = False
        tmp_timeline = cadicates_timeline[subjectid]
        # check point2
        for relation_key in relation_keys:
          _tmp_after_dict = all_relations_dict[relation_key][subjectid][1]
          _tmp_point2_times_set = _tmp_after_dict.keys()
          if tmp_timeline[point2_index] == None:
            tmp_timeline[point2_index] = _tmp_point2_times_set
          else:
            tmp_timeline[point2_index] = tmp_timeline[point2_index] & _tmp_point2_times_set
          if len(tmp_timeline[point2_index]) == 0:
            removed_cadicates_set.add(subjectid)
            removed_cadicate_flag = True
            break
        if removed_cadicate_flag:
          break
        # check point1
        for relation_key in relation_keys:
          point1_index = int(relation_key.split("-")[0])
          _tmp_after_dict = all_relations_dict[relation_key][subjectid][1]
          _tmp_point1_times_set = set([])
          for point2_time in tmp_timeline[point2_index]:
            _tmp_point1_times_set = _tmp_point1_times_set.union(_tmp_after_dict[point2_time])
          if tmp_timeline[point1_index] == None:
            tmp_timeline[point1_index] = _tmp_point1_times_set
          else:
            tmp_timeline[point1_index] = tmp_timeline[point1_index] & _tmp_point1_times_set
          if len(tmp_timeline[point1_index]) == 0:
            removed_cadicates_set.add(subjectid)
            removed_cadicate_flag = True
            break
      cadicates_set = cadicates_set-removed_cadicate_flag
    print("cadicates_set", len(cadicates_set))


def get_fixed_segment_start_datetime(segment_start_datetime,segment_duration):
  _start_hour, _start_minute = segment_start_datetime.hour, segment_start_datetime.minute
  _total_minutes = (_start_hour*60 + _start_minute)//segment_duration*segment_duration
  _new_hour = _total_minutes//60
  _new_minute = _total_minutes%60
  _new_datetime = segment_start_datetime.replace(hour=_new_hour,minute=_new_minute,second=0,microsecond=0)
  return _new_datetime
