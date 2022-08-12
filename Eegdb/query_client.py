from nis import match
import re
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
    return self.__database["annotation_relation_pt_timeline_v2"]
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
  

  def get_time_by_annotation_relations_test(self, relations, subjectid_list = None ):
    # todo: 2 relations with same anno1, anno2, different time_diff
    # relation: (anno1_list,anno2_list,time_diff)
    relation_index_dict = {}
    for relation_index,relation in enumerate(relations):
      annotationid1_list,annotationid2_list,time_diff = relation
      for annotationid1 in annotationid1_list:
        for annotationid2 in annotationid2_list:
          try:
            relation_index_dict[(annotationid1,annotationid2)].append((relation_index,time_diff))
          except:
            relation_index_dict[(annotationid1,annotationid2)] = [(relation_index,time_diff)]
    or_stmt = []
    for k,v in relation_index_dict.items():
      annotationid1,annotationid2 = k
      new_time_diff = v[0][1]
      if len(v)>1:
        for relation_index,time_diff in v[1:]:
          if time_diff[0]<new_time_diff[0]:
            new_time_diff[0] = time_diff[0]
          if time_diff[1]>new_time_diff[1]:
            new_time_diff[1] = time_diff[1]
      or_stmt.append({
        "annotationid1": annotationid1, 
        "annotationid2": annotationid2, 
        "time_diff": { "$elemMatch": { "$gte": new_time_diff[0], "$lte": new_time_diff[1] } } })
    if subjectid_list:
      _match_stmt = { "subjectid":{"$in":subjectid_list}, 
      "$or": or_stmt}
    else:
      _match_stmt = { "$or": or_stmt}
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
                    "cond": {"$gte": ["$$item",time_diff[0]]}
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
      if subjectid == "XKNQ88462976439395" and annotationid1 == 4 and annotationid2 == 25:
        print(time)
      for relation_index,time_diff in relation_index_dict[(annotationid1,annotationid2)]:
        try:
          results[subjectid][relation_index].append((annotationid1,annotationid2,time))
        except:
          results[subjectid] = [ [] for i in range(len(relations)) ]
          results[subjectid][relation_index].append((annotationid1,annotationid2,time))
    return results

  def get_time_by_annotation_relations(self, relations, subjectid_list = None ):
    # relation: (anno1_list,anno2_list,time_diff)
    candidates_set = set(subjectid_list) if subjectid_list else None
    result = {}
    for relation_index,relation in enumerate(relations):
      anno1_list,anno2_list,time_diff = relation
      subjectid_list = list(candidates_set) if candidates_set else None
      relation_result = self.get_time_by_annotation_relation(time_diff = time_diff, annotationid1_list = anno1_list, annotationid2_list = anno2_list, subjectid_list = subjectid_list )
      if candidates_set == None :
        candidates_set = relation_result.keys()
      else:
        candidates_set = candidates_set & relation_result.keys()
      if not candidates_set:
        return None
      for subjectid in candidates_set:
        if relation_index == 0:
          result[subjectid] = [None for i in range(len(relations))]
          result[subjectid][0] = relation_result[subjectid]
        else:
          result[subjectid][relation_index] = relation_result[subjectid]
    new_result = {}
    for subjectid in candidates_set:
      new_result[subjectid] = result[subjectid]
    return new_result

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
      _match_stmt = { "subjectid":{"$in":subjectid_list}, "annotationid1": {"$in":annotationid1_list}, "annotationid2": {"$in":annotationid2_list}, "time_diff": { "$elemMatch": { "$gte": time_diff[0], "$lte": time_diff[1] } } }
    else:
      _match_stmt = { "annotationid1": {"$in":annotationid1_list},"annotationid2": {"$in":annotationid2_list}, "time_diff": { "$elemMatch": { "$gte": time_diff[0], "$lte": time_diff[1] } } }
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
                    "cond": {"$gte": ["$$item",time_diff[0]]}
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

  def temproal_query_v0(self,input):
    # print(input)
    # input: [ (point1), (point2), ...]
    # point: (annotation_list, negation_flag, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input or len(input)<2:
      print("ERROR at temproal_query: input is empty or size<2")
      return -1
    neg_point_index_list = []
    input_length = len(input)
    candidates_set = None
    annotationid_pt_time_dict = {}
    input_relation_dict = {}
    neg_relation_dict = {}
    for point_index, point_data in enumerate(input):
      annotationid_list,negation_flag,relation_list = point_data
      if negation_flag:
        neg_point_index_list.append(point_index)

      if relation_list:
        input_relation_dict[point_index] = relation_list
        for relation in relation_list:
          next_point_index = relation[0]
          if negation_flag or input[next_point_index][1]:
            try: 
              neg_relation_dict[point_index].append(next_point_index)
            except:
              neg_relation_dict[point_index] = [next_point_index]

      _ap_stmt = [
        { "$match" : { "annotationid_partial": {"$in":annotationid_list}}},
        { "$project": {"_id": 0, "subjectid": 1, "annotationid_partial":1, "time": 1} },
        { "$group": { 
          "_id": {"subjectid": "$subjectid", "annotationid": "$annotationid_partial"},
          "time_list":{"$addToSet":"$time"},
        } }
      ]
      docs = self.get_annotations_collection().aggregate(_ap_stmt,allowDiskUse=False)
      new_candidates_set = set([])
      for doc in docs:
        subjectid = doc["_id"]["subjectid"]
        annotationid = doc["_id"]["annotationid"]
        try:
          annotationid_pt_time_dict[subjectid] += [(point_index,x,annotationid) for x in doc["time_list"]]
        except:
          annotationid_pt_time_dict[subjectid] = [(point_index,x,annotationid) for x in doc["time_list"]]
        new_candidates_set.add(subjectid)
      if not negation_flag:
        if candidates_set == None:
          candidates_set = new_candidates_set
        else:
          candidates_set = candidates_set & new_candidates_set
        if len(candidates_set) == 0:
          break
    # print("candidates_set", len(candidates_set))
    if len(candidates_set) == 0:
      return {}

    # candidates_set = set(["147F147Z0881747573"])
    # for k,v in input_relation_dict.items():
    #   print(k,v)

    result = {}
    for subjectid in candidates_set:
      time_annotation_dict = {}
      for point_index,time,annotationid in annotationid_pt_time_dict[subjectid]:
        try:
          time_annotation_dict[time].append( (point_index,annotationid) )
        except:
          time_annotation_dict[time] = [(point_index,annotationid)]
      patterns_by_length = [ [] for i in range(input_length-1)]
      for i in range(input_length-1):
        if input[i][1] == True:
          new_pattern = [None for i in range(input_length)]
          patterns_by_length[i].append(new_pattern)
        else:
          break
      matched_patterns = []
      for time, annotation_list in sorted(time_annotation_dict.items(),key=lambda x:x[0],reverse=False):
        # print(time.strftime("%m/%d/%Y, %H:%M:%S"),annotation_list)
        for annotation in sorted(annotation_list,key=lambda x:x[0],reverse=False):
          _tmp_point_index,_tmp_annotationid = annotation
          if _tmp_point_index == 0:
            new_pattern = [None for i in range(input_length)]
            new_pattern[0] = (time,_tmp_annotationid)
            try:
              for relation_list in input_relation_dict[0]:
                _tmp_point2_index = relation_list[0]
                _tmp_time_diff = relation_list[1]
                new_pattern[_tmp_point2_index] = [time+relativedelta(seconds=_tmp_time_diff[0]),time+relativedelta(seconds=_tmp_time_diff[1])]
            except:
              pass
            patterns_by_length[0].append(new_pattern)
            for i in range(1,input_length):
              if input[i][1] == True:
                # new_pattern[i] = None
                patterns_by_length[i].append(new_pattern)
              else:
                break
          else:
            for existing_pattern in patterns_by_length[_tmp_point_index-1]:
              new_pattern = existing_pattern.copy()
              if new_pattern[_tmp_point_index]==None or (new_pattern[_tmp_point_index] and time>=new_pattern[_tmp_point_index][0] and time<=new_pattern[_tmp_point_index][1]):
                new_pattern[_tmp_point_index] = (time,_tmp_annotationid)
                if _tmp_point_index == input_length-1:
                  matched_patterns.append(new_pattern)
                else:
                  try:
                    for relation_list in input_relation_dict[_tmp_point_index]:
                      _tmp_point2_index = relation_list[0]
                      _tmp_time_diff = relation_list[1]
                      new_pattern[_tmp_point2_index] = [time+relativedelta(seconds=_tmp_time_diff[0]),time+relativedelta(seconds=_tmp_time_diff[1])]
                  except:
                    pass
                  patterns_by_length[_tmp_point_index].append(new_pattern)
                  for i in range(_tmp_point_index+1,input_length):
                    if input[i][1] == True:
                      # new_pattern[i] = None
                      if i<input_length-1:
                        patterns_by_length[i].append(new_pattern)
                      else:
                        matched_patterns.append(new_pattern)
                    else:
                      break
      # if subjectid == "XKNQ88462976439395":
      #   print("*********************")
      #   for i,patterns in enumerate(patterns_by_length):
      #     for p in patterns:
      #       if p[0] == None:
      #         print(p)

      # an annotation should not appear in two points
      new_matched_patterns = []
      for matched_pattern in matched_patterns:
        matched_pattern = [a if type(a) is tuple else None for a in matched_pattern]
        tmp_annotation_list = [a for a in matched_pattern if a]
        if len(set(tmp_annotation_list)) == len(tmp_annotation_list):
          new_matched_patterns.append(matched_pattern)
      matched_patterns = new_matched_patterns

      if neg_point_index_list:
        # if subjectid == "HLCV89FV9555554111":
        #   for matched_pattern in matched_patterns:
        #     print(matched_pattern)
        matched_negation_annotation_set = set([])
        new_matched_patterns = []
        for matched_pattern in matched_patterns:
          if all([ matched_pattern[i]==None for i in neg_point_index_list]):
            new_matched_patterns.append([x for x in matched_pattern if x is not None])
          else:
            for p1i,p2i_list in neg_relation_dict.items():
              for p2i in p2i_list:
                if matched_pattern[p1i] and matched_pattern[p2i]:
                  if p1i not in neg_point_index_list:
                    matched_negation_annotation_set.add(matched_pattern[p1i])
                  if p2i not in neg_point_index_list:
                    matched_negation_annotation_set.add(matched_pattern[p2i])
        matched_patterns = new_matched_patterns
        new_matched_patterns = []
        for matched_pattern in matched_patterns:
          _tmp_flag = True
          for _tmp_annotation in matched_pattern:
            if _tmp_annotation in matched_negation_annotation_set:
              _tmp_flag = False
              break
          if _tmp_flag:
            new_matched_patterns.append(matched_pattern)
        matched_patterns = new_matched_patterns
        # if subjectid == "HLCV89FV9555554111":
        #   print("after neg check")
        #   for matched_pattern in matched_patterns:
        #     print(matched_pattern)

      matched_annotation_set = set([])
      new_matched_patterns = []
      if matched_patterns:
        for matched_pattern in sorted(matched_patterns,key=lambda x:x[0][0],reverse=False):
          if not any([x in matched_annotation_set for x in matched_pattern]):
            matched_annotation_set = matched_annotation_set.union(set(matched_pattern))
            new_matched_patterns.append(matched_pattern)
        result[subjectid] = new_matched_patterns

    return result

  def temproal_query_v1(self,input):
    # p_timer = Timer()
    # input: [ (point1), (point2), ...]
    # point: (annotation_list, exclude_annotation_list, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input or len(input)<2:
      print("ERROR at temproal_query: input is empty or size<2")

    input_length = len(input)
    all_relations_by_point = [ [] for i in range(input_length)]
    all_nodes_by_subject_dict = {}
    candidate_set = None
    for point_index, point_data in enumerate(input):
      if point_index == input_length-1 and not all_relations_by_point[point_index]:
        break
      annotation_list, exclude_annotation_list, relation_list = point_data
      # add a before relation to next point if not exists
      if not relation_list or sorted(relation_list,key=lambda x:x[0])[0][0]!=point_index+1:
        if point_index != input_length-1:
          relation_list.insert( 0,(point_index+1,[0,24*3600]) )
      else:
        relation_list = sorted(relation_list,key=lambda x:x[0])

      if relation_list:
        # todo set more precise time diff

        for relation_data in relation_list:
          # print(relation_data)
          point2_index = relation_data[0]
          if point2_index>point_index+1:
            all_relations_by_point[point2_index].append(point_index)
          annotationid2_list = input[point2_index][0]
          time_diff = relation_data[1]
          # print(time_diff,annotation_list,annotationid2_list)
          # _tmp_timer = Timer()
          candidate_list = list(candidate_set) if candidate_set else None
          _relation_results = self.get_time_by_annotation_relation(time_diff=time_diff,annotationid1_list=annotation_list,annotationid2_list=annotationid2_list,subjectid_list=candidate_list)
          new_candidate_set = set(_relation_results.keys())
          if candidate_set == None:
            candidate_set = new_candidate_set
          else:
            candidate_set = candidate_set & new_candidate_set
            if len(candidate_set) == 0:
              return {}
          # print(_tmp_timer.click())
          # for subjectid in candidate_set:
          #   _tmp_relation_list = _relation_results[subjectid]
          for subjectid, _tmp_relation_list in _relation_results.items():
            if subjectid not in candidate_set:
              continue
            try:
              all_nodes_by_subject_dict[subjectid]
            except:
              if point_index == 0 and point2_index == 1:
                all_nodes_by_subject_dict[subjectid] = {}
              else:
                continue
            try:
              all_nodes_by_subject_dict[subjectid][point_index]
            except:
              all_nodes_by_subject_dict[subjectid][point_index] = {}
            if point2_index>point_index+1:
              try:
                all_nodes_by_subject_dict[subjectid][point2_index]
              except:
                all_nodes_by_subject_dict[subjectid][point2_index] = {}

            for _tmp_relation_data in _tmp_relation_list:
              _tmp_anno1,_tmp_anno2,_tmp_time_list = _tmp_relation_data
              for _tmp_time in _tmp_time_list:
                _tmp_time1, _tmp_time2 = _tmp_time
                node1_key = (_tmp_time1,_tmp_anno1)
                node2_key = (_tmp_time2,_tmp_anno2)

                try:
                  node1 = all_nodes_by_subject_dict[subjectid][point_index][node1_key]
                except:
                  node1 = {}
                  all_nodes_by_subject_dict[subjectid][point_index][node1_key] = node1
                try:
                  node1[point2_index].add(node2_key)
                except:
                  node1[point2_index] = set([node2_key])
                if point2_index>point_index+1:
                  try:
                    node2 = all_nodes_by_subject_dict[subjectid][point2_index][node2_key]
                  except:
                    node2 = {}
                    all_nodes_by_subject_dict[subjectid][point2_index][node2_key] = node2
                  try:
                    node2[point_index].add(node1_key)
                  except:
                    node2[point_index] = set([node1_key])

    def annotation_pattern_match_dfs(subjectid,point_index,node1_key,matched_pattern = []):
      # if subjectid == "QEKK75731133138110":
      #   print(point_index,node1_key)
      new_patterns = []
      if point_index == input_length-1:
        new_patterns.append(matched_pattern+[node1_key])
        return new_patterns
      try:
        node1 = all_nodes_by_subject_dict[subjectid][point_index][node1_key]
      except:
        return None
      # check if the relations link to the nodes before are satisfied
      for pre_point_index in all_relations_by_point[point_index]:
        try:
          if matched_pattern[pre_point_index] not in node1[pre_point_index]:
            return None
        except:
          return None
      # check next point
      try:
        node2_keys = node1[point_index+1]
      except:
        return None
      for node2_key in node2_keys:
        next_patterns = annotation_pattern_match_dfs(subjectid,point_index+1,node2_key,matched_pattern+[node1_key])
        if next_patterns:
          new_patterns+= next_patterns
          for next_pattern in next_patterns:
            new_patterns.append(matched_pattern + [node1_key] + next_pattern)

      return new_patterns
    
    # print("all_nodes_by_subject_dict",len(all_nodes_by_subject_dict.keys()))
    print("candidate_set",len(candidate_set))
    result = {}
    for subjectid in candidate_set:
      subject_nodes_by_point_dict = all_nodes_by_subject_dict[subjectid]
    # for subjectid, subject_nodes_by_point_dict in all_nodes_by_subject_dict.items():
      pt_patterns = []
      # if subjectid == "QEKK75731133138110":
      # # if subjectid:
      #   for i,nodes in subject_nodes_by_point_dict.items():
      #     print(i)
      #     for k,v in nodes.items():
      #       print(k,v)
      if len(subject_nodes_by_point_dict.keys()) < input_length-1:
        continue
      for start_node_key in subject_nodes_by_point_dict[0].keys():
        patterns = annotation_pattern_match_dfs(subjectid,0,start_node_key)
        if patterns:
          pt_patterns += patterns

      # distinct nodes
      distinct_patterns = []
      node_set = set([])
      for pt_pattern in pt_patterns:
        if not any([x in node_set for x in pt_pattern]):
          distinct_patterns.append(pt_pattern)
          node_set=node_set.union(pt_pattern)
      if distinct_patterns:
        result[subjectid] = distinct_patterns
    # for p in result["QEKK75731133138110"]:
    #   print(p)
    return result

  def temproal_query_v1_1_with_negation(self,input):
    non_neg_input = []
    neg_input_dict = {}
    for point_index,point_data in enumerate(input):
      annotation_list, neg_flag, relation_list = point_data
      if neg_flag:
        try:
          neg_input_dict[point_index].append((point_index, annotation_list, neg_flag, relation_list))
        except:
           neg_input_dict[point_index] = [(point_index, annotation_list, neg_flag, relation_list)]
        for relation in relation_list:
          next_point_index, time_diff = relation
          if not input[next_point_index][1]:
            neg_input_dict[point_index].append((next_point_index, input[next_point_index][0], input[next_point_index][1], []))
      else:
        non_neg_relation_list = []
        for relation in relation_list:
          next_point_index, time_diff = relation
          if input[next_point_index][1]:
            try:
              neg_input_dict[next_point_index].append((point_index, annotation_list, neg_flag, [(next_point_index, time_diff)]))
            except:
              neg_input_dict[next_point_index] = [(point_index, annotation_list, neg_flag, [(next_point_index, time_diff)])]
          else:
            non_neg_relation_list.append((next_point_index, time_diff))
        non_neg_input.append( (point_index, annotation_list, neg_flag, non_neg_relation_list) )
    


    non_neng_new_index_mapping = {}
    for new_point_index,point_data in enumerate(non_neg_input):
      point_index, _, _, _ = point_data
      non_neng_new_index_mapping[point_index] = new_point_index
    new_non_neg_input=[]
    for new_point_index,point_data in enumerate(non_neg_input):
      point_index, annotation_list, neg_flag, relation_list = point_data
      new_relation_list = [ (non_neng_new_index_mapping[relation[0]], relation[1]) for relation in relation_list]
      new_non_neg_input.append((annotation_list, neg_flag, new_relation_list))
    # for x in new_non_neg_input:
    #   print(x)
    non_neg_result = self.temproal_query_v1_1(new_non_neg_input,return_all_possibilities=True)
    if not non_neg_result:
      return non_neg_result

    pt_unsatisfied_annotation_set_dict = {}
    for neg_point_index,neg_input in neg_input_dict.items():
      new_index_mapping = {}
      non_neg_point_index_list = []
      for new_point_index,point_data in enumerate(neg_input):
        point_index, _, neg_flag, _ = point_data
        if not neg_flag:
          non_neg_point_index_list.append((new_point_index,non_neng_new_index_mapping[point_index]))
        new_index_mapping[point_index] = new_point_index
      new_neg_input=[]
      for new_point_index,point_data in enumerate(neg_input):
        point_index, annotation_list, neg_flag, relation_list = point_data
        new_relation_list = [ (new_index_mapping[relation[0]], relation[1]) for relation in relation_list]
        new_neg_input.append((annotation_list, neg_flag, new_relation_list))
      # for x in new_neg_input:
      #   print(x)
      neg_result = self.temproal_query_v1_1(new_neg_input,return_all_possibilities=True)
      for subjectid in non_neg_result.keys():
        try:
          pt_neg_patterns = neg_result[subjectid]
        except:
          continue
        for pattern in pt_neg_patterns:
          for i,new_non_neg_point_index in non_neg_point_index_list:
            try:
              pt_unsatisfied_annotation_set_dict[subjectid][new_non_neg_point_index].add(pattern[i])
            except:
              pt_unsatisfied_annotation_set_dict[subjectid] = [set([]) for x in range(len(non_neg_input))]
              pt_unsatisfied_annotation_set_dict[subjectid][new_non_neg_point_index].add(pattern[i])
    result = {}
    for subjectid, pt_patterns in non_neg_result.items():
      distinct_patterns = []
      node_set = set([])
      try:
        unsatisfied_node_set_list = pt_unsatisfied_annotation_set_dict[subjectid]
      except:
        unsatisfied_node_set_list = [set([]) for x in range(len(non_neg_input))]
      for pt_pattern in pt_patterns:
        if not any([x in node_set or x in unsatisfied_node_set_list[i] for i,x in enumerate(pt_pattern)]):
          distinct_patterns.append(pt_pattern)
          node_set=node_set.union(pt_pattern)
      if distinct_patterns:
        result[subjectid] = distinct_patterns
    return result


  def temproal_query_v1_1(self,input,return_all_possibilities=False):
    # p_timer = Timer()
    # input: [ (point1), (point2), ...]
    # point: (annotation_list, exclude_annotation_list, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input or len(input)<2:
      print("ERROR at temproal_query: input is empty or size<2")

    input_length = len(input)
    reversed_relations_by_point = [ [] for i in range(input_length)]
    all_relations_by_point = [ [] for i in range(input_length)]
    query_relations = []
    pt_query_result = {}
    all_nodes_by_subject_dict = {}
    candidate_set = None
    for point_index, point_data in enumerate(input):
      if point_index == input_length-1 and not reversed_relations_by_point[point_index]:
        break
      annotation_list, exclude_annotation_list, relation_list = point_data
      # add a before relation to next point if not exists
      if not relation_list or sorted(relation_list,key=lambda x:x[0])[0][0]!=point_index+1:
        if point_index != input_length-1:
          relation_list.insert( 0,(point_index+1,[0,24*3600]) )
      else:
        relation_list = sorted(relation_list,key=lambda x:x[0])

      if relation_list:
        # todo set more precise time diff
        all_relations_by_point[point_index] = relation_list
        for relation_data in relation_list:
          point2_index = relation_data[0]
          if point2_index>point_index+1:
            reversed_relations_by_point[point2_index].append(point_index)
          query_relations.append((annotation_list,input[point2_index][0],relation_data[1]))

    _relation_results = self.get_time_by_annotation_relations(query_relations)
    for subjectid,relation_data_by_relation_index in _relation_results.items():
      if any([r==[] for r in relation_data_by_relation_index]):
        continue
      # if subjectid == "XKNQ88462976439395":
      #   for x in relation_data_by_relation_index[1]:
      #     print(x)
      relation_index = -1
      for point_index, point_data in enumerate(input):
        relation_list = all_relations_by_point[point_index]
        if relation_list:
          for relation_data in relation_list:
            point2_index = relation_data[0]
            relation_index+=1
            _tmp_relation_list = relation_data_by_relation_index[relation_index]
            try:
              all_nodes_by_subject_dict[subjectid]
            except:
              all_nodes_by_subject_dict[subjectid] = {}
            try:
              all_nodes_by_subject_dict[subjectid][point_index]
            except:
              all_nodes_by_subject_dict[subjectid][point_index] = {}
            if point2_index>point_index+1:
              try:
                all_nodes_by_subject_dict[subjectid][point2_index]
              except:
                all_nodes_by_subject_dict[subjectid][point2_index] = {}

            for _tmp_relation_data in _tmp_relation_list:
              _tmp_anno1,_tmp_anno2,_tmp_time_list = _tmp_relation_data
              for _tmp_time in _tmp_time_list:
                _tmp_time1, _tmp_time2 = _tmp_time
                # if _tmp_anno1 == _tmp_anno2 and _tmp_time1 == _tmp_time2:
                #   continue
                node1_key = (_tmp_time1,_tmp_anno1)
                node2_key = (_tmp_time2,_tmp_anno2)

                try:
                  node1 = all_nodes_by_subject_dict[subjectid][point_index][node1_key]
                except:
                  node1 = {}
                  all_nodes_by_subject_dict[subjectid][point_index][node1_key] = node1
                try:
                  node1[point2_index].add(node2_key)
                except:
                  node1[point2_index] = set([node2_key])
                if point2_index>point_index+1:
                  try:
                    node2 = all_nodes_by_subject_dict[subjectid][point2_index][node2_key]
                  except:
                    node2 = {}
                    all_nodes_by_subject_dict[subjectid][point2_index][node2_key] = node2
                  try:
                    node2[point_index].add(node1_key)
                  except:
                    node2[point_index] = set([node1_key])

    def annotation_pattern_match_dfs(subjectid,point_index,node1_key,matched_pattern = []):
      new_patterns = []
      if point_index == input_length-1:
        new_patterns.append(matched_pattern+[node1_key])
        return new_patterns
      try:
        node1 = all_nodes_by_subject_dict[subjectid][point_index][node1_key]
      except:
        return None
      # check if the relations link to the nodes before are satisfied
      for pre_point_index in reversed_relations_by_point[point_index]:
        try:
          if matched_pattern[pre_point_index] not in node1[pre_point_index]:
            return None
        except:
          return None
      # check next point
      try:
        node2_keys = node1[point_index+1]
      except:
        return None
      for node2_key in node2_keys:
        next_patterns = annotation_pattern_match_dfs(subjectid,point_index+1,node2_key,matched_pattern+[node1_key])
        if next_patterns:
          new_patterns += next_patterns
      return new_patterns

    # print("all_nodes_by_subject_dict",len(all_nodes_by_subject_dict.keys()))
    result = {}
    for subjectid,subject_nodes_by_point_dict in all_nodes_by_subject_dict.items():
      pt_patterns = []
      # if subjectid == "QEKK75731133138110":
      # # if subjectid:
      #   for i,nodes in subject_nodes_by_point_dict.items():
      #     print(i)
      #     for k,v in nodes.items():
      #       print(k,v)
      if len(subject_nodes_by_point_dict.keys()) < input_length-1:
        continue
      for start_node_key in subject_nodes_by_point_dict[0].keys():
        patterns = annotation_pattern_match_dfs(subjectid,0,start_node_key)
        if patterns:
          pt_patterns += patterns

      if return_all_possibilities:
        result[subjectid] = pt_patterns
      else:
        # distinct nodes
        distinct_patterns = []
        node_set = set([])
        for pt_pattern in pt_patterns:
          if not any([x in node_set for x in pt_pattern]):
            distinct_patterns.append(pt_pattern)
            node_set=node_set.union(pt_pattern)
        if distinct_patterns:
          result[subjectid] = distinct_patterns
    # for p in result["QEKK75731133138110"]:
    #   print(p)
    return result




  def temproal_query(self,input):
    # p_timer = Timer()
    # input: [ (point1), (point2), ...]
    # point: (annotation_list, exclude_annotation_list, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input or len(input)<2:
      print("ERROR at temproal_query: input is empty or size<2")

    input_length = len(input)
    all_relations_by_points = [ [] for i in range(input_length)]
    all_relations_dict = {}
    for point_index, point_data in enumerate(input):
      if point_index == input_length-1 and not all_relations_by_points[point_index]:
        break
      annotation_list, exclude_annotation_list, relation_list = point_data
      # add a before relation to next point if not exists
      if not relation_list or sorted(relation_list,key=lambda x:x[0],reverse=False)[0][0]!=point_index+1:
        if point_index != input_length-1:
          relation_list.append( (point_index+1,[0,24*3600]) )

      if relation_list:
        for relation_data in relation_list:
          # print(relation_data)
          pt_relation_dict = {}
          point2_index = relation_data[0]
          relation_key = str(point_index)+"-"+str(point2_index)
          all_relations_by_points[point2_index].append(relation_key)
          annotationid2_list = input[point2_index][0]
          time_diff = relation_data[1]
          # print(time_diff,annotation_list,annotationid2_list)
          # _tmp_timer = Timer()
          _relation_results = self.get_time_by_annotation_relation(time_diff=time_diff,annotationid1_list=annotation_list,annotationid2_list=annotationid2_list)
          # print(_tmp_timer.click())
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
      elif not all_relations_by_points[point_index]:
        _point_result = self.get_time_by_annotation(annotationid_list = annotation_list)
        all_relations_dict[str(point_index)] = _point_result
    
    # print("all_relations_by_points", all_relations_by_points)
    # print("all_relations_dict",all_relations_dict.keys())

    # for time1, data in all_relations_dict["1-2"]["147F147Z0881747573"][0].items():
    #   print(time1)
    #   print(data)
    # print(p_timer.click())
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

    # cadicates_set = set(["MJBD18937780295960"])
    # print(p_timer.click())
    for subjectid in cadicates_set:
      cadicates_timeline[subjectid] = [ None for i in range(input_length)]
    for point2_index in range(input_length-1,-1,-1):
      relation_keys = all_relations_by_points[point2_index]
      removed_cadicates_set = set([])
      for subjectid in cadicates_set:
        # print(subjectid)
        removed_cadicate_flag = False
        tmp_timeline = cadicates_timeline[subjectid]
        # check point2
        for relation_key in relation_keys:
          # print("relation_key",relation_key)
          _tmp_after_dict = all_relations_dict[relation_key][subjectid][1]
          _tmp_point2_times_set = set(_tmp_after_dict.keys())
          # print(_tmp_point2_times_set)
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
            _tmp_point1_times_set = _tmp_point1_times_set.union(set([ x[0] for x in _tmp_after_dict[point2_time]]))
          if tmp_timeline[point1_index] == None:
            tmp_timeline[point1_index] = _tmp_point1_times_set
          else:
            tmp_timeline[point1_index] = tmp_timeline[point1_index] & _tmp_point1_times_set
          if len(tmp_timeline[point1_index]) == 0:
            removed_cadicates_set.add(subjectid)
            removed_cadicate_flag = True
            break
        # if subjectid == "YDTY25419400633238":
        #   print(tmp_timeline)
        if not removed_cadicate_flag:
          cadicates_timeline[subjectid] = tmp_timeline

      cadicates_set = cadicates_set-removed_cadicates_set
    # print("cadicates_set", len(cadicates_set))
    # print(p_timer.click())
    # get patterns
    result = {}
    for subjectid in cadicates_set:
      # print(subjectid)
      tmp_timeline = cadicates_timeline[subjectid]
      if not tmp_timeline[0]:
        continue
      for _tmp_time in sorted(list(tmp_timeline[0]),reverse=False):
        pattern = None
        for point_index in range(input_length-1):
          relation_key = str(point_index)+"-"+str(point_index+1)
          next_relation_key = str(point_index+1)+"-"+str(point_index+2)
          # print(relation_key)
          # print(all_relations_dict[relation_key][subjectid][0].keys())
          _temp_before_relations = all_relations_dict[relation_key][subjectid][0][_tmp_time]
          # if subjectid == "MJBD18937780295960":
          #   print(point_index)
          #   print(_tmp_time)
          #   print(_temp_before_relations)
          for _tmp_time2,_tmp_anno1,_tmp_anno2 in sorted(_temp_before_relations,key = lambda x:x[0],reverse=False):
            if _tmp_time2 in tmp_timeline[point_index+1]:
              if point_index < input_length - 2 and not _tmp_time2 in all_relations_dict[next_relation_key][subjectid][0].keys():
                continue
              if pattern == None:
                pattern = [(_tmp_time,_tmp_anno1)]
              pattern.append((_tmp_time2,_tmp_anno2))
              _tmp_time = _tmp_time2
              break
          if not pattern or len(pattern) != point_index+2:
            break
        # matched
        if pattern and len(pattern) == input_length:
          for i in range(1,input_length):
            tmp_timeline[i].remove(pattern[i][0])
          try:
            result[subjectid].append(pattern)
          except:
            result[subjectid] = [pattern]
    # print(p_timer.click())
    return result


def get_fixed_segment_start_datetime(segment_start_datetime,segment_duration):
  _start_hour, _start_minute = segment_start_datetime.hour, segment_start_datetime.minute
  _total_minutes = (_start_hour*60 + _start_minute)//segment_duration*segment_duration
  _new_hour = _total_minutes//60
  _new_minute = _total_minutes%60
  _new_datetime = segment_start_datetime.replace(hour=_new_hour,minute=_new_minute,second=0,microsecond=0)
  return _new_datetime
