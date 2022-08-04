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
    # point: (annotation_list, exclude_annotation_list, relation_list)
    # relation_list: [(input_index,time_diff)]
    if not input or len(input)<2:
      print("ERROR at temproal_query: input is empty or size<2")

    input_length = len(input)
    candidates_set = None
    annotationid_pt_time_dict = {}
    input_relation_dict = {}
    for point_index, point_data in enumerate(input):
      annotationid_list = point_data[0]
      relation_list = point_data[2]
      if relation_list:
        input_relation_dict[point_index] = relation_list
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
      matched_patterns = []
      for time, annotation_list in sorted(time_annotation_dict.items(),key=lambda x:x[0],reverse=False):
        # print(time.strftime("%m/%d/%Y, %H:%M:%S"),annotation_list)
        for annotation in sorted(annotation_list,key=lambda x:x[0],reverse=False):
          _tmp_point_index = annotation[0]
          _tmp_annotationid = annotation[1]
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
          # print("*********************")
          # for patterns in patterns_by_length:
          #   print(patterns)
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
          _relation_results = self.get_time_by_annotation_relation(time_diff=time_diff,annotationid1_list=annotation_list,annotationid2_list=annotationid2_list)
          # print(_tmp_timer.click())
          for subjectid, _tmp_relation_list in _relation_results.items():
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
                    node2 = all_nodes_by_subject_dict[subjectid][point_index][node2_key]
                  except:
                    node2 = {}
                    all_nodes_by_subject_dict[subjectid][point_index][node2_key] = node2
                  try:
                    node2[point_index].add(node1_key)
                  except:
                    node2[point_index] = set([node1_key])

    def annotation_pattern_match_dfs(subjectid,point_index,node1_key):
      new_patterns = []
      if point_index == input_length-1:
        new_patterns.append([node1_key])
        return new_patterns

      node1 = all_nodes_by_subject_dict[subjectid][point_index][node1_key]
      # check if the relations link to the nodes before are satisfied
      for pre_point_index in all_relations_by_point[point_index]:
        try:
          if node1_key not in node1[pre_point_index].keys():
            return None
        except:
          return None
      # check next point
      try:
        node2_keys = node1[point_index+1]
      except:
        return None
      for node2_key in node2_keys:
        next_patterns = annotation_pattern_match_dfs(subjectid,point_index+1,node2_key)
        if next_patterns:
          for next_pattern in next_patterns:
            new_patterns.append([node1_key] + next_pattern)

      return new_patterns

    print("all_nodes_by_subject_dict",all_nodes_by_subject_dict.keys())
    result = {}
    for subjectid, subject_nodes_by_point_dict in all_nodes_by_subject_dict.items():
      pt_patterns = []
      if len(subject_nodes_by_point_dict.keys()) < len(input_length):
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
