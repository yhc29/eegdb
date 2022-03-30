import sys
sys.path.insert(0, '..')

import numpy as np
import os
import math
from datetime import datetime
import pymongo

from Eegdb.data_file import DataFile

BATCH_SIZE = 5000

class Eegdb:
  def __init__(self,mongo_url,db_name,output_folder,data_folder=None):
    self.__mongo_client = pymongo.MongoClient(mongo_url)
    self.__database = self.__mongo_client[db_name]
    self.__output_folder = output_folder
    if not os.path.exists(output_folder):
      os.makedirs(output_folder)
    self.__data_folder = data_folder
    if not os.path.exists(self.__data_folder):
      print("Data folder",self.__data_folder,"does not exist.")

  def drop_collections(self,collection_name_list):
    for collection_name in collection_name_list:
      self.__database[collection_name].drop()

  def import_docs(self,doc_list,collection,batch_size=None):
    if not batch_size:
      batch_size = BATCH_SIZE

    num_docs = len(doc_list)
    num_batch = math.ceil(num_docs/batch_size)
    for i in range(num_batch):
      _ = self.__database[collection].insert_many(doc_list[i*batch_size:(i+1)*batch_size])
    print(num_docs,"docs imported to",collection,"with",num_batch,"batches")
  
  def import_data_file(self,data_file,max_segment_length=1):
    # import file
    print("import edf file info to database")
    file_doc = data_file.get_doc()
    file_collection = "files"
    self.import_docs([file_doc],file_collection)

    # import segments
    print("segmentation with max_segment_length =",max_segment_length)
    segment_docs = [x.get_doc() for x in data_file.segmentation(max_segment_length)]
    segments_collection = "segments"
    print("import segment data to database")
    self.import_docs(segment_docs,segments_collection)

  def import_csr_eeg_file(self,subjectid,sessionid,filepath,max_segment_length=1,annotation_filepath=None,check_existing=True):
    import_edf_flag = True
    import_annotation_flag = True

    print("import",subjectid,sessionid,filepath,annotation_filepath)

    if filepath:
      if check_existing:
        fileid = filepath.split("/")[-1]
        existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          print(filepath,"exists, skip import edf.")
          import_edf_flag = False

      if import_edf_flag:
        # print("load edf file")
        file_type = "edf"
        try:
          data_file = DataFile(subjectid,filepath,file_type,sessionid)
        except:
          print("Read EDF error on", filepath)
          return -1

        # import file
        # print("import edf file info to database")
        file_doc = data_file.get_doc()
        file_collection = "files"
        self.import_docs([file_doc],file_collection)

        # import segments
        # print("import segment data to database, segmentation with max_segment_length =",max_segment_length)
        segment_docs = [x.get_doc() for x in data_file.segmentation(max_segment_length)]
        segments_collection = "segments"
        self.import_docs(segment_docs,segments_collection)

    # import annotation
    if annotation_filepath:
      if check_existing:
        fileid = annotation_filepath.split("/")[-1].split(".")[0]+".edf"
        existing_file_doc = self.__database["annotations"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          print(filepath,"exists, skip import annotation.")
          import_annotation_flag = False

      if import_annotation_flag:
        annotation_docs = data_file.load_annotations(annotation_filepath)
        if annotation_filepath:
          annotation_collection = "annotations"
          # print("import annotation data to database")
          self.import_docs(annotation_docs,annotation_collection)

  def build_index(self):
    print("build_index: files")
    self.__database["files"].create_index([("subjectid","hashed")])
    self.__database["files"].create_index([("start_datetime",1),("end_datetime",1)])
    print("build_index: segments")
    self.__database["segments"].create_index([("subjectid",1),("channel_labels",1),("start_datetime",1),("end_datetime",1)])


  def data_export(self,subjectid_list,channel_list,query_start_datetime=None,query_end_datetime=None):
    if not query_start_datetime:
      query_start_datetime = datetime(1900,1,1)
    if not query_end_datetime:
      query_end_datetime = datetime(2099,12,31)
    
    query_stmt = {"subjectid":{"$in":subjectid_list}, "channel_label":{"$in":channel_list}, "start_datetime":{"$lt":query_end_datetime}, "end_datetime":{"$gt":query_start_datetime}}
    segment_docs = self.__database["segments"].find(query_stmt)
    export_data = self.generate_export_data(segment_docs)
    # clip to match query time window
    for key,signals_doc_list in export_data.items():
      key_in_list = key.split("|")
      sample_rate = float(key_in_list[-1])
      signals_doc_list[0] = self.clip_signals(signals_doc_list[0],sample_rate,query_start_datetime,query_end_datetime)
      if len(signals_doc_list)>1:
        signals_doc_list[-1] = self.clip_signals(signals_doc_list[-1],sample_rate,query_start_datetime,query_end_datetime)
    return export_data


  def generate_export_data(self,segment_docs):
    export_data_by_file = {}
    for segment_doc in segment_docs:
      seg_key_data = [ segment_doc["subjectid"],segment_doc["fileid"], str(segment_doc["channel_index"]), segment_doc["channel_label"], str(segment_doc["sample_rate"])]
      seg_key = "|".join(seg_key_data)
      segment_signals_doc = {"start_datetime":segment_doc["start_datetime"],"end_datetime":segment_doc["end_datetime"],"signals":segment_doc["signals"]}
      try:
        export_data_by_file[seg_key].append(segment_signals_doc)
      except:
        export_data_by_file[seg_key] = [segment_signals_doc]
    export_data_by_channel = {}
    for seg_key, segment_signals_docs in export_data_by_file.items():
      seg_key_in_list = seg_key.split("|")
      sample_rate = float(seg_key_in_list[-1])
      seg_key_in_list.pop(1)
      new_seg_key = "|".join(seg_key_in_list)
      merged_signals = self.merge_segment_signals(segment_signals_docs,sample_rate)
      try:
        export_data_by_channel[new_seg_key] += merged_signals
      except:
        export_data_by_channel[new_seg_key] = merged_signals
    for seg_key, segment_signals_docs in export_data_by_channel.items():
      seg_key_in_list = seg_key.split("|")
      sample_rate = float(seg_key_in_list[-1])
      export_data_by_channel[seg_key] = self.merge_segment_signals(segment_signals_docs,sample_rate)
    return export_data_by_channel

  def merge_segment_signals(self,segment_signals_docs,sample_rate):
    section_list = []
    segment_signals_docs = sorted(segment_signals_docs,key=lambda x:x["start_datetime"])
    section = None
    for segment_signals_doc in segment_signals_docs:
      segment_start_datetime = segment_signals_doc["start_datetime"]
      segment_end_datetime = segment_signals_doc["end_datetime"]
      segment_signals = segment_signals_doc["signals"]
      if not section:
        section = {"start_datetime":segment_start_datetime,"end_datetime":segment_end_datetime,"signals":segment_signals}
        continue

      if segment_start_datetime == section["end_datetime"]:
        section["end_datetime"] = segment_end_datetime
        section["signals"] += segment_signals
      elif segment_start_datetime < section["end_datetime"]:
        if segment_end_datetime > section["end_datetime"]:
          offset_in_seconds = (section["end_datetime"]-segment_start_datetime).seconds
          offset_in_data_points = int(offset_in_seconds*sample_rate)
          new_segment_signals = segment_signals[offset_in_data_points:]
          section["end_datetime"] = segment_end_datetime
          section["signals"] += new_segment_signals
      else:
        section_list.append(section.copy())
        section = {"start_datetime":segment_start_datetime,"end_datetime":segment_end_datetime,"signals":segment_signals}
    if section:
      section_list.append(section.copy())
    return section_list
  
  def clip_signals(self,signals_doc,sample_rate,clip_start_datetime,clip_end_datetime):
    if clip_start_datetime and clip_end_datetime and clip_start_datetime>clip_end_datetime:
      print("Error: clip_start_datetime should be ealier than clip_end_datetime")
      return -1
    signals_start_datetime = signals_doc["start_datetime"]
    signals_end_datetime = signals_doc["end_datetime"]
    signals = signals_doc["signals"]

    if clip_start_datetime>signals_start_datetime and clip_start_datetime<=signals_end_datetime:
      offset_in_seconds = (clip_start_datetime-signals_start_datetime).seconds
      start_offset_in_data_points = int(offset_in_seconds*sample_rate)
      new_signals_start_datetime = clip_start_datetime
    else:
      start_offset_in_data_points = 0
      new_signals_start_datetime = signals_start_datetime

    if clip_end_datetime>signals_start_datetime and clip_end_datetime<signals_end_datetime:
      offset_in_seconds = (clip_end_datetime-signals_start_datetime).seconds
      end_offset_in_data_points = int(offset_in_seconds*sample_rate)
      new_signals_end_datetime = clip_end_datetime
    else:
      end_offset_in_data_points = len(signals)
      new_signals_end_datetime = signals_end_datetime

    
    new_segment_signals = signals[start_offset_in_data_points:end_offset_in_data_points]
    new_signals_doc = {"start_datetime":new_signals_start_datetime,"end_datetime":new_signals_end_datetime,"signals":new_segment_signals}
    return new_signals_doc


