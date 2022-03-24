import sys
sys.path.insert(0, '..')

import numpy as np
import os
import math
import datetime
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
    print(num_docs,"docs imported with",num_batch,"batches")
  
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

  def import_csr_eeg_file(self,subjectid,sessionid,filepath,max_segment_length=1):
    print("import",subjectid,sessionid,filepath)

    print("load edf file")
    file_type = "edf"
    data_file = DataFile(subjectid,filepath,file_type,sessionid)

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

  def build_index(self):
    self.__database["files"].create_index([("subjectid","hashed")])
    self.__database["files"].create_index([("start_datetime",1),("end_datetime",1)])

    self.__database["segments"].create_index([("subjectid","hashed"),("channel_labels",1),("start_datetime",1),("end_datetime",1)])


  def data_export(self,subjectid,channel_list,query_start_datetime=None,query_end_datetime=None):
    if not query_start_datetime:
      query_start_datetime = datetime(2000,1,1)
    if not query_end_datetime:
      query_end_datetime = datetime(2099,12,31)
    
    query_stmt = {"subjectid":subjectid, "channel_label":{"$in":channel_list}}

  def generate_export_data(self,segment_docs):
    export_data = {}
    for segment_doc in segment_docs:
      seg_key_data = [ segment_doc["subjectid"], str(segment_doc["channel_index"]), segment_doc["channel_label"], str(segment_doc["sample_rate"])]
      seg_key = "|".join(seg_key_data)
      segment_signals_doc = {"start_datetime":segment_doc["start_datetime"],"end_datetime":segment_doc["end_datetime"],"signals":segment_doc["signals"]}
      try:
        export_data[seg_key].append(segment_signals_doc)
      except:
        export_data[seg_key] = [segment_signals_doc]
    for seg_key, segment_signals_docs in export_data.items():
      export_data[seg_key] = self.merge_signals(segment_signals_docs)
    return export_data

  def merge_signals(segment_signals_docs):
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
      else:
        section_list.append(section.copy())
        section = {"start_datetime":segment_start_datetime,"end_datetime":segment_end_datetime,"signals":segment_signals}
    if section:
      section_list.append(section.copy())
    return section_list

  



