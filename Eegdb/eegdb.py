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
  



