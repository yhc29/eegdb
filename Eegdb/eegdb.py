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

  def import_docs(self,doc_list,collection,batch_size=None):
    if not batch_size:
      batch_size = BATCH_SIZE

    num_docs = len(doc_list)
    num_batch = math.ceil(num_docs/batch_size)
    for i in range(num_batch):
      _ = self.__database[collection].insert_many(doc_list[i*batch_size:(i+1)*batch_size])
    print(num_docs,"docs imported with",num_batch,"batches")
  
  def import_csr_eeg_file(self,subjectid,sessionid,filepath):
    file_type = "edf"
    datafile = DataFile(subjectid,filepath,file_type,sessionid)

    # import file
    file_doc = datafile.get_doc()
    file_collection = "files"
    self.import_docs([file_doc],file_collection)

    # import segments
    segments = datafile.segmentation(1)
    segments_collection = "segments"
    self.import_docs(segments,segments_collection)
  



