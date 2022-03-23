import sys
sys.path.insert(0, '..')

import numpy as np
import os
import math
import datetime
import pymongo



BATCH_SIZE = 5000

class Eegdb:
  def __init__(self,mongo_url,db_name,output_folder,data_folder=None,):
    self.__mongo_client = pymongo.MongoClient(mongo_url)
    self.__database = self.__mongo_client[db_name]
    self.__output_folder = output_folder
    if not os.path.exists(output_folder):
      os.makedirs(output_folder)
    self.__data_folder = data_folder
  



  