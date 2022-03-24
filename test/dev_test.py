import sys
sys.path.insert(0, '..')

from Utils.timer import Timer

from Eegdb.data_file import DataFile
from Eegdb.eegdb import Eegdb

import config.db_config_ibm as config_file

def read_test():
  subjectid = "BJED03029788302444"
  sessionid = "BJED0302978830244401"
  filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/BJED0302978830244401-20160803-162846-21600.edf"
  file_type = "edf"
  datafile = DataFile(subjectid,filepath,file_type,sessionid)
  segments = datafile.segmentation(1)
  print(len(segments))
  segment_doc = segments[100000].get_doc()
  for key,value in segment_doc.items():
    print(key,value)

def import_test():
  eegdb = Eegdb(config_file.mongo_url,config_file.eegdb_name,config_file.output_folder,config_file.data_folder)
  eegdb.drop_collections(["files","segments"])

  subjectid = "BJED03029788302444"
  sessionid = "BJED0302978830244401"
  filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/eegdb_test/BJED0302978830244401/BJED0302978830244401-20160803-162846-21600.edf"

  eegdb.import_csr_eeg_file(subjectid,sessionid,filepath)


if __name__ == '__main__':
  my_timer = Timer()

  # read_test()
  import_test()

  print(my_timer.stop())