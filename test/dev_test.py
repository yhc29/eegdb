import sys
sys.path.insert(0, '..')

from Utils.timer import Timer

from Eegdb.data_file import DataFile

def dev_test():
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

if __name__ == '__main__':
  my_timer = Timer()

  dev_test()

  print(my_timer.stop())