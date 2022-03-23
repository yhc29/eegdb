import sys
sys.path.insert(0, '..')

from Utils.timer import Timer

from Eegdb.data_fileDataFile import DataFile

def dev_test():
  filepath = ""
  file_type = "edf"
  datafile = DataFile(filepath,file_type)

if __name__ == '__main__':
  my_timer = Timer()

  dev_test()

  print(my_timer.stop())