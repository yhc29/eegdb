import os
import pyedflib
import numpy as np
from datetime import datetime, timedelta

class DataFile:
  def __init__(self,filepath,file_type):
    if file_type == "edf":
      self.__doc,self.__channel_list = self.load_edf(filepath)
    else:
      print(file_type,"is not supported")

  def load_edf(filepath):
    doc = {}
    channel_list = []

    f = pyedflib.EdfReader(filepath)  # https://pyedflib.readthedocs.io/en/latest/#description

    # N_channel = f.signals_in_file
    # sample_rate = f.getSampleFrequency(0)
    # signal_labels = f.getSignalLabels()
    # start_datetime = f.getStartdatetime()
    # data = f.readSignal
    # duration = f.getFileDuration()
    header = f.getHeader()
    print(header)
    return doc,channel_list