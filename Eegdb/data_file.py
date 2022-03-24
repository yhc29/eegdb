import os
from eegdb.segment import Segment
import pyedflib
import numpy as np
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class DataFile:
  def __init__(self,subjectid,filepath,file_type,sessionid=None):
    self.__doc = {"subjectid":subjectid}
    self.__doc = {"fileid":filepath.split("/")[-1]}
    if sessionid:
      self.__doc["sessionid"] = sessionid

    if file_type == "edf":
      _edf_doc,self.__channel_list = self.load_edf(filepath)
    else:
      print(file_type,"is not supported")
      self.__channel_list = None
  
    self.__doc.update(_edf_doc)


  def load_edf(self,filepath):
    _doc = {}
    _channel_list = []

    f = pyedflib.EdfReader(filepath)  # https://pyedflib.readthedocs.io/en/latest/#description

    N_channel = f.signals_in_file
    _doc["n_channel"] = N_channel
    print("N_channel",N_channel)

    channel_labels = f.getSignalLabels()
    _doc["channel_labels"] = channel_labels
    print("channel_labels",channel_labels)

    start_datetime = f.getStartdatetime()
    _doc["start_datetime"] = start_datetime
    print("start_datetime",start_datetime)

    duration = f.getFileDuration()
    _doc["duration"] = duration
    print("duration",duration)
    end_datetime = start_datetime+relativedelta(seconds = duration)
    _doc["end_datetime"] = end_datetime
    print("end_datetime",end_datetime)

    data = f.readSignal

    for i in range(N_channel):
      channel_label = channel_labels[i]
      sample_rate = f.getSampleFrequency(i)
      signals = data(i)
      # print("signal_label",signal_label,"sample_rate",sample_rate,"signals",signals[:5])
      _channel_doc = {
        "channel_label":channel_label,
        "sample_rate":sample_rate,
        "signals":signals
      }
      _channel_list.append(_channel_doc)
    
    header = f.getHeader()
    print("header",header)
    return _doc,_channel_list

  def segmentation(self,max_length):
    _segments = []
    for channel_doc in self.__channel_list:
      channel_label = channel_doc["channel_label"]
      sample_rate = channel_doc["sample_rate"]
      file_signals = channel_doc["signals"]
      n_data_point = len(file_signals)
      n_segment = math.ceil(self.__doc["duration"]/max_length)
      for segment_index in range(n_segment):
        offset = segment_index*max_length
        start_datetime = self.__doc["start_datetime"] + relativedelta(seconds = offset)
        end_datetime = start_datetime + relativedelta(seconds = max_length)
        if end_datetime > self.__doc["end_datetime"]:
          end_datetime = self.__doc["end_datetime"]

        offset_data_point = offset*sample_rate
        offset_data_point_end = offset_data_point + max_length*sample_rate
        if offset_data_point_end > n_data_point:
          offset_data_point_end = n_data_point
        segment_signals = file_signals[offset_data_point:offset_data_point_end]
        
        segment = Segment(self.__doc["subjectid"],self.__doc["fileid"],channel_label,sample_rate,start_datetime,end_datetime,segment_signals)
        _segments.append(segment)
    return _segments
