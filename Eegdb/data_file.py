import os
import pyedflib
import numpy as np
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from Eegdb.segment import Segment

MAX_SIGNAL_ARRAY_LENGTH = 180*200

class DataFile:
  def __init__(self,subjectid=None,filepath=None,file_type=None,sessionid=None,load_data=True):
    if not file_type:
      file_type = "unknown"
    self.__doc = { "subjectid": subjectid }
    if filepath:
      self.__doc["fileid"] = filepath.split("/")[-1]
    else:
      self.__doc["fileid"] = None
    if sessionid:
      self.__doc["sessionid"] = sessionid

    if file_type == "edf":
      if load_data:
        _edf_doc,self.__channel_list = self.load_edf(filepath)
        self.__doc.update(_edf_doc)
    elif file_type == "unknown":
      pass
    else:
      print(file_type,"is not supported")
      self.__channel_list = None
  

  def get_doc(self):
    return self.__doc.copy()

  def get_channel(self,channel_index):
    return self.__channel_list[channel_index].copy()
  
  def set_subjectid(self,new_subjectid):
    self.__doc["subjectid"] = new_subjectid
  
  def set_sessionid(self,new_sessionid):
    self.__doc["sessionid"] = new_sessionid

  def set_start_datetime(self,new_start_datetime):
    duration = self.__doc["duration"]
    self.__doc["start_datetime"] = new_start_datetime
    new_end_datetime = new_start_datetime+relativedelta(seconds = duration)
    self.__doc["end_datetime"] = new_end_datetime
  
  def load_mongo_doc(self,mongo_doc):
    self.__doc["subjectid"] =mongo_doc["subjectid"]
    self.__doc["fileid"] =mongo_doc["fileid"]
    self.__doc["sessionid"] =mongo_doc["sessionid"]
    self.__doc["n_channel"] =mongo_doc["n_channel"]
    self.__doc["channel_labels"] =mongo_doc["channel_labels"]
    self.__doc["start_datetime"] =mongo_doc["start_datetime"]
    self.__doc["duration"] =mongo_doc["duration"]
    self.__doc["end_datetime"] =mongo_doc["end_datetime"]

  def load_edf(self,filepath):
    _doc = {}
    _channel_list = []

    f = pyedflib.EdfReader(filepath)  # https://pyedflib.readthedocs.io/en/latest/#description

    N_channel = f.signals_in_file
    _doc["n_channel"] = N_channel
    # print("N_channel",N_channel)

    channel_labels = f.getSignalLabels()
    _doc["channel_labels"] = channel_labels
    # print("channel_labels",channel_labels)

    start_datetime = f.getStartdatetime()
    _doc["start_datetime"] = start_datetime
    # print("start_datetime",start_datetime)

    duration = f.getFileDuration()
    _doc["duration"] = duration
    # print("duration",duration)
    end_datetime = start_datetime+relativedelta(seconds = duration)
    _doc["end_datetime"] = end_datetime
    # print("end_datetime",end_datetime)

    data = f.readSignal

    for i in range(N_channel):
      channel_label = channel_labels[i]
      # sample_rate = f.getSampleFrequency(i) Bug: not correct for csr edf
      signals = data(i)
      sample_rate = len(signals)/duration
      # print("signal_label",signal_label,"sample_rate",sample_rate,"signals",signals[:5])
      _channel_doc = {
        "channel_index":i,
        "channel_label":channel_label,
        "sample_rate":sample_rate,
        "signals":signals
      }
      _channel_list.append(_channel_doc)
    
    header = f.getHeader()
    # print("header",header)
    return _doc,_channel_list

  def segmentation(self,max_segment_length=None):
    _segments = []
    for channel_doc in self.__channel_list:
      channel_index = channel_doc["channel_index"]
      channel_label = channel_doc["channel_label"]
      sample_rate = channel_doc["sample_rate"]
      if not max_segment_length:
        max_segment_length = math.ceil(MAX_SIGNAL_ARRAY_LENGTH/sample_rate)
      file_signals = channel_doc["signals"]
      n_data_point = len(file_signals)
      n_segment = math.ceil(self.__doc["duration"]/max_segment_length)
      for segment_index in range(n_segment):
        offset = segment_index*max_segment_length
        start_datetime = self.__doc["start_datetime"] + relativedelta(seconds = offset)
        end_datetime = start_datetime + relativedelta(seconds = max_segment_length)
        if end_datetime > self.__doc["end_datetime"]:
          end_datetime = self.__doc["end_datetime"]

        offset_data_point = int(offset*sample_rate)
        offset_data_point_end = offset_data_point + int(max_segment_length*sample_rate)
        if offset_data_point_end > n_data_point:
          offset_data_point_end = n_data_point
        segment_signals = list(file_signals[offset_data_point:offset_data_point_end])
        
        segment = Segment(self.__doc["subjectid"],self.__doc["fileid"],channel_index,channel_label,sample_rate,start_datetime,end_datetime,segment_signals)
        _segments.append(segment)
    return _segments

  def load_annotations(self,filepath):
    annotation_docs = []
    subjectid = self.__doc["subjectid"]
    fileid = self.__doc["fileid"]
    file_start_datetime = self.__doc["start_datetime"]

    with open(filepath,encoding='ascii') as f:
      lines = f.readlines()
      for line in lines:
        line = line.strip().strip('\x00')
        relative_time_str = line.split("\t")[0]
        annotation = line.split("\t")[1]

        relative_hour = int(relative_time_str.split(":")[0])
        relative_min = int(relative_time_str.split(":")[1])
        relative_sec = float(relative_time_str.split(":")[2])

        relative_time_in_seconds = relative_hour*3600 + relative_min*60 + relative_sec
        absolute_time = file_start_datetime + relativedelta(seconds=relative_time_in_seconds)

        annotation_doc = {"subjectid":subjectid, "fileid":fileid, "file_time":relative_time_in_seconds, "time":absolute_time, "annotation":annotation}
        annotation_docs.append(annotation_doc)
    return annotation_docs
