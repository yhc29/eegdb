import os
import pyedflib
import numpy as np
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import gzip
import json

from Eegdb.segment import Segment

MAX_SIGNAL_ARRAY_LENGTH = 180*200

class DataFile:
  def __init__(self,subjectid=None,filepath=None,file_type=None,sessionid=None,vendor=None,load_data=True):
    if not vendor:
      vendor = "unknown"
    if not file_type:
      file_type = "unknown"
    self.__doc = { "subjectid": subjectid }
    if filepath:
      self.__doc["fileid"] = filepath.split("/")[-1]
    else:
      self.__doc["fileid"] = None
    if sessionid:
      self.__doc["sessionid"] = sessionid
    if vendor:
      self.__doc["vendor"] = vendor
    if file_type:
      self.__doc["file_type"] = file_type

    if file_type == "edf":
      if load_data:
        _file_info_doc,self.__channel_list = self.load_edf(filepath)
        self.__doc.update(_file_info_doc)
    elif vendor == "samsung_wearable" and file_type in ["bppg","hribi","gyro"]:
      if load_data:
        _file_info_doc,self.__channel_list = self.load_samsung_wearable_data(filepath,file_type)
        self.__doc.update(_file_info_doc)
    elif file_type == "unknown":
      pass
    else:
      print(vendor,file_type,"is not supported")
      self.__channel_list = None
  

  def get_doc(self):
    return self.__doc.copy()

  def get_channels(self):
    return self.__channel_list.copy()
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
  
  def set_end_datetime(self,new_end_datetime):
    self.__doc["end_datetime"] = new_end_datetime
    self.__doc["duration"] = (self.__doc["start_datetime"] - new_end_datetime).total_seconds()
  
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

    _doc["sample_rates"] = []
    for i in range(N_channel):
      channel_label = channel_labels[i]
      # sample_rate = f.getSampleFrequency(i) Bug: not correct for csr edf
      signals = data(i)
      sample_rate = int(len(signals)/duration+0.5)
      _doc["sample_rates"].append(sample_rate)
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

  def load_samsung_wearable_data(self, filepath,file_type):
    _doc = {}
    _channel_list = []
    default_sample_rate_dict = {"bppg":25, "gyro":25, "hribi":1}
    default_channels_dict = {
      "bppg":[ 'Green', 'AccX', 'AccY', 'AccZ' ],
      "gyro":['X', 'Y', 'Z'],
      "hribi":[ 'Bpm', 'Rri' ]}

    # df = pd.read_json(fn, compression='gzip')
    with gzip.open(filepath, 'r') as f:
      json_bytes = f.read()
    json_str = json_bytes.decode('utf-8')
    data = json.loads(json_str)

    channel_labels = default_channels_dict[file_type]
    _doc["channel_labels"] = channel_labels

    N_channel = len(channel_labels)
    _doc["n_channel"] = N_channel

    # n_data_point = len(data)
    # print("n_data_point:",n_data_point)

    start_datetime = datetime.fromtimestamp(data[0]["Timestamp"]/1000.0)
    _doc["start_datetime"] = start_datetime
    end_datetime = datetime.fromtimestamp(data[-1]["Timestamp"]/1000.0)
    _doc["end_datetime"] = end_datetime
    duration = (end_datetime-start_datetime).total_seconds()
    _doc["duration"] = duration

    sample_rate = default_sample_rate_dict[file_type]
    for i, channel_label in enumerate(channel_labels):
      _channel_doc = {
        "channel_index":i,
        "channel_label":channel_label,
        "sample_rate": sample_rate,
        "time_points": [ datetime.fromtimestamp(record["Timestamp"]/1000.0) for record in data ],
        "signals":[ record[channel_label] for record in data ]
      }
      _channel_list.append(_channel_doc)

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
      n_segment = math.ceil(n_data_point/MAX_SIGNAL_ARRAY_LENGTH)
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
        
        segment = Segment(self.__doc["subjectid"],self.__doc["fileid"],self.__doc["vendor"],self.__doc["file_type"],channel_index,channel_label,sample_rate,start_datetime,end_datetime,segment_signals)
        _segments.append(segment)
    return _segments

  def segmentation_by_time(self,segment_duration=None):
    _segments = []
    _file_start_datetime = self.__doc["start_datetime"]
    _file_end_datetime = self.__doc["end_datetime"]
    _segment_duration_set = set([])
    for channel_doc in self.__channel_list:
      channel_index = channel_doc["channel_index"]
      channel_label = channel_doc["channel_label"]
      sample_rate = channel_doc["sample_rate"]
      if not segment_duration:
        if sample_rate<300:
          segment_duration=60
        elif sample_rate<700:
          segment_duration=30
        elif sample_rate<1000:
          segment_duration=20
        elif sample_rate<1500:
          segment_duration=10
      _segment_duration_set.add(segment_duration)
      file_signals = channel_doc["signals"]
      n_data_point = len(file_signals)
      segment_count = 0
      _segment_start_datetime = get_fixed_segment_start_datetime(_file_start_datetime,segment_duration)
      while _segment_start_datetime <= _file_end_datetime:
        segment_count +=1
        if segment_count == 1:
          offset = 0
        else:
          offset = (_segment_start_datetime - _file_start_datetime).total_seconds()
        start_datetime = self.__doc["start_datetime"] + relativedelta(seconds = offset)
        end_datetime = _segment_start_datetime + relativedelta(seconds = segment_duration*60)
        if end_datetime > self.__doc["end_datetime"]:
          end_datetime = self.__doc["end_datetime"]

        offset_data_point = int(offset*sample_rate)
        offset_data_point_end = offset_data_point + int(segment_duration*60*sample_rate)
        if offset_data_point_end > n_data_point:
          offset_data_point_end = n_data_point
        segment_signals = list(file_signals[offset_data_point:offset_data_point_end])
        
        segment = Segment(self.__doc["subjectid"],self.__doc["fileid"],self.__doc["vendor"],self.__doc["file_type"],channel_index,channel_label,sample_rate,start_datetime,end_datetime,segment_signals,None,_segment_start_datetime,segment_duration)
        _segments.append(segment)
        _segment_start_datetime = _segment_start_datetime + relativedelta(seconds = segment_duration*60)
    print(_segment_duration_set)
    return _segments
  
  def segmentation_by_data_points(self,max_signal_array_length=None):
    _segments = []
    for channel_doc in self.__channel_list:
      channel_index = channel_doc["channel_index"]
      channel_label = channel_doc["channel_label"]
      sample_rate = channel_doc["sample_rate"]
      time_points = channel_doc["time_points"]
      if not max_signal_array_length:
        max_signal_array_length = MAX_SIGNAL_ARRAY_LENGTH
      channel_signals = channel_doc["signals"]
      n_data_point = len(channel_signals)
      n_segment = math.ceil(n_data_point/max_signal_array_length)
      for segment_index in range(n_segment):
        offset_data_point = segment_index*max_signal_array_length
        offset_data_point_end = offset_data_point + max_signal_array_length
        if offset_data_point_end > n_data_point:
          offset_data_point_end = n_data_point
        segment_time_points = list(time_points[offset_data_point:offset_data_point_end])
        segment_signals = list(channel_signals[offset_data_point:offset_data_point_end])
        segment = Segment(self.__doc["subjectid"],self.__doc["fileid"],self.__doc["vendor"],self.__doc["file_type"],channel_index,channel_label,sample_rate,segment_time_points[0],segment_time_points[-1],segment_signals,segment_time_points)
        _segments.append(segment)
    return _segments
  
  def signals_concatenate(self,sorted_data_file_list):
    new_channel_dict = {}
    
    for channel_doc in self.__channel_list:
      channel_index = channel_doc["channel_index"]
      channel_label = channel_doc["channel_label"]
      sample_rate = channel_doc["sample_rate"]
      signals = channel_doc["signals"]
      try:
        time_points = channel_doc["time_points"]
      except:
        print("No time_points for channel data.")
        return -1
      channel_key = (channel_index,channel_label,sample_rate)
      new_channel_dict[channel_key] = [ time_points, signals ]
    
    count = 0
    n = len(sorted_data_file_list)
    finished_perc_list = []
    for new_data_file in sorted_data_file_list:
      count += 1
      finished_perc = int(count*100/n)
      if finished_perc in [ 10,20,30,40,50,60,70,80,90 ] and finished_perc not in finished_perc_list:
        finished_perc_list.append(finished_perc)
        print(str(finished_perc)+"% " + "finished.")

      new_channel_list = new_data_file.get_channels()
      for channel_doc in new_channel_list:
        channel_index = channel_doc["channel_index"]
        channel_label = channel_doc["channel_label"]
        sample_rate = channel_doc["sample_rate"]
        signals = channel_doc["signals"]
        try:
          time_points = channel_doc["time_points"]
        except:
          continue
        channel_key = (channel_index,channel_label,sample_rate)
        try:
          new_channel_dict[channel_key][0] += time_points
          new_channel_dict[channel_key][1] += signals
        except:
          print("No matched channel exists.")
    _new_channel_list = []
    for channel_key,channel_data in new_channel_dict.items():
      _channel_doc = {
        "channel_index": channel_key[0],
        "channel_label":channel_key[1],
        "sample_rate": channel_key[2],
        "time_points": channel_data[0],
        "signals": channel_data[1]
      }
      _new_channel_list.append(_channel_doc)
    self.__channel_list = _new_channel_list
    self.set_end_datetime(_new_channel_list[0]["time_points"][-1])

  def load_annotations(self,filepath):
    annotation_docs = []
    subjectid = self.__doc["subjectid"]
    fileid = self.__doc["fileid"]
    file_start_datetime = self.__doc["start_datetime"]

    with open(filepath,encoding='utf-8',errors='ignore') as f:
      lines = f.readlines()
      for line in lines:
        line = line.strip().strip('\x00')
        relative_time_str = line.split("\t")[0]
        try:
          annotation = line.split("\t")[1]
        except:
          print("annotation record read error.")
          continue

        relative_hour = int(relative_time_str.split(":")[0])
        relative_min = int(relative_time_str.split(":")[1])
        relative_sec = float(relative_time_str.split(":")[2])

        relative_time_in_seconds = relative_hour*3600 + relative_min*60 + relative_sec
        absolute_time = file_start_datetime + relativedelta(seconds=relative_time_in_seconds)

        annotation_doc = {"subjectid":subjectid, "fileid":fileid, "file_time":relative_time_in_seconds, "time":absolute_time, "annotation":annotation}
        annotation_docs.append(annotation_doc)
    return annotation_docs








def get_fixed_segment_start_datetime(segment_start_datetime,segment_duration):
  _start_hour, _start_minute = segment_start_datetime.hour, segment_start_datetime.minute
  _total_minutes = (_start_hour*60 + _start_minute)//segment_duration*segment_duration
  _new_hour = _total_minutes//60
  _new_minute = _total_minutes%60
  _new_datetime = segment_start_datetime.replace(hour=_new_hour,minute=_new_minute,second=0)
  return _new_datetime
