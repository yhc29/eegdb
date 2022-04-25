import os
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class Segment:
  def __init__(self,subjectid,fileid,vendor,file_type,channel_index,channel_label,sample_rate,start_datetime,end_datetime,signals,time_points=None,segment_datetime=None):
    self.__doc = {
      "subjectid": subjectid,
      "fileid": fileid,
      "vendor": vendor,
      "file_type": file_type,
      "channel_index":channel_index,
      "channel_label":channel_label,
      "sample_rate":sample_rate,
      "start_datetime":start_datetime,
      "end_datetime":end_datetime,
      "time_points": time_points,
      "signals":signals,
      "segment_datetime":segment_datetime
    }
  
  def get_doc(self):
    return self.__doc
  
  def generate_mongo_doc(self,segment_duration):
    _mongo_doc = self.__doc
    _signals_array = _mongo_doc["signals"]
    n_data_point = len(_signals_array)
    _start_datetime = _mongo_doc["start_datetime"]
    _end_datetime = _mongo_doc["end_datetime"]
    _sample_rate = _mongo_doc["sample_rate"]
    _clip_datetime = _start_datetime
    _signals_dict = {}
    while _clip_datetime <= _end_datetime:
      _minute = _clip_datetime.minute
      _second = _clip_datetime.second
      offset = (_clip_datetime - _start_datetime).total_seconds()
      offset_data_point = int(offset*_sample_rate)
      offset_data_point_end = offset_data_point + int(_sample_rate)
      if offset_data_point_end > n_data_point:
        offset_data_point_end = n_data_point
      _clip_signals = list(_signals_array[offset_data_point:offset_data_point_end])
      try:
        _signals_dict[_minute]
      except:
        _signals_dict[_minute] = {}
      _signals_dict[_minute][_second]={"values":_clip_signals}

      _clip_datetime = _clip_datetime + relativedelta(seconds = 1)
    _mongo_doc["signals"] = _signals_dict

    return _mongo_doc