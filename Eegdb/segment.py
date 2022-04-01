import os
import numpy as np
from datetime import datetime, timedelta

class Segment:
  def __init__(self,subjectid,fileid,vendor,file_type,channel_index,channel_label,sample_rate,start_datetime,end_datetime,signals,time_points=None):
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
      "signals":signals
    }
  
  def get_doc(self):
    return self.__doc