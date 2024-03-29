import sys
sys.path.insert(0, '..')

import numpy as np
import os
import math
from datetime import datetime
import pymongo
import csv

from Eegdb.data_file import DataFile
from Utils.timer import Timer

BATCH_SIZE = 5000

class Eegdb:
  def __init__(self,mongo_url,db_name,output_folder,data_folder=None):
    self.__mongo_client = pymongo.MongoClient(mongo_url)
    self.__db_name = db_name
    self.__database = self.__mongo_client[db_name]
    self.__output_folder = output_folder
    if output_folder and not os.path.exists(output_folder):
      os.makedirs(output_folder)
    self.__data_folder = data_folder
    if data_folder and not os.path.exists(self.__data_folder):
      print("Data folder",self.__data_folder,"does not exist.")
  
  def get_db_name(self):
    return self.__db_name

  def drop_collections(self,collection_name_list):
    for collection_name in collection_name_list:
      self.__database[collection_name].drop()
  
  def get_collection(self,collection_name):
    return self.__database[collection_name]

  def import_docs(self,doc_list,collection,batch_size=None):
    if not batch_size:
      batch_size = BATCH_SIZE

    num_docs = len(doc_list)
    num_batch = math.ceil(num_docs/batch_size)
    for i in range(num_batch):
      _ = self.__database[collection].insert_many(doc_list[i*batch_size:(i+1)*batch_size])
    print(num_docs,"docs imported to",collection,"with",num_batch,"batches")
  
  def import_data_file(self,data_file,max_segment_length=1):
    # import file
    print("import edf file info to database")
    file_doc = data_file.get_doc()
    file_collection = "files"
    self.import_docs([file_doc],file_collection)

    # import segments
    print("segmentation with max_segment_length =",max_segment_length)
    segment_docs = [x.get_doc() for x in data_file.segmentation(max_segment_length)]
    segments_collection = "segments"
    print("import segment data to database")
    self.import_docs(segment_docs,segments_collection)

  def import_csr_eeg_file(self,subjectid,sessionid,filepath,max_segment_length=None,annotation_filepath=None,check_existing=True,max_sample_rate=None):
    import_edf_flag = True
    import_annotation_flag = True
    vendor = "csr_uh"

    print("import",subjectid,sessionid,filepath,annotation_filepath)

    if filepath:
      if check_existing:
        fileid = filepath.split("/")[-1]
        existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          data_file = DataFile()
          data_file.load_mongo_doc(existing_file_doc)
          print(filepath,"exists, skip import edf.")
          import_edf_flag = False

      if import_edf_flag:
        # print("load edf file")
        file_type = "edf"
        try:
          data_file = DataFile(subjectid,filepath,file_type,sessionid,vendor=vendor)
        except:
          print("Read EDF error on", filepath)
          return -1

        # import file
        # print("import edf file info to database")
        file_doc = data_file.get_doc()
        if max_sample_rate:
          first_channel = data_file.get_channel(0)
          first_channel_sr = first_channel["sample_rate"]
          if first_channel_sr>max_sample_rate:
            print("sample_rate",first_channel_sr,"> max_sample_rate",max_sample_rate," import terminated.")
            return -3
        file_collection = "files"
        self.import_docs([file_doc],file_collection)

        # import segments
        # print("import segment data to database, segmentation with max_segment_length =",max_segment_length)
        segment_docs = [x.get_doc() for x in data_file.segmentation(max_segment_length=max_segment_length)]
        segments_collection = "segments"
        self.import_docs(segment_docs,segments_collection)
    elif annotation_filepath:
      fileid = annotation_filepath.split("/")[-1]
      fileid.replace(".txt",".edf")
      existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
      if existing_file_doc:
        data_file = DataFile()
        data_file.load_mongo_doc(existing_file_doc)
      else:
        return -2

    # import annotation
    if annotation_filepath:
      if check_existing:
        fileid = annotation_filepath.split("/")[-1].split(".")[0]+".edf"
        existing_file_doc = self.__database["annotations"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          print(filepath,"exists, skip import annotation.")
          import_annotation_flag = False

      if import_annotation_flag:
        annotation_docs = data_file.load_annotations(annotation_filepath)
        if annotation_filepath:
          annotation_collection = "annotations"
          # print("import annotation data to database")
          self.import_docs(annotation_docs,annotation_collection)

  def import_csr_eeg_file_v2(self,subjectid,sessionid,filepath,segment_duration=None,annotation_filepath=None,check_existing=True,max_sample_rate=None):
    import_edf_flag = True
    import_annotation_flag = True
    vendor = "csr_uh"
    sample_rate_set = set([])
    print("import",subjectid,sessionid,filepath,annotation_filepath)
    import_log = {}

    if filepath:
      if check_existing:
        fileid = filepath.split("/")[-1]
        existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          data_file = DataFile()
          data_file.load_mongo_doc(existing_file_doc)
          print(filepath,"exists, skip import edf.")
          import_edf_flag = False

      if import_edf_flag:
        edf_timer = Timer()
        # print("load edf file")
        file_type = "edf"
        try:
          data_file = DataFile(subjectid,filepath,file_type,sessionid,vendor=vendor)
        except:
          print("Read EDF error on", filepath)
          return -1,None

        # import file
        # print("import edf file info to database")
        file_doc = data_file.get_doc()
        sample_rate_set.update(set(file_doc["sample_rates"]))
        if max_sample_rate:
          first_channel = data_file.get_channel(0)
          first_channel_sr = first_channel["sample_rate"]
          if first_channel_sr>max_sample_rate:
            print("sample_rate",first_channel_sr,"> max_sample_rate",max_sample_rate," import terminated.")
            return -3,None
        file_collection = "files"
        self.import_docs([file_doc],file_collection)

        # import segments
        # print("import segment data to database, segmentation with max_segment_length =",max_segment_length)
        segment_docs = [x.generate_mongo_doc() for x in data_file.segmentation_by_time(segment_duration=segment_duration)]
        import_log['edf_file_process'] = edf_timer.stop()
        segments_collection = "segments"
        seg_timer = Timer()
        self.import_docs(segment_docs,segments_collection)
        import_log['segment_import'] = seg_timer.stop()
    elif annotation_filepath:
      fileid = annotation_filepath.split("/")[-1]
      fileid.replace(".txt",".edf")
      existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
      if existing_file_doc:
        data_file = DataFile()
        data_file.load_mongo_doc(existing_file_doc)
      else:
        return -2,None

    # import annotation
    if annotation_filepath:
      if check_existing:
        fileid = annotation_filepath.split("/")[-1].split(".")[0]+".edf"
        existing_file_doc = self.__database["annotations"].find_one({"subjectid":subjectid,"fileid":fileid})
        if existing_file_doc:
          print(filepath,"exists, skip import annotation.")
          import_annotation_flag = False

      if import_annotation_flag:
        annotation_docs = data_file.load_annotations(annotation_filepath)
        if annotation_filepath:
          annotation_collection = "annotations"
          # print("import annotation data to database")
          self.import_docs(annotation_docs,annotation_collection)
    return 1,sample_rate_set,import_log

  def import_csr_edf_plus(self,csr_site_name,subjectid,sessionid,filepath,segment_duration=None,check_existing=True,max_sample_rate=None,import_edf_flag = True,import_annotation_flag = True):
    sample_rate_set = set([])
    print("import",csr_site_name,subjectid,sessionid,filepath)

    if check_existing:
      fileid = filepath.split("/")[-1]
      existing_file_doc = self.__database["files"].find_one({"subjectid":subjectid,"fileid":fileid})
      if existing_file_doc:
        data_file = DataFile()
        data_file.load_mongo_doc(existing_file_doc)
        print(filepath,"exists, skip import edf.")
        import_edf_flag = False

    if import_edf_flag:
      file_type = "edf+"
      try:
        data_file = DataFile(subjectid,filepath,file_type,sessionid,vendor=csr_site_name)
      except Exception as e:
        print(e)
        print("Read EDF error on", filepath)
        return -1,None

      # import file
      # print("import edf file info to database")
      # file_doc = data_file.get_doc()
      # sample_rate_set.update(set(file_doc["sample_rates"]))
      # if max_sample_rate:
      #   first_channel = data_file.get_channel(0)
      #   first_channel_sr = first_channel["sample_rate"]
      #   if first_channel_sr>max_sample_rate:
      #     print("sample_rate",first_channel_sr,"> max_sample_rate",max_sample_rate," import terminated.")
      #     return -3,None
      # file_collection = "files"
      # self.import_docs([file_doc],file_collection)

      # # import segments
      # # print("import segment data to database, segmentation with max_segment_length =",max_segment_length)
      # segment_docs = [x.generate_mongo_doc() for x in data_file.segmentation_by_time(segment_duration=segment_duration)]
      # segments_collection = "segments"
      # self.import_docs(segment_docs,segments_collection)

    return 1,sample_rate_set

  
  def import_samsung_wearable_data(self,vendor,file_type,subjectid,subject_filepath_list):
    subject_data_list = []
    count = 0
    n = len(subject_filepath_list)
    finished_perc_list = []
    for filepath in subject_filepath_list:
      # count += 1
      # finished_perc = int(count*100/n)
      # if finished_perc in [ 10,20,30,40,50,60,70,80,90 ] and finished_perc not in finished_perc_list:
      #   finished_perc_list.append(finished_perc)
      #   print(str(finished_perc)+"% " + "finished.")

      data_file = DataFile(subjectid=subjectid,filepath=filepath,file_type=file_type,sessionid=None,vendor=vendor)
      subject_data_list.append(data_file)
    file_collection = "files"
    self.import_docs([ x.get_doc() for x in subject_data_list], file_collection)

    subject_data_list = sorted(subject_data_list,key = lambda x:x.get_doc()["start_datetime"])
    merged_subject_data = subject_data_list[0]
    if len(subject_data_list)>1:
      merged_subject_data.signals_concatenate(subject_data_list[1:])
      segments = merged_subject_data.segmentation_by_data_points()
      segment_docs = [ x.get_doc() for x in segments ]
      segments_collection = "segments"
      self.import_docs(segment_docs,segments_collection)
      # start_datetime = segments[0].get_doc()["start_datetime"]
      # end_datetime = segments[-1].get_doc()["end_datetime"]
      # print(start_datetime,end_datetime,(end_datetime-start_datetime).total_seconds())
      # print("len(segments)",len(segments))
  
  def import_samsung_wearable_annotation(self,annotation_folder):
    vendor = "samsung_wearable"
    _file_list = []
    _annotation_docs = []
    # scan csv files in the folder
    for filename in os.listdir(annotation_folder):
      if os.path.isfile(os.path.join(annotation_folder, filename)):
        if filename.split(".")[-1] == "csv":
          _file_list.append( annotation_folder + filename)
    for file_path in _file_list:
      fileid = file_path.split("/")[-1]
      _annotation_label = fileid.split("-")[0]
      if _annotation_label not in ["checkin","seizure","weather","EMUseizure"]:
        continue
      print("load",file_path)
      with open(file_path,encoding='utf-8-sig') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
          subjectid = row["AWSID"]
          _tmp_doc = {"subjectid":subjectid,"fileid":fileid,"vendor":vendor,"annotation_label":_annotation_label}
          # start time column
          if _annotation_label == "weather":
            start_datetime = datetime.strptime(row["date"], '%m/%d/%Y')
          elif _annotation_label == "EMUseizure":
            start_datetime = datetime.strptime(row["Seizure_beginning"], '%m/%d/%y %H:%M')
          else:
            start_datetime = datetime.strptime(row["timestamp"], '%d-%b-%Y %H:%M:%S')
          _tmp_doc["start_datetime"] = start_datetime
          if _annotation_label == "EMUseizure":
            end_datetime = datetime.strptime(row["Seizure_termination"], '%m/%d/%y %H:%M')
            _tmp_doc["end_datetime"] = end_datetime
          for column in row.keys():
            if column in [ "AWSID", "timestamp", "date","Seizure_beginning","Seizure_termination"]:
              continue
            _tmp_doc[column] = float(row[column]) if _annotation_label == "weather" else row[column]
          _annotation_docs.append(_tmp_doc)
    _annotation_collection = "annotations"
    # print("import_samsung_wearable_annotation")
    self.drop_collections([_annotation_collection])
    self.import_docs(_annotation_docs,_annotation_collection)
          



  def build_index(self):
    print("build_index: files")
    self.__database["files"].create_index([("subjectid","hashed")])
    self.__database["files"].create_index([("start_datetime",1),("end_datetime",1)])
    print("build_index: segments")
    # self.__database["segments"].create_index([("subjectid",1),("channel_labels",1),("start_datetime",1),("end_datetime",1)])
    self.__database["segments"].create_index([("subjectid",1),("channel_labels",1),("segment_datetime",1)])


  def data_export(self,subjectid_list,channel_list,query_start_datetime=None,query_end_datetime=None):
    if not query_start_datetime:
      query_start_datetime = datetime(1900,1,1)
    if not query_end_datetime:
      query_end_datetime = datetime(2099,12,31)
    
    query_stmt = {"subjectid":{"$in":subjectid_list}, "channel_label":{"$in":channel_list}, "start_datetime":{"$lt":query_end_datetime}, "end_datetime":{"$gt":query_start_datetime}}
    segment_docs = self.__database["segments"].find(query_stmt)
    export_data = self.generate_export_data(segment_docs)
    # clip to match query time window
    for key,signals_doc_list in export_data.items():
      key_in_list = key.split("|")
      sample_rate = float(key_in_list[-1])
      signals_doc_list[0] = self.clip_signals(signals_doc_list[0],sample_rate,query_start_datetime,query_end_datetime)
      if len(signals_doc_list)>1:
        signals_doc_list[-1] = self.clip_signals(signals_doc_list[-1],sample_rate,query_start_datetime,query_end_datetime)
    return export_data


  def generate_export_data(self,segment_docs):
    export_data_by_file = {}
    for segment_doc in segment_docs:
      seg_key_data = [ segment_doc["subjectid"],segment_doc["fileid"], str(segment_doc["channel_index"]), segment_doc["channel_label"], str(segment_doc["sample_rate"])]
      seg_key = "|".join(seg_key_data)
      segment_signals_doc = {"start_datetime":segment_doc["start_datetime"],"end_datetime":segment_doc["end_datetime"],"signals":segment_doc["signals"]}
      try:
        export_data_by_file[seg_key].append(segment_signals_doc)
      except:
        export_data_by_file[seg_key] = [segment_signals_doc]
    export_data_by_channel = {}
    for seg_key, segment_signals_docs in export_data_by_file.items():
      seg_key_in_list = seg_key.split("|")
      sample_rate = float(seg_key_in_list[-1])
      seg_key_in_list.pop(1)
      new_seg_key = "|".join(seg_key_in_list)
      merged_signals = self.merge_segment_signals(segment_signals_docs,sample_rate)
      try:
        export_data_by_channel[new_seg_key] += merged_signals
      except:
        export_data_by_channel[new_seg_key] = merged_signals
    for seg_key, segment_signals_docs in export_data_by_channel.items():
      seg_key_in_list = seg_key.split("|")
      sample_rate = float(seg_key_in_list[-1])
      export_data_by_channel[seg_key] = self.merge_segment_signals(segment_signals_docs,sample_rate)
    return export_data_by_channel

  def merge_segment_signals(self,segment_signals_docs,sample_rate):
    section_list = []
    segment_signals_docs = sorted(segment_signals_docs,key=lambda x:x["start_datetime"])
    section = None
    for segment_signals_doc in segment_signals_docs:
      segment_start_datetime = segment_signals_doc["start_datetime"]
      segment_end_datetime = segment_signals_doc["end_datetime"]
      segment_signals = segment_signals_doc["signals"]
      if not section:
        section = {"start_datetime":segment_start_datetime,"end_datetime":segment_end_datetime,"signals":segment_signals}
        continue

      if segment_start_datetime == section["end_datetime"]:
        section["end_datetime"] = segment_end_datetime
        section["signals"] += segment_signals
      elif segment_start_datetime < section["end_datetime"]:
        if segment_end_datetime > section["end_datetime"]:
          offset_in_seconds = (section["end_datetime"]-segment_start_datetime).seconds
          offset_in_data_points = int(offset_in_seconds*sample_rate)
          new_segment_signals = segment_signals[offset_in_data_points:]
          section["end_datetime"] = segment_end_datetime
          section["signals"] += new_segment_signals
      else:
        section_list.append(section.copy())
        section = {"start_datetime":segment_start_datetime,"end_datetime":segment_end_datetime,"signals":segment_signals}
    if section:
      section_list.append(section.copy())
    return section_list
  
  def clip_signals(self,signals_doc,sample_rate,clip_start_datetime,clip_end_datetime):
    if clip_start_datetime and clip_end_datetime and clip_start_datetime>clip_end_datetime:
      print("Error: clip_start_datetime should be ealier than clip_end_datetime")
      return -1
    signals_start_datetime = signals_doc["start_datetime"]
    signals_end_datetime = signals_doc["end_datetime"]
    signals = signals_doc["signals"]

    if clip_start_datetime>signals_start_datetime and clip_start_datetime<=signals_end_datetime:
      offset_in_seconds = (clip_start_datetime-signals_start_datetime).seconds
      start_offset_in_data_points = int(offset_in_seconds*sample_rate)
      new_signals_start_datetime = clip_start_datetime
    else:
      start_offset_in_data_points = 0
      new_signals_start_datetime = signals_start_datetime

    if clip_end_datetime>signals_start_datetime and clip_end_datetime<signals_end_datetime:
      offset_in_seconds = (clip_end_datetime-signals_start_datetime).seconds
      end_offset_in_data_points = int(offset_in_seconds*sample_rate)
      new_signals_end_datetime = clip_end_datetime
    else:
      end_offset_in_data_points = len(signals)
      new_signals_end_datetime = signals_end_datetime

    
    new_segment_signals = signals[start_offset_in_data_points:end_offset_in_data_points]
    new_signals_doc = {"start_datetime":new_signals_start_datetime,"end_datetime":new_signals_end_datetime,"signals":new_segment_signals}
    return new_signals_doc


