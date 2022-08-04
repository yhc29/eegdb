import sys
sys.path.insert(0, '..')

import os
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing.pool import ThreadPool
import csv
import math

from Utils.timer import Timer

from Eegdb.data_file import DataFile
from Eegdb.eegdb import Eegdb

# import config.db_config_ibm as config_file
import config.db_config_sbmi as config_file

def annotation_file_import(eegdb,annotation_file_path):
  print("load",annotation_file_path)
  date_format = "%Y-%m-%d %H:%M:%S"
  annotation_record_doc_list = []
  annotation_tii_dict ={}
  annotation_id_tii_dict ={}
  annotation_id_partial_tii_dict ={}
  with open(annotation_file_path,encoding='utf-8-sig') as f:
    csv_reader = csv.DictReader(f)
    row_count = 0
    for row in csv_reader:
      row_count += 1 
      subjectid = row["patient_id"]
      fileid = row["edf_file_id"]+".edf"
      file_time = float(row["relative_time"])
      time_str = row["datetime"]
      time_str = time_str.split(" ")[0] + " " + time_str.split(" ")[1]
      time = datetime.strptime(time_str,date_format)
      # if row_count<10:
      #   print(time_str,time)
      annotation = row["annotation"]
      try:
        annotation_tii_dict[annotation].add(subjectid)
      except:
        annotation_tii_dict[annotation] = {subjectid}
      annotation_record_doc = {"subjectid":subjectid, "fileid":fileid,"file_time":file_time, "time":time, "annotation":annotation}
      annotationid = int(row["annotation_id"])
      if annotationid!= -1:
        annotation_record_doc["annotationid"] = annotationid
        try:
          annotation_id_tii_dict[annotationid].add(subjectid)
        except:
          annotation_id_tii_dict[annotationid] = {subjectid}
      annotationid_partial = int(row["annotation_id_partial"])
      if annotationid_partial!= -1:
        annotation_record_doc["annotationid_partial"] = annotationid_partial
        try:
          annotation_id_partial_tii_dict[annotationid_partial].add(subjectid)
        except:
          annotation_id_partial_tii_dict[annotationid_partial] = {subjectid}
      annotation_record_doc_list.append(annotation_record_doc)
    print(row_count,"annotation records loaded.")

  collection_name = "annotation_records"
  eegdb.drop_collections([collection_name])
  eegdb.import_docs(annotation_record_doc_list,collection_name,batch_size=10000)

  collection_name = "annotation_tii"
  annotation_tii_doc_list = []
  for annotation,subjectid_set in annotation_tii_dict.items():
    annotation_tii_doc = {"annotation":annotation, "subjectid_list":list(subjectid_set)}
    annotation_tii_doc_list.append(annotation_tii_doc)
  for annotationid,subjectid_set in annotation_id_tii_dict.items():
    annotation_tii_doc = {"annotationid":annotationid, "subjectid_list":list(subjectid_set)}
    annotation_tii_doc_list.append(annotation_tii_doc)
  for annotationid_partial,subjectid_set in annotation_id_partial_tii_dict.items():
    annotation_tii_doc = {"annotationid_partial":annotationid_partial, "subjectid_list":list(subjectid_set)}
    annotation_tii_doc_list.append(annotation_tii_doc)
  eegdb.drop_collections([collection_name])
  eegdb.import_docs(annotation_tii_doc_list,collection_name,batch_size=10000)


def edf_plus_import(eegdb,data_folder):
  data_file_dict = {}
  # for sessionid in os.listdir(data_folder):
  #   session_folder = os.path.join(data_folder, sessionid)
  #   if os.path.isdir(session_folder):
  #     subjectid = sessionid[:-2]
  
  csr_site_name = "UH"
  subjectid = "WKQB28950888871511"
  sessionid = "WKQB2895088887151101"
  filepath = "/Users/yhuang22/Documents/Data/CSR_EEG/csr/UH/EEG/WKQB2895088887151101/DA1167CT_1-1+.edf"
  _code,_tmp_sr_set = eegdb.import_csr_edf_plus(csr_site_name,subjectid,sessionid,filepath,segment_duration=None,check_existing=True,max_sample_rate=299)


def annotation_ralation_pt_timeline_import(eegdb):
  max_n_relation_in_doc = 100000
  collection_name = "annotation_relation_pt_timeline_v1"
  eegdb.drop_collections([collection_name])
  subjectid_list = eegdb.get_collection("annotation_records").distinct("subjectid")
  subjectid_count = 0
  for subjectid in subjectid_list:
    doc_list = []
    subjectid_count+=1
    print(subjectid_count,subjectid)
    docs = eegdb.get_collection("annotation_records").find({"subjectid":subjectid,"annotationid_partial":{"$exists":True}})
    pt_annotation_dict = {}
    for doc in docs:
      time = doc["time"]
      annotationid = doc["annotationid_partial"]
      try:
        pt_annotation_dict[annotationid].add(time)
      except:
        pt_annotation_dict[annotationid] = set([time])
    pt_timeline = []
    for annotationid,time_set in pt_annotation_dict.items():
      for time in time_set:
        pt_timeline.append((annotationid,time))
    pt_timeline = sorted(pt_timeline,key=lambda x:x[1])
    print(len(pt_timeline), "annotation records found")
    pt_annotation_relation_dict = {}
    
    for i in range(len(pt_timeline)-1):
      annotation1 = pt_timeline[i]
      for j in range(i+1, len(pt_timeline)):
        annotation2 = pt_timeline[j]
        time_diff = (annotation2[1]-annotation1[1]).total_seconds()
        try:
          pt_annotation_relation_dict[(annotation1[0],annotation2[0])].append((time_diff,annotation1[1],annotation2[1]))
        except:
          pt_annotation_relation_dict[(annotation1[0],annotation2[0])] = [(time_diff,annotation1[1],annotation2[1])]
        if time_diff == 0:
          try:
            pt_annotation_relation_dict[(annotation2[0],annotation1[0])].append((time_diff,annotation2[1],annotation1[1]))
          except:
            pt_annotation_relation_dict[(annotation2[0],annotation1[0])] = [(time_diff,annotation2[1],annotation1[1])]
    for relation, time_diff_list in pt_annotation_relation_dict.items():
      annotationid1 = relation[0]
      annotationid2 = relation[1]
      time_diff_list = sorted(time_diff_list,key=lambda x:x[0])
      # v_2
      # if len(time_diff_list)<=max_n_relation_in_doc:
      #   time_diff = [ x[0] for x in time_diff_list]
      #   time1 = [ x[1] for x in time_diff_list]
      #   time2 = [ x[2] for x in time_diff_list]
      #   import_doc = {"subjectid":subjectid, "annotationid1":annotationid1,"annotationid2":annotationid2,"time_diff":time_diff,"time1":time1,"time2":time2}
      #   doc_list.append(import_doc)
      # else:
      #   split_count = math.ceil(len(time_diff_list)/max_n_relation_in_doc)
      #   for s in range(split_count):
      #     time_diff = [ x[0] for x in time_diff_list[s*max_n_relation_in_doc:(s+1)*max_n_relation_in_doc]]
      #     time1 = [ x[1] for x in time_diff_list[s*max_n_relation_in_doc:(s+1)*max_n_relation_in_doc]]
      #     time2 = [ x[2] for x in time_diff_list[s*max_n_relation_in_doc:(s+1)*max_n_relation_in_doc]]
      #     import_doc = {"subjectid":subjectid, "annotationid1":annotationid1,"annotationid2":annotationid2,"time_diff":time_diff,"time1":time1,"time2":time2}
      #     doc_list.append(import_doc)

      # v_1
      for time_diff,time1,time2 in time_diff_list:
        import_doc = {"subjectid":subjectid, "annotationid1":annotationid1,"annotationid2":annotationid2,"time_diff":time_diff,"time1":time1,"time2":time2}
        doc_list.append(import_doc)

    print(len(doc_list), "docs generated")
    eegdb.import_docs(doc_list,collection_name,batch_size=10000)


if __name__ == '__main__':
  my_timer = Timer()

  eegdb = Eegdb(config_file.mongo_url,config_file.eegdb_name,config_file.output_folder,config_file.data_folder)
  # edf_plus_import(eegdb,config_file.data_folder)

  # annotation_file_path = config_file.data_folder+"eeg_annotation_records_edf+cd.csv"
  # annotation_file_import(eegdb,annotation_file_path)

  annotation_ralation_pt_timeline_import(eegdb)




  print(my_timer.stop())