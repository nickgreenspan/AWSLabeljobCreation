import boto3
import json
import yaml
import cv2
import math
from datetime import datetime
import os
import zipfile
#import io

s3client = boto3.client('s3', region_name = 'us-east-1')
s3 = boto3.resource('s3', region_name = 'us-east-1')

def uploadInfo(input_data_bucket, lab_group_name, sequence_1, data_base, data_name, dataset_name, job_name, labels, shortintruct, fullinstruct):
        #uploads sequence file    
        s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + "/" + dataset_name + '/seq1.json'), Body=(bytes(json.dumps(sequence_1).encode('UTF-8'))))

        #creates manifest file
        manifest = {}
        manifest["source-ref"] = "s3://" + input_data_bucket + '/' + lab_group_name + '/inputs/' + job_name + "/" + dataset_name + "/seq1.json"

        s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + "/" + dataset_name + '.manifest.json'), Body=(bytes(json.dumps(manifest).encode('UTF-8'))))

        #create label config file
        label_config = {}
        label_config["document-version"]= datetime.today().strftime('%Y-%m-%d')
        label_config["annotationType"] = "Keypoint" 
        label_config["instructions"] = {"shortInstruction": shortintruct, "fullInstruction": fullinstruct}
        label_config["labels"] = labels

        s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + '/label_config.json'), Body=(bytes(json.dumps(label_config).encode('UTF-8'))))


def sorter(x):
    x = x.split("/")[-1]
    x = x.split(".")[0]
    num_idx = -1
    for i in range(len(x)):
        if ((x[i].isnumeric())):
            num_idx = i
            break
    if (num_idx == -1):
        return 0
    try:
        return int(x[num_idx:])
    except:
        return 0

def preprocess_frames_job(job_name, file_names, unzippedfolder, data_path, data_base, data_name, input_data_bucket, target_bucket, lab_group_name, labels, dataset_name, shortintruct, fullinstruct):
    frames = []
    f = 0
    sequence_1 = {}
    sequence_1["seq-no"] = 1
    sequence_1["prefix"] = ("s3://" + input_data_bucket + "/" + lab_group_name + '/inputs/' + job_name + "/" + dataset_name + "/")

    file_names_sorted = sorted(file_names, key = sorter)
    for name in file_names_sorted:
        name_split = name.split("/")
        name_start = name_split[0]
        if name_start != data_base:
            print("name_start != data_base")
            continue
        #folder_name = name_split[1]
        file_name = name_split[2]
        if len(file_name) == 0:
            continue
        extension = file_name.split(".")[1]
        if (extension != "png" and extension != "jpeg" and extension != "jpg"): #think about other extensions
            continue
        unzippedfolder.extract(name)
        s3client.upload_file(Bucket = input_data_bucket, Key = (lab_group_name + '/inputs/' + job_name + "/" + dataset_name + "/" + file_name), Filename = name)
        os.remove(name)
        frame_dict = {}
        frame_dict["frame-no"] = f + 1
        frame_dict["unix-timestamp"] = 2 #doesn't matter
        frame_dict["frame"] = file_name
        frames.append(frame_dict)
        f += 1
    sequence_1["frames"] = frames
    sequence_1["number-of-frames"] = f
    uploadInfo(input_data_bucket, lab_group_name, sequence_1, data_base, data_name, dataset_name, job_name, labels, shortintruct, fullinstruct)


#TODO: add fancy frame selection tools
def preprocess_video_job(job_name, video_name, unzippedfolder, data_path, data_base, data_name, input_data_bucket, target_bucket, lab_group_name, numframes, labels, shortintruct, fullinstruct):
    #s3.Bucket(input_data_bucket).download_file(data_path, (data_name + )
    #s3client.copy_object(Bucket = target_bucket, CopySource = {"Bucket" : input_data_bucket, "Key": data_path}, Key = job_name + "/data/videos/"+ data_name) #copies the original video to the output location
    unzippedfolder.extract(data_base + '/' + video_name)
    cap = cv2.VideoCapture(video_name)
    frameRate = cap.get(5)
    frames = []
    f = 0
    sequence_1 = {} #starting with only one sequence
    sequence_1["seq-no"] = 1
    sequence_1["prefix"] = ("s3://" + input_data_bucket + "/" + lab_group_name + '/inputs/' + job_name + "/" + video_name + "/")
    while(cap.isOpened()):
            if (f >= numframes):
                    print("exceeded")
                    break
            frameId = cap.get(1) #current frame number
            ret, frame = cap.read()
            if (ret != True):
                break
            hasFrame, imageBytes = cv2.imencode(".jpg", frame)
            if(hasFrame):
                    s3client.put_object(Bucket= input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + "/" + video_name + '/frame_' + str(f) + '.jpeg'), Body=imageBytes.tobytes())
                    frame_dict = {}
                    frame_dict["frame-no"] = f + 1
                    frame_dict["unix-timestamp"] = 2 #doesn't matter
                    frame_dict["frame"] = ("frame_" + str(f) + '.jpeg')
                    frames.append(frame_dict)
                    f += 1          
    cap.release()
    sequence_1["frames"] = frames
    sequence_1["number-of-frames"] = f + 1
    uploadInfo(input_data_bucket, lab_group_name, sequence_1, data_base, data_name, video_name, job_name, labels, shortintruct, fullinstruct)




