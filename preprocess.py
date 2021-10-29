import boto3
import json
import yaml
import cv2
import math
from datetime import datetime
import os
import zipfile
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans


s3client = boto3.client('s3', region_name = 'us-east-1')
s3 = boto3.resource('s3', region_name = 'us-east-1')

RANDOM_SEED = 10
PCA_DIM = 175

def uploadInfo(input_data_bucket, lab_group_name, sequence_1, data_base, data_name, dataset_name, job_name, labels, shortintruct, fullinstruct):
    #uploads sequence file   
    print(input_data_bucket) 
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

def preprocess_frames_job(job_name, file_names, unzippedfolder, data_path, data_base, data_name, input_data_bucket, lab_group_name, labels, dataset_name, shortintruct, fullinstruct):
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

def preprocess_video_job(job_name, video_name, video_format, unzippedfolder, data_path, data_base, data_name, input_data_bucket, lab_group_name, numframes, start_point, end_point, selection_mode, labels, shortintruct, fullinstruct):
    #s3.Bucket(input_data_bucket).download_file(data_path, (data_name + )
    #s3client.copy_object(Bucket = target_bucket, CopySource = {"Bucket" : input_data_bucket, "Key": data_path}, Key = job_name + "/data/videos/"+ data_name) #copies the original video to the output location
    if video_format[0] != '.':
        video_format = "." + video_format
    unzippedfolder.extract(data_base + '/' + video_name + video_format)
    cap = cv2.VideoCapture(data_base + '/' + video_name + video_format)
    frame_width = cap.get(3)
    frame_height = cap.get(4)
    frameRate = cap.get(5)
    totFrameCount = cap.get(7)
    start_frame = int(totFrameCount * start_point)
    end_frame = int(totFrameCount * end_point) + 1
    print(start_frame, end_frame)
    relFrameCount = end_frame - start_frame
    print(frame_width, frame_height)
    print(totFrameCount, flush=True)
    frame_freq = relFrameCount // numframes
    print(frame_freq)
    cap.set(1, start_frame)
    frames = []
    f = 0
    sequence_1 = {} #only one sequence
    sequence_1["seq-no"] = 1
    sequence_1["prefix"] = ("s3://" + input_data_bucket + "/" + lab_group_name + '/inputs/' + job_name + "/" + video_name + "/")
    if selection_mode == "base":
        while(cap.isOpened()):
            if f >= numframes:
                print("exceeded")
                break
            frameId = cap.get(1) #current frame number  
            if frameId > end_frame:
                print("reached last relevant frame")
                break
            ret, frame = cap.read()
            if (ret != True):
                break
            if frameId % frame_freq != 0:
                continue
            hasFrame, imageBytes = cv2.imencode(".jpg", frame)
            if(hasFrame):
                s3client.put_object(Bucket= input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + "/" + video_name + '/frame_' + str(int(frameId)) + '.jpg'), Body=imageBytes.tobytes())
                frame_dict = {}
                frame_dict["frame-no"] = f + 1
                frame_dict["unix-timestamp"] = 2 #doesn't matter
                frame_dict["frame"] = ("frame_" + str(int(frameId)) + '.jpg')
                frames.append(frame_dict)
                f += 1          
        cap.release()
    
    #TODO: add fancy frame selection tools
    elif selection_mode == "motion_pca_cluster":
        #240 input frames is too large 
        #210 input frames is ok 
        #frame size we have been using is 512 *  640 * 3 = 983,040
        frame_size = frame_height * frame_width * 3
        final_array_size = int(frame_size) * numframes
        frame_mult = 206438400 // final_array_size #could be more precise about the max memory capacity for the pca_array
        if frame_mult < 1:
            print("You are trying to select too many frames given your frame size")
            exit()
        #memory issue is in computing PCA
        #240 times this video size (H * W * C) is too large
        numinputframes = int(numframes * frame_mult)
        print(numinputframes, flush = True)
        motion_energy_values = []
        if start_frame != 0:
            cap.set(1, start_frame - 1)
            ret, prev_frame = cap.read()
        else:
            ret, prev_frame = cap.read()
            #cap.set(1, start_frame + 1)
        print(prev_frame.shape)
        while(cap.isOpened()):
            frameId = cap.get(1) #current frame number  
            if frameId > end_frame:
                break
            ret, frame = cap.read()
            if (ret != True):
                break
            me = np.mean(np.absolute(frame - prev_frame))
            motion_energy_values.append((frameId, me))
            prev_frame = frame
        print("computed motion energy", flush=True)
        motion_energy_values.sort(key=lambda x:x[1])
        motion_energy_values = motion_energy_values[:numinputframes]
        frame_array = np.empty(shape=(numinputframes, int(frame_height), int(frame_width), 3))
        top_me_frames = dict(motion_energy_values)
        cap.set(1, start_frame)
        frame_array_idx = 0
        frame_array_frame_idxs = {}
        while(cap.isOpened()):
            frameId = cap.get(1) #current frame number  
            if frameId > end_frame:
                break
            ret, frame = cap.read()
            if (ret != True):
                break
            if frameId in top_me_frames.keys():
                frame_array[frame_array_idx] = frame
                frame_array_frame_idxs[frame_array_idx] = frameId
                frame_array_idx += 1
        print("filled frame array", flush=True)
        pca_array = frame_array.reshape((numinputframes, -1))
        pca_components = min(pca_array.shape[0], pca_array.shape[1], PCA_DIM)
        pca = PCA(n_components = pca_components) #check what SLEAP does, they do
        print(pca_array.shape, flush = True)
        compressed_array = pca.fit_transform(pca_array)
        print("computed PCA", flush=True)      
        print(sum(pca.explained_variance_ratio_), flush = True)
        print(compressed_array.shape)
        kmeans = KMeans(n_clusters = numframes, random_state = RANDOM_SEED)
        cluster_idxs = kmeans.fit_predict(compressed_array)
        print("Ran K means", flush=True)      
        print(cluster_idxs)
        used_clusters = set()
        final_idxs = [] #indexs are of the frame array, not of the actual video
        for frame_idx, cluster_idx in enumerate(cluster_idxs):
            if cluster_idx not in used_clusters:
                final_idxs.append(frame_idx)
                used_clusters.add(cluster_idx)
        
        print(final_idxs, flush=True)
        print(frame_array.shape)
        final_frame_array = frame_array[final_idxs]
        final_frame_idxs = [motion_energy_values[i][0] for i in final_idxs]
        final_frame_og_idxs = [frame_array_frame_idxs[idx] for idx in final_idxs]
        print(final_frame_array.shape)
        for fin_idx, frame in enumerate(final_frame_array):
            hasFrame, imageBytes = cv2.imencode(".jpg", frame)
            if(hasFrame):
                s3client.put_object(Bucket= input_data_bucket, Key=(lab_group_name + '/inputs/' + job_name + "/" + video_name + '/frame_' + str(int(final_frame_og_idxs[fin_idx])) + '.jpg'), Body=imageBytes.tobytes())
                frame_dict = {}
                frame_dict["frame-no"] = f + 1
                frame_dict["unix-timestamp"] = 2 #doesn't matter
                frame_dict["frame"] = ("frame_" + str(int(final_frame_og_idxs[fin_idx])) + '.jpg')
                frames.append(frame_dict)
                f += 1               
        cap.release()
    os.remove(data_base + '/' + video_name + video_format)
    sequence_1["frames"] = frames
    sequence_1["number-of-frames"] = f + 1
    uploadInfo(input_data_bucket, lab_group_name, sequence_1, data_base, data_name, video_name, job_name, labels, shortintruct, fullinstruct)
    #exit()



