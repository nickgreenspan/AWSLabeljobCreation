import boto3
import json
import yaml
import cv2
import math
from datetime import datetime
import os
#import io

s3client = boto3.client('s3')
s3 = boto3.resource('s3')

def preprocess_job(video_bucket, video_path, video_name, input_data_bucket, numframes, anntype, labels, datasetname, shortintruct, fullinstruct):
	task_labels = []
	s3.Bucket(video_bucket).download_file("username/configs/config.yaml", "config.yaml")
	with open('config.yaml', 'r') as f:
		doc = yaml.load(f)
		bodyparts = doc['bodyparts']
		print(bodyparts)
		print(type(bodyparts))
		for label in bodyparts:
			task_labels.append({'label': label})
		numframes = doc['numframes2pick'] #overrides
		print(numframes)
	#newbuc = s3.create_bucket(ACL = 'public-read-write', Bucket=input_data_bucket) #if bucket doens't already exist
	#s3response = s3client.get_object(Bucket= video_bucket, Key= video_name)
	#video = s3response["Body"]
	s3.Bucket(video_bucket).download_file(video_path + video_name + '.avi', video_name+ '.avi')
	cap = cv2.VideoCapture(video_name+'.avi') #not sure if compatable, typically wants videofile name
	frameRate = cap.get(5)
	frames = []
	f = 0
	sequence_1 = {} #starting with only one sequence
	sequence_1["seq-no"] = 1
	sequence_1["prefix"] = ("s3://" + input_data_bucket + "/" + video_name + "/")

	while(cap.isOpened()):
		if (f > numframes):
			print("exceeded")
			break
		frameId = cap.get(1) #current frame number
		ret, frame = cap.read()
		if (ret != True):
		    break
		if (frameId % math.floor(frameRate) == 0):
			hasFrame, imageBytes = cv2.imencode(".jpg", frame)
			if(hasFrame):
				s3client.put_object(Bucket= input_data_bucket, Key=(video_name + '/frame_' + str(f) + '.jpeg'), Body=imageBytes.tobytes()) #need to change numbering scheme for bigger nums
				frame_dict = {}
				frame_dict["frame-no"] = f + 1
				frame_dict["unix-timestamp"] = 2 #doesn't matter
				frame_dict["frame"] = ("frame_" + str(f) + '.jpeg') #only works for 10 or less frames (proof of concept)
				frames.append(frame_dict)
				f += 1		
	cap.release()

	sequence_1["frames"] = frames
	sequence_1["number-of-frames"] = f + 1

	#uploads sequence file    
	s3client.put_object(Bucket = input_data_bucket, Key=(video_name + '/seq1.json'), Body=(bytes(json.dumps(sequence_1).encode('UTF-8'))))

	#creates manifest file
	manifest = {}
	manifest["source-ref"] = "s3://" + input_data_bucket + "/" + video_name + "/seq1.json"

	s3client.put_object(Bucket = input_data_bucket, Key=(datasetname + '.manifest.json'), Body=(bytes(json.dumps(manifest).encode('UTF-8'))))

	#create label config file
	label_config = {}
	label_config["document-version"]= datetime.today().strftime('%Y-%m-%d')
	label_config["annotationType"] = anntype #ex: "Keypoint" 
	label_config["instructions"] = {"shortInstruction": shortintruct, "fullInstruction": fullinstruct}
	# for label in labels:
	# 	task_labels.append({'label': label})
	label_config["labels"] = task_labels

	s3client.put_object(Bucket = input_data_bucket, Key=('label_config_full_2.json'), Body=(bytes(json.dumps(label_config).encode('UTF-8'))))
	os.remove(video_name+'.avi')
	os.remove('config.yaml')

