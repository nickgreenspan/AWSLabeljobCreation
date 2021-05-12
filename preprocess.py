import boto3
import json
import yaml
import cv2
import math
from datetime import datetime
import os
#import io

s3client = boto3.client('s3', region_name = 'us-east-1')
s3 = boto3.resource('s3', region_name = 'us-east-1')

def preprocess_job(video_bucket, video_path, video_name, input_data_bucket, lab_group_name, numframes, anntype, labels, datasetname, shortintruct, fullinstruct):
	#newbuc = s3.create_bucket(ACL = 'public-read-write', Bucket=input_data_bucket) #if bucket doens't already exist
	video_base = video_name.split('.', 1)[0] #gets the filename without extension
	s3.Bucket(video_bucket).download_file(video_path, video_name)
	cap = cv2.VideoCapture(video_name)
	frameRate = cap.get(5)
	frames = []
	f = 0
	sequence_1 = {} #starting with only one sequence
	sequence_1["seq-no"] = 1
	sequence_1["prefix"] = ("s3://" + input_data_bucket + "/" + lab_group_name + '/inputs/' + video_base + "/")

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
				s3client.put_object(Bucket= input_data_bucket, Key=(lab_group_name + '/inputs/' + video_base + '/frame_' + str(f) + '.jpeg'), Body=imageBytes.tobytes())
				frame_dict = {}
				frame_dict["frame-no"] = f + 1
				frame_dict["unix-timestamp"] = 2 #doesn't matter
				frame_dict["frame"] = ("frame_" + str(f) + '.jpeg')
				frames.append(frame_dict)
				f += 1		
	cap.release()

	sequence_1["frames"] = frames
	sequence_1["number-of-frames"] = f + 1

	#uploads sequence file    
	s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/' + video_base + '/seq1.json'), Body=(bytes(json.dumps(sequence_1).encode('UTF-8'))))

	#creates manifest file
	manifest = {}
	manifest["source-ref"] = "s3://" + input_data_bucket + '/' + lab_group_name + '/inputs/' + video_base + "/seq1.json"

	s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/' + datasetname + '.manifest.json'), Body=(bytes(json.dumps(manifest).encode('UTF-8'))))

	#create label config file
	label_config = {}
	label_config["document-version"]= datetime.today().strftime('%Y-%m-%d')
	label_config["annotationType"] = anntype #ex: "Keypoint" 
	label_config["instructions"] = {"shortInstruction": shortintruct, "fullInstruction": fullinstruct}
	label_config["labels"] = labels

	s3client.put_object(Bucket = input_data_bucket, Key=(lab_group_name + '/inputs/label_config_full.json'), Body=(bytes(json.dumps(label_config).encode('UTF-8'))))
	os.remove(video_name)

