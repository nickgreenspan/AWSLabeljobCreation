import boto3
import json
import sys
import os
import yaml
from preprocess import preprocess_job

if __name__ == "__main__":
	video_bucket = sys.argv[1] #$bucketname
	video_name = sys.argv[2] #$dataname
	video_path = sys.argv[3] #$inputpath
	config_name = sys.argv[4] #$configname
	config_path = sys.argv[5] #$configpath and assume custom config file layout
	input_data_bucket = video_bucket #assuming they should be the same
	output_dir = sys.argv[6] #$processdir
	lab_group_name = sys.argv[7] #$groupdir


#testing purposes ###############################
#trial_num = 251
#################################################

client = boto3.client('cognito-idp', region_name = 'us-east-1')
smclient = boto3.client('sagemaker', region_name = 'us-east-1')
s3 = boto3.resource('s3', region_name = 'us-east-1')
print("video_bucket:" +  video_bucket)
s3.Bucket(video_bucket).download_file(config_path, config_name)
task_labels = []
with open('config.yaml', 'r') as f:
		doc = yaml.load(f)
		users = doc['users']
		annotationtype = doc['anntype']
		jobname = doc['jobname']
		datasetname = doc['datasetname'] #note requirements for datasetname
		shortintruct = doc['shortintruct']
		fullinstruct = doc['fullinstruct']
		bodyparts = doc['bodyparts']
		target_bucket = doc['finaldatabucket']
		skeleton = doc['skeleton']
		data_format = doc['dataformat']
		for label in bodyparts:
			task_labels.append({'label': label})
		numframes = doc['numframes2pick']

os.remove(config_name)
model_config = {
'process_dir': output_dir, #added as lambda function needs this info to get the output of the labeling job
'video_name': video_name.split('.')[0],
'Task': 'Reaching',
'TrainingFraction': [0.95],
'alphavalue': 0.7,
'batch_size': 4,
'bodyparts': bodyparts,
'colormap': 'jet',
'corner2move2': [50, 50],
'cropping': 'false',
'date': 'Aug30',
'default_net_type': 'resnet_50',
'dotsize': 12,
'iteration': 0,
'move2corner': 'true',
'numframes2pick': 40,
'pcutoff': 0.4,
'project_path': "data", #changed, no /Reaching-Mackenzie-2018-08-30
"resnet": "null",
"scorer": "Mackenzie",
"skeleton": skeleton, #[["Hand", "Finger1"],["Joystick1", "Joystick2"]],
"skeleton_color": 'blue',
"snapshotindex": -1,
"start": 0,
"stop": 1,
"video_sets": {
  "videos/" + video_path.split('/')[-1]:
    {'crop' : (0, 832, 0, 747)}
},
'x1': 0,
'x2': 640,
'y1': 277,
'y2': 624}
with open('config.yaml', 'w') as f: #creating dlc config file
	yaml.dump(model_config, f)

s3.Bucket(target_bucket).upload_file('config.yaml', jobname + '/data/config.yaml')
upid = 'us-east-1_ZxGaQUSI2'
clientId = '7cujg3m8o3sh6cqg39lcbc1ool'
#labeluri = 'https://pgvx2rzogw.labeling.us-east-1.sagemaker.aws'

def createLabelJob(users, jobname, input_data_bucket, datasetname, anntype):   #You must use the same clientId for all of your workteams, so all workteams must be part of the same userpool
	try:
		group = client.create_group(GroupName = lab_group_name, UserPoolId = upid) #note groupname requirements
	except:
		group = client.get_group(GroupName = lab_group_name, UserPoolId = upid)
	
	for user in users:
		try: 
			userinfo = client.admin_create_user(UserPoolId=upid, Username = user[0], UserAttributes=[{'Name': 'email', 'Value': user[1]}]) #once the users are added to the userpool they don't need to be added again
		except:
			print(user[0] + " already in userpool, continuing")
		
		try:
			client.admin_add_user_to_group(UserPoolId = upid, Username = user[0], GroupName = group['Group']['GroupName'])
		except:
			print(user[0] + " already in user group, continuing")

	#You cannot create more than 25 work teams in an account and region
	try: 
		workteam = smclient.create_workteam(WorkteamName= "Team" + lab_group_name, MemberDefinitions=[{'CognitoMemberDefinition': {'UserPool': upid, 'UserGroup': group['Group']['GroupName'], 'ClientId': clientId}}], Description='Team' + lab_group_name)
		workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
	except:
		workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
	
	humantaskuiarn = ""
	prehumantasklambdaarn = ""
	annotationconsolidationconfigarn = ""
	if (anntype == "Keypoint"):
		humantaskuiarn = 'arn:aws:sagemaker:us-east-1:394669845002:human-task-ui/VideoObjectTracking'
		prehumantasklambdaarn = 'arn:aws:lambda:us-east-1:432418664414:function:PRE-VideoObjectTracking'
		annotationconsolidationconfigarn = 'arn:aws:lambda:us-east-1:432418664414:function:ACS-VideoObjectTracking'

	job = smclient.create_labeling_job(LabelingJobName= jobname, LabelAttributeName= "Label-ref", #could change labelattributename
		InputConfig={'DataSource': {'S3DataSource': {'ManifestS3Uri' : "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + datasetname + ".manifest.json"}}}, 
		OutputConfig= {'S3OutputPath': ("s3://"+ input_data_bucket + "/" + lab_group_name + "/" + output_dir)}, 
		RoleArn = "arn:aws:iam::739988523141:role/labeljobcreator", #predefined role
		LabelCategoryConfigS3Uri = "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/label_config_full.json",
		HumanTaskConfig = {
			'WorkteamArn': workteam['Workteam']['WorkteamArn'],
			'UiConfig': {'HumanTaskUiArn': humantaskuiarn}, #need to be changed depending on label job type
	        'PreHumanTaskLambdaArn': prehumantasklambdaarn,
	        'TaskTitle': (jobname),
	        'TaskDescription': "Label the data", #can change if necessary
	        'NumberOfHumanWorkersPerDataObject': 1, #can change this
	        'TaskTimeLimitInSeconds' : 172800,
	        'AnnotationConsolidationConfig': {
	            'AnnotationConsolidationLambdaArn': annotationconsolidationconfigarn #we should figure what we want to do about conflicting annotations
	        }
	    })

	#workteam_info = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
	labeluri = workteam['Workteam']['SubDomain']

	#writes labeling url info to file and uploads to bucket
	file1 = open('labeling_urls.txt', 'w')
	file1.write("https://neurocaas.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse\n")
	file1.write(labeluri)
	file1.close()
	s3.Bucket(input_data_bucket).upload_file('labeling_urls.txt', lab_group_name + '/' + output_dir +  '/labeling_urls.txt')

	print("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse")
	print("labeluri: " + labeluri)

preprocess_job(data_format, jobname, video_bucket, video_path, video_name, input_data_bucket, target_bucket, lab_group_name, numframes, annotationtype, task_labels, datasetname, shortintruct, fullinstruct)
createLabelJob(users, jobname, input_data_bucket, datasetname, annotationtype)

os.remove(config_name)
os.remove('labeling_urls.txt')

