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
trial_num = 5
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
		for label in bodyparts:
			task_labels.append({'label': label})
		numframes = doc['numframes2pick']

upid = 'us-east-1_ZxGaQUSI2'
clientId = '7cujg3m8o3sh6cqg39lcbc1ool'
#labeluri = 'https://pgvx2rzogw.labeling.us-east-1.sagemaker.aws'

def createLabelJob(users, jobname, input_data_bucket, datasetname, anntype):   #You must use the same clientId for all of your workteams, so all workteams must be part of the same userpool
	group = client.create_group(GroupName = (lab_group_name + str(trial_num)), UserPoolId = upid) #note groupname requirements
	for user in users:
		try: 
			userinfo = client.admin_create_user(UserPoolId=upid, Username = user[0], UserAttributes=[{'Name': 'email', 'Value': user[1]}]) #once the users are added to the userpool they don't need to be added again
		except:
			print(user[0] + " already in userpool, continuing")
		client.admin_add_user_to_group(UserPoolId = upid, Username = user[0], GroupName = group['Group']['GroupName'])

	#You cannot create more than 25 work teams in an account and region
	workteam = smclient.create_workteam(WorkteamName= "Team" + str(trial_num) + lab_group_name, MemberDefinitions=[{'CognitoMemberDefinition': {'UserPool': upid, 'UserGroup': group['Group']['GroupName'], 'ClientId': clientId}}], Description='Team' + lab_group_name)
	humantaskuiarn = ""
	prehumantasklambdaarn = ""
	annotationconsolidationconfigarn = ""
	if (anntype == "Keypoint"):
		humantaskuiarn = 'arn:aws:sagemaker:us-east-1:394669845002:human-task-ui/VideoObjectTracking'
		prehumantasklambdaarn = 'arn:aws:lambda:us-east-1:432418664414:function:PRE-VideoObjectTracking'
		annotationconsolidationconfigarn = 'arn:aws:lambda:us-east-1:432418664414:function:ACS-VideoObjectTracking'

	job = smclient.create_labeling_job(LabelingJobName= (jobname+ str(trial_num)), LabelAttributeName= "Label-ref", #could change labelattributename
		InputConfig={'DataSource': {'S3DataSource': {'ManifestS3Uri' : "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + datasetname + ".manifest.json"}}}, 
		OutputConfig= {'S3OutputPath': ("s3://"+ input_data_bucket + "/" + lab_group_name + "/" + output_dir)}, 
		RoleArn = "arn:aws:iam::739988523141:role/labeljobcreator", #predefined role
		LabelCategoryConfigS3Uri = "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/label_config_full.json",
		HumanTaskConfig = {
			'WorkteamArn': workteam['WorkteamArn'],
			'UiConfig': {'HumanTaskUiArn': humantaskuiarn}, #need to be changed depending on label job type
	        'PreHumanTaskLambdaArn': prehumantasklambdaarn,
	        'TaskTitle': (jobname+ str(trial_num)),
	        'TaskDescription': "Label the data", #can change if necessary
	        'NumberOfHumanWorkersPerDataObject': 1, #can change this
	        'TaskTimeLimitInSeconds' : 172800,
	        'AnnotationConsolidationConfig': {
	            'AnnotationConsolidationLambdaArn': annotationconsolidationconfigarn #we should figure what we want to do about conflicting annotations
	        }
	    })

	workteam_info = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
	labeluri = workteam_info['Workteam']['SubDomain']

	#writes labeling url info to file and uploads to bucket
	file1 = open('labeling_urls.txt', 'w')
	file1.write("https://neurocaas.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse\n")
	file1.write(labeluri)
	file1.close()
	s3.Bucket(input_data_bucket).upload_file('labeling_urls.txt', lab_group_name + '/' + output_dir +  '/labeling_urls.txt')

	print("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse")
	print("labeluri: " + labeluri)

preprocess_job(video_bucket, video_path, video_name, input_data_bucket, lab_group_name, numframes, annotationtype, task_labels, datasetname, shortintruct, fullinstruct)
createLabelJob(users, jobname, input_data_bucket, datasetname, annotationtype)

os.remove(config_name)
os.remove('labeling_urls.txt')

