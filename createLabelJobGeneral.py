import boto3
import json
import sys
import os
from preprocess import preprocess_job

if __name__ == "__main__":
	video_bucket = sys.argv[0] #$bucketname
	video_name = sys.argv[1] #$dataname
	video_path = sys.argv[2] #$inputpath
	config_name = sys.argv[3] #$configname
	config_path = sys.argv[4] #$configpath and assume custom config file layout
	input_data_bucket = video_bucket #assuming they should be the same
	output_dir = sys.argv[5] #$processdir
	lab_group_name = sys.argv[6] #$groupdir

client = boto3.client('cognito-idp')
smclient = boto3.client('sagemaker')

s3.Bucket(video_bucket).download_file(configpath, configname)
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

upid = 'us-east-1_p4vRURAiH'
clientId = 'kj12tmdfgd99gmmr94ho33lbb'
clientName = 'NeuroCAASClient'
#labeluri = 'h1iq9tykmt.labeling.us-east-1.sagemaker.aws'

def createLabelJob(users, jobname, input_data_bucket, datasetname, anntype):   #You must use the same clientId for all of your workteams, so all workteams must be part of the same userpool
	group = client.create_group(GroupName = lab_group_name, UserPoolId = upid) #note groupname requirements
	for user in users:
		try: 
			userinfo = client.admin_create_user(UserPoolId=upid, Username = user[0], UserAttributes=[{'Name': 'email', 'Value': user[1]}]) #once the users are added to the userpool they don't need to be added again
		except:
			print(user[0] + " already in userpool, continuing")
		client.admin_add_user_to_group(UserPoolId = upid, Username = user[0], GroupName = group['Group']['GroupName'])

	#You cannot create more than 25 work teams in an account and region
	workteam = smclient.create_workteam(WorkteamName= "Team" + lab_group_name, MemberDefinitions=[{'CognitoMemberDefinition': {'UserPool': upid, 'UserGroup': group['Group']['GroupName'], 'ClientId': clientId}}], Description='Team' + lab_group_name)
	humantaskuiarn = ""
	prehumantasklambdaarn = ""
	annotationconsolidationconfigarn = ""
	if (anntype == "Keypoint"):
		humantaskuiarn = 'arn:aws:sagemaker:us-east-1:394669845002:human-task-ui/VideoObjectTracking'
		prehumantasklambdaarn = 'arn:aws:lambda:us-east-1:432418664414:function:PRE-VideoObjectTracking'
		annotationconsolidationconfigarn = 'arn:aws:lambda:us-east-1:432418664414:function:ACS-VideoObjectTracking'

	job = smclient.create_labeling_job(LabelingJobName=jobname, LabelAttributeName= "Label-ref", #could change labelattributename
		InputConfig={'DataSource': {'S3DataSource': {'ManifestS3Uri' : "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + datasetname + ".manifest.json"}}}, 
		OutputConfig= {'S3OutputPath': ("s3://"+ input_data_bucket + "/" + lab_group_name + "/" + output_dir)}, 
		RoleArn = "arn:aws:iam::020259116521:role/labeljobcreator", #predefined role
		LabelCategoryConfigS3Uri = "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/label_config_full.json",
		HumanTaskConfig = {
			'WorkteamArn': workteam['WorkteamArn'],
			'UiConfig': {'HumanTaskUiArn': humantaskuiarn}, #need to be changed depending on label job type
	        'PreHumanTaskLambdaArn': prehumantasklambdaarn,
	        'TaskTitle': jobname,
	        'TaskDescription': "Label the data", #can change if necessary
	        'NumberOfHumanWorkersPerDataObject': 1, #len(users), #need to think about this, not sure how multiple people labeling one dataset will work
	        'TaskTimeLimitInSeconds' : 172800,
	        'AnnotationConsolidationConfig': {
	            'AnnotationConsolidationLambdaArn': annotationconsolidationconfigarn #we should figure what we want to do about conflicting annotations
	        }
	    })

	workteam_info = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
	labeluri = workteam_info['Workteam']['SubDomain']

	#writes labeling url info to file and uploads to bucket
	file1 = open('labeling_urls.txt', 'w')
	file1.write("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse\n")
	file1.write(labeluri)
	file1.close()
	s3.Bucket(input_data_bucket).upload_file('labeling-urls.txt', lab_group_name + '/' + output_dir +  '/labeling-urls.txt')


	print("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + clientId + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse")
	print("labeluri: " + labeluri)

# users = [['Nicktestname', 'nicholasg101@gmail.com'], ['Nicktest2', 'nrg2148@columbia.edu']] sample format for users list
# users1 = [['Nicktest2', 'nrg2148@columbia.edu']]
# users2 = [['Nicktestname', 'nicholasg101@gmail.com']]
# labels = ['cat', 'dog', 'fish'] #not used
# jobname = "GeneralTestAWS14"
# video_bucket = 'sagemakerneurocaastest'
# #video_bucket = 'bucket-video-test-1'
# video_path = 'username/inputs/'
# #video_path = ''
# video_name = 'reachingvideo1'
# #video_name = 'vid_view_1'
# input_data_bucket = 'nickneurocaastest2'
# anntype = "Keypoint"
# numframes = 100 #not used
# datasetname = 'dataset200'
# shortintruct = "please label"
# fullinstruct = "please label now"

preprocess_job(video_bucket, video_path, video_name, input_data_bucket, lab_group_name, numframes, anntype, task_labels, datasetname, shortintruct, fullinstruct)
createLabelJob(users, jobname, input_data_bucket, datasetname, anntype)

os.remove(configname)
os.remove('labeling_urls.txt')

