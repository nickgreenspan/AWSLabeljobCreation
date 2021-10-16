import boto3
import json
import sys
import os
import yaml
from preprocess import preprocess_job
import collections

UPID = 'us-east-1_ZxGaQUSI2'
CLIENTID = '7cujg3m8o3sh6cqg39lcbc1ool'
#LABELURI = 'https://pgvx2rzogw.labeling.us-east-1.sagemaker.aws'

def createLabelJob(users, job_name, input_data_bucket, datasetname):   #You must use the same clientId for all of your workteams, so all workteams must be part of the same userpool
        # try:
        #         group = client.create_group(GroupName = lab_group_name, UserPoolId = UPID) #note groupname requirements
        # except:
        #         group = client.get_group(GroupName = lab_group_name, UserPoolId = UPID)
        
        # for user in users:
        #         try: 
        #                 userinfo = client.admin_create_user(UserPoolId=UPID, Username = user[0], UserAttributes=[{'Name': 'email', 'Value': user[1]}]) #once the users are added to the userpool they don't need to be added again
        #         except:
        #                 print(user[0] + " already in userpool, continuing")
                
        #         try:
        #                 client.admin_add_user_to_group(UserPoolId = UPID, Username = user[0], GroupName = group['Group']['GroupName'])
        #         except:
        #                 print(user[0] + " already in user group, continuing")

        # #You cannot create more than 25 work teams in an account and region
        # try: 
        #         workteam = smclient.create_workteam(WorkteamName= "Team" + lab_group_name, MemberDefinitions=[{'CognitoMemberDefinition': {'UserPool': UPID, 'UserGroup': group['Group']['GroupName'], 'ClientId': clientId}}], Description='Team' + lab_group_name)
        #         workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
        # except:
        #         workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
                
        humantaskuiarn = 'arn:aws:sagemaker:us-east-1:394669845002:human-task-ui/VideoObjectTracking'
        prehumantasklambdaarn = 'arn:aws:lambda:us-east-1:432418664414:function:PRE-VideoObjectTracking'
        annotationconsolidationconfigarn = 'arn:aws:lambda:us-east-1:432418664414:function:ACS-VideoObjectTracking'

        #check labeling job is unique by making sure "describe labeling job s3 command fails"
        job = smclient.create_labeling_job(LabelingJobName= job_name, LabelAttributeName= "Label-ref", #could change labelattributename
                InputConfig={'DataSource': {'S3DataSource': {'ManifestS3Uri' : "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + job_name + '/' + datasetname + ".manifest.json"}}}, 
                OutputConfig= {'S3OutputPath': ("s3://"+ input_data_bucket + "/" + lab_group_name + "/" + job_name + "/" + output_dir)}, 
                RoleArn = "arn:aws:iam::739988523141:role/labeljobcreator", #predefined role
                LabelCategoryConfigS3Uri = "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + job_name + "/label_config.json",
                HumanTaskConfig = {
                        'WorkteamArn': workteam['Workteam']['WorkteamArn'],
                        'UiConfig': {'HumanTaskUiArn': humantaskuiarn}, #need to be changed depending on label job type
                'PreHumanTaskLambdaArn': prehumantasklambdaarn,
                'TaskTitle': (job_name),
                'TaskDescription': "Label the data", #can change if necessary
                'NumberOfHumanWorkersPerDataObject': 1, #can change this
                'TaskTimeLimitInSeconds' : 172800,
                'AnnotationConsolidationConfig': {
                'AnnotationConsolidationLambdaArn': annotationconsolidationconfigarn #we should figure what we want to do about conflicting annotations
                }
        })
        

        #writes labeling url info to file and uploads to bucket
        # file1 = open('labeling_urls.txt', 'w')
        # file1.write("https://neurocaas.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + CLIENTID + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse\n")
        # file1.write(labeluri)
        # file1.close()
        # s3.Bucket(input_data_bucket).upload_file('labeling_urls.txt', lab_group_name + '/' + output_dir +  '/labeling_urls.txt')
        # os.remove('labeling_urls.txt')
        print("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + CLIENTID + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse")
        print("labeluri: " + labeluri)

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

print(config_name)
with open(config_name, 'r') as f:
        doc = yaml.load(f)
        users = doc['labelers']
        jobs_info = doc['jobs_info']
        #datasetname = doc['datasetname'] #note requirements for datasetname
        shortintruct = doc['shortintruct']
        fullinstruct = doc['fullinstruct']
        #bodyparts = doc['bodyparts']
        target_bucket = doc['finaldatabucket']
        #skeleton = doc['skeleton']
        data_format = doc['dataformat']
        # for label in bodyparts:
        #         task_labels.append({'label': label})
        numframes = doc['numframes2pick']
os.remove(config_name)

try:
        group = client.create_group(GroupName = lab_group_name, UserPoolId = UPID) #note groupname requirements
except:
        group = client.get_group(GroupName = lab_group_name, UserPoolId = UPID)

for user in users:
        try: 
                userinfo = client.admin_create_user(UserPoolId=UPID, Username = user[0], UserAttributes=[{'Name': 'email', 'Value': user[1]}]) #once the users are added to the userpool they don't need to be added again
        except:
                print(user[0] + " already in userpool, continuing")
        
        try:
                client.admin_add_user_to_group(UserPoolId = UPID, Username = user[0], GroupName = group['Group']['GroupName'])
        except:
                print(user[0] + " already in user group, continuing")

#You cannot create more than 25 work teams in an account and region
try: 
        workteam = smclient.create_workteam(WorkteamName= "Team" + lab_group_name, MemberDefinitions=[{'CognitoMemberDefinition': {'UserPool': UPID, 'UserGroup': group['Group']['GroupName'], 'ClientId': CLIENTID}}], Description='Team' + lab_group_name)
        workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)
except:
        workteam = smclient.describe_workteam(WorkteamName = "Team" + lab_group_name)

labeluri = workteam['Workteam']['SubDomain']
print("labeluri: " + labeluri)

video_base = video_name.split('.', 1)[0] #gets the name of the zipfile without the extention
s3.Bucket(video_bucket).download_file(video_path, video_name) #downloads zip file of folder of frames
unzippedfile = zipfile.ZipFile(video_name, "r")
file_dict = collections.defaultdict(list)
for file in unzippedfile.namelist():
        file_split = file.split("/")
        if len(file_split) != 3 or file_split[-1] == '.DS_Store': #basefolder/datasetfolder/imagename
                continue
        datasetname = file_split[1]
        if datasetname in jobs_info.keys():
                file_dict[datasetname].append(file)
        
for job_name, jobinfo in jobs_info.items():
        datasetname = jobinfo["datasetname"]
        bodyparts = jobinfo["bodyparts"]
        skeleton = jobinfo["skeleton"]
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
                'numframes2pick': numframes,
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
        with open('dlc_config.yaml', 'w') as f: #creating dlc config file
                yaml.dump(model_config, f)
        s3.Bucket(target_bucket).upload_file('dlc_config.yaml', job_name + '/data/dlc_config.yaml')
        os.remove('dlc_config.yaml') #deleting model training config file
        labels = []
        for label in bodyparts:
                labels.append({'label': label})
        if data_format == "frames":
                preprocess_frames_job(job_name, file_dict[datasetname], unzippedfile, video_bucket, video_path, video_name, input_data_bucket, target_bucket, lab_group_name, labels, datasetname, shortintruct, fullinstruct)
        else:
                preprocess_video_job(job_name, video_bucket, video_path, video_name, input_data_bucket, target_bucket, lab_group_name, numframes, labels, datasetname, shortintruct, fullinstruct)
        createLabelJob(users, job_name, input_data_bucket, datasetname)

