import boto3
import json
import sys
import os
import yaml
from preprocess import preprocess_frames_job, preprocess_video_job
import collections
import zipfile
from datetime import datetime

UPID = 'us-east-1_ZxGaQUSI2'
CLIENTID = '7cujg3m8o3sh6cqg39lcbc1ool'
#LABELURI = 'https://pgvx2rzogw.labeling.us-east-1.sagemaker.aws'

def createLabelJob(users, job_name, input_data_bucket, datasetname):   #You must use the same clientId for all of your workteams, so all workteams must be part of the same userpool                
        humantaskuiarn = 'arn:aws:sagemaker:us-east-1:394669845002:human-task-ui/VideoObjectTracking'
        prehumantasklambdaarn = 'arn:aws:lambda:us-east-1:432418664414:function:PRE-VideoObjectTracking'
        annotationconsolidationconfigarn = 'arn:aws:lambda:us-east-1:432418664414:function:ACS-VideoObjectTracking'

        job = smclient.create_labeling_job(LabelingJobName= job_name, LabelAttributeName= "Label-ref", #could change labelattributename
                InputConfig={'DataSource': {'S3DataSource': {'ManifestS3Uri' : "s3://"+ input_data_bucket + "/" + lab_group_name + "/inputs/" + job_name + '/' + datasetname + ".manifest.json"}}}, 
                OutputConfig= {'S3OutputPath': ("s3://"+ input_data_bucket + "/" + lab_group_name + "/" + output_dir)}, 
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
         
        print("https://neurocaasdomain.auth.us-east-1.amazoncognito.com/login?response_type=code&client_id=" + CLIENTID + "&redirect_uri=https://"+ labeluri + "/oauth2/idpresponse")
        print("labeluri: " + labeluri)

if __name__ == "__main__":
        #might want to change all the video_ variables to outer_folder_ or data_ 
        input_data_bucket = sys.argv[1] #$bucketname
        data_name = sys.argv[2] #$dataname, might want to rename to outerfolder name
        data_path = sys.argv[3] #$inputpath, likewise
        config_name = sys.argv[4] #$configname
        config_path = sys.argv[5] #$configpath and assume custom config file layout
        output_dir = sys.argv[6] #$processdir
        lab_group_name = sys.argv[7] #$groupdir


client = boto3.client('cognito-idp', region_name = 'us-east-1')
smclient = boto3.client('sagemaker', region_name = 'us-east-1')
s3 = boto3.resource('s3', region_name = 'us-east-1')
print("input_data_bucket:" +  input_data_bucket)
s3.Bucket(input_data_bucket).download_file(config_path, config_name)
print(config_name)
with open(config_name, 'r') as f:
        doc = yaml.load(f)
        users = doc['labelers']
        jobs_info = doc['jobs_info']
        shortinstruct = doc['shortinstruct']
        fullinstruct = doc['fullinstruct']
        data_format = doc['dataformat']
        bodyparts = doc['bodyparts']
        skeleton = doc["skeleton"]
        #target_bucket =  doc['finaldatabucket']
print(doc)
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

data_base = data_name.split('.', 1)[0] #gets the name of the zipfile without the extention
s3.Bucket(input_data_bucket).download_file(data_path, data_name) #downloads zip file of data
unzippedfolder = zipfile.ZipFile(data_name, "r")

if data_format == "frames":
        file_dict = collections.defaultdict(list)
        for file in unzippedfolder.namelist():
                file_split = file.split("/")   
                if len(file_split) != 3 or file_split[-1] == '.DS_Store': #basefolder/datasetfolder/imagename
                        continue
                datasetname = file_split[1]
                file_dict[datasetname].append(file)    

updated_jobs_info = {}
labels = []
for label in bodyparts:
        labels.append({'label': label})

for job_name, jobinfo in jobs_info.items():
        try:
                smclient.describe_labeling_job(LabelingJobName= job_name)
                now = datetime.now()
                now = now.strftime("%Y%m%d%H%M%S%f")
                print("Job name already exists, adding date and time to job name")
                unique_job_name = job_name + now
        except:
                unique_job_name = job_name
        datasetname = jobinfo["datasetname"]
        #bodyparts = jobinfo["bodyparts"]
        #skeleton = jobinfo["skeleton"]
        #model_config = {
                #'process_dir': output_dir, #added as lambda function needs this info to get the output of the labeling job
                #'data_name': data_name.split('.')[0],
                # 'Task': 'Reaching',
                # 'TrainingFraction': [0.95],
                # 'alphavalue': 0.7,
                # 'batch_size': 4,
                # #'bodyparts': bodyparts,
                # 'colormap': 'jet',
                # 'corner2move2': [50, 50],
                # 'cropping': 'false',
                # 'date': 'Aug30',
                # 'default_net_type': 'resnet_50',
                # 'dotsize': 12,
                # 'iteration': 0,
                # 'move2corner': 'true',
                # #'numframes2pick': numframes,
                # 'pcutoff': 0.4,
                # 'project_path': "data", #changed, no /Reaching-Mackenzie-2018-08-30
                # "resnet": "null",
                # "scorer": "Mackenzie",
                # "skeleton": skeleton, #[["Hand", "Finger1"],["Joystick1", "Joystick2"]],
                # "skeleton_color": 'blue',
                # "snapshotindex": -1,
                # "start": 0,
                # "stop": 1,
                # "video_sets": {
                # "videos/" + data_path.split('/')[-1]:
                # {'crop' : (0, 832, 0, 747)}
                # },
                # 'x1': 0,
                # 'x2': 640,
                # 'y1': 277,
                # 'y2': 624}
        # with open('dlc_config.yaml', 'w') as f: #creating dlc config file
        #         yaml.dump(model_config, f)
        #s3.Bucket(target_bucket).upload_file('dlc_config.yaml', unique_job_name + '/data/dlc_config.yaml')
        #os.remove('dlc_config.yaml') #deleting model training config file
        #s3client.copy_object(Bucket = input_data_bucket, CopySource = {"Bucket" : input_data_bucket, "Key": config_path}, Key = lab_group_name + "/configs/" + unique_job_name + "/config.yaml") 
        
        if data_format == "frames":
                preprocess_frames_job(unique_job_name, file_dict[datasetname], unzippedfolder, data_path, data_base, data_name, input_data_bucket, lab_group_name, labels, datasetname, shortinstruct, fullinstruct)
        else:
                start_point = jobinfo["start_point_proportion"]
                end_point = jobinfo["end_point_proportion"]
                selection_mode = jobinfo["selection_mode"]
                numframes = jobinfo["numframes2pick"]
                video_format = jobinfo["format"]
                preprocess_video_job(unique_job_name, datasetname, video_format, unzippedfolder, data_path, data_base, data_name, input_data_bucket, lab_group_name, numframes, start_point, end_point, selection_mode, labels, shortinstruct, fullinstruct)
        createLabelJob(users, unique_job_name, input_data_bucket, datasetname)
        updated_jobs_info[unique_job_name] = jobinfo

os.remove(data_name) #name of outer folder
os.remove(config_name) #base config file

doc['jobs_info'] = updated_jobs_info
print(doc)
with open('config.yaml', 'w') as f:
        yaml.dump(doc, f)
for job_name in doc['jobs_info'].keys():
        s3.Bucket(input_data_bucket).upload_file('config.yaml', lab_group_name + '/configs/' + job_name + "/config.yaml")
os.remove('config.yaml')

#or just copy to the original config file to the output directory and change the name to config.yaml
# output_config = {
#         'process_dir': output_dir,
#         'outerfoldername': data_base
# }


