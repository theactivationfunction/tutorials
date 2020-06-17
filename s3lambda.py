import json
import boto3
import os
#from flatten_json import flatten 
s3=boto3.resource('s3')
target_bucket='<your TARGET bucket name here>' # please update with the S3 bucket you will be sending files to 

# Amazon Rekognition
def detect_labels(source_image, bucket):
    client=boto3.client('rekognition')
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':source_image}},
        MaxLabels=8)
    return response
    
def detect_text(source_image, bucket):
    client=boto3.client('rekognition')
    response = client.detect_text(Image={'S3Object':{'Bucket':bucket,'Name':source_image}},
                                 Filters={'WordFilter':{'MinConfidence':80.0}})
    return response

    
# Flatten function so JSON will work nicely with Glue/Apache
# Tried import flatten_json from flatten, but not available
# This function from https://towardsdatascience.com/flattening-json-objects-in-python-f5343c794b10
def flatten(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def lambda_handler(event, context):
      try:  
         source_image=event["Records"][0]["s3"]["object"]["key"]
         source_bucket= event["Records"][0]["s3"]["bucket"]["name"]
         #print("Received event: " + json.dumps(event, indent=2))
         #print(event)
         #print(source_image)
         #print(source_bucket)
         
         # Call AWS Rekognition Services 
         photo_labels = detect_labels(source_image=source_image, bucket=source_bucket)
         photo_text = detect_text(source_image=source_image, bucket=source_bucket)
         print(photo_text)

         
         # We should now have a dictionary full of labels, and a dictionary full of text

         # 1. Process the photo_label dictionary into flattened json, one named detected object per line

         j=0
         outputstring=""
         for r in photo_labels["Labels"]:
            photo_labels["Labels"][j]["Source"]=source_image
            outputstring=outputstring+(json.dumps(flatten(photo_labels["Labels"][j])))+'\n'
            j=j+1
         print(outputstring)
         # Save the photo_label json file to the target S3 bucket
         # ** MAKE SURE THIS IS NOT THE IMAGE SOURCE BUCKET OR YOU MAY GET A LARGE BILL **
         if len(outputstring) > 0:
            print("Labels detected")
            object = s3.Object(target_bucket, "labels/" + source_image.split('.')[0] + ".labels.json")
            object.put(Body=outputstring)
         else:
             print("No Labels detected")
         # 2. Process the photo_text dictionary into flattened json, one named detected object per line
         j=0
         outputstring=""
         for r in photo_text["TextDetections"]:
           photo_text["TextDetections"][j]["Source"]=source_image
           outputstring=outputstring+(json.dumps(flatten(photo_text["TextDetections"][j])))+'\n'
           j=j+1
         # ** MAKE SURE THIS IS NOT THE IMAGE SOURCE BUCKET OR YOU MAY GET A LARGE BILL **
         # Save the photo_text json file to the target S3 bucket
         print(outputstring)
         if len(outputstring) > 0:
             print("Text detected")
             object = s3.Object(target_bucket, "text/" + source_image.split('.')[0] + ".text.json")
             object.put(Body=outputstring)
         else:
             print("No text detected")
     
         
         return {
            'statusCode': 200,
            'body': json.loads(json.dumps(event)) }
      except Exception as err:
        return {
            'statusCode': 400,
            'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(err)}
