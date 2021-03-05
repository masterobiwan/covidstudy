import requests
import json
import boto3
# request data from api
response = requests.get("https://covidmap.umd.edu/api/resources?indicator=covid&type=smoothed&country=Afghanistan&daterange=20200424-20200426").text

#convert json data to dic data for use!
jsonData = json.loads(response)

print(json.dumps(jsonData, indent=4, sort_keys=True))

content = ""
for record in jsonData["data"]:
    content = "{ctt}{rec}\n".format(ctt=content, rec=record)

session = boto3.Session(profile_name="perso")
s3 = session.client('s3')
s3.put_object(Body=content, Bucket='castelnajac-open-data', Key='covidstudy/test.json')

print(content)
