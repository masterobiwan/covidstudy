from lib.covidapi import formatJson, printJson, Client
import boto3

apiclient = Client()

selected_countries = "France"
#selected_countries = "all"
if selected_countries == "all":
    countries_data = apiclient.get_countries()["data"]
else:
    countries_data = [{"country": "France"}]

indicators = ["covid"]

session = boto3.Session(profile_name="perso")
s3 = session.client('s3')
for country in countries_data:
    country_dates = apiclient.get_dates(country=country["country"])
    for country_date in country_dates["data"]:
        for indicator in indicators:
            indicator_data = apiclient.get_indicator(indicator=indicator, type="daily", country=country["country"], date=country_date["survey_date"], formatter=formatJson)
            s3.put_object(Body=indicator_data, Bucket="castelnajac-open-data", Key="covidstudy/country={country}/date={date}/indicators.json".format(country=country["country"], date=country_date["survey_date"]))
