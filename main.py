from lib.covidapi import formatJson, printJson, Client
from pandas import read_json
import json
import boto3

apiclient = Client()

selected_countries = "France"
if selected_countries is None:
    countries_data = apiclient.get_countries()["data"]
else:
    countries_data = [
        {"country": "France"},
        {"country": "Germany"},
    ]

indicators = [
    {"name": "covid", "field": "cli"},
    {"name": "flu", "field": "ili"},
    {"name": "mask", "field": "mc"},
    {"name": "contact", "field": "dc"},
    {"name": "finance", "field": "hf"},
    {"name": "anosmia", "field": "anos"},
    {"name": "vaccine_acpt", "field": "vu"},
    {"name": "covid_vaccine"},
    {"name": "trust_fam"},
    {"name": "trust_healthcare"},
    {"name": "trust_who"},
    {"name": "trust_govt"},
    {"name": "trust_politicians"},
    {"name": "twodoses"},
    {"name": "concerned_sideeffects"},
    {"name": "hesitant_sideeffects"},
    {"name": "modified_acceptance"},
    {"name": "access_wash"},
    {"name": "wash_hands_24h_3to6"},
    {"name": "wash_hands_24h_7orMore"},
    {"name": "cmty_covid"},
    {"name": "barrier_reason_side_effects"},
    {"name": "barrier_reason_wontwork"},
    {"name": "barrier_reason_dontbelieve"},
    {"name": "barrier_reason_dontlike"},
    {"name": "barrier_reason_waitlater"},
    {"name": "barrier_reason_otherpeople"},
    {"name": "barrier_reason_cost"},
    {"name": "barrier_reason_religious"},
    {"name": "barrier_reason_government"},
    {"name": "barrier_reason_other"},
    {"name": "trust_doctors"},
    {"name": "barrier_reason_dontneed_alreadyhad"},
    {"name": "barrier_reason_dontneed_dontspendtime"},
    {"name": "barrier_reason_dontneed_nothighrisk"},
    {"name": "barrier_reason_dontneed_takeprecautions"},
    {"name": "barrier_reason_dontneed_notserious"},
    {"name": "barrier_reason_dontneed_notbeneficial"},
    {"name": "barrier_reason_dontneed_other"},
    {"name": "informed_access"},
    {"name": "appointment_have"},
    {"name": "appointment_tried"},
]

selected_dates = [
    {"survey_date": 20200510},
    {"survey_date": 20210310},
]

session = boto3.Session(profile_name="perso")
s3 = session.client('s3')
countries_json = {}
for country in countries_data:
    if selected_dates is None:
        country_dates = apiclient.get_dates(country=country["country"])["data"]
    else:
        country_dates = selected_dates
    for country_date in country_dates:
        for indicator_dict in indicators:
            indicator = indicator_dict["name"]
            indicator_data = apiclient.get_indicator(indicator=indicator, type="daily", country=country["country"], date=country_date["survey_date"])["data"]
            #df[indicator] = read_json(json.dumps(indicator_data))
            if indicator not in countries_json:
                countries_json[indicator] = []
            countries_json[indicator].extend(indicator_data)
            #s3.put_object(Body=indicator_data, Bucket="castelnajac-open-data", Key="covidstudy/country={country}/date={date}/indicators.json".format(country=country["country"], date=country_date["survey_date"]))
    
printJson(countries_json)
