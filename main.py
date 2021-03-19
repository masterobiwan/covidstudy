from lib.covidapi import Client
from pandas import read_json, to_datetime
import json

apiclient = Client()

#selected_countries = None
selected_countries = [{"country": "France"}]
if selected_countries is None:
    countries_data = apiclient.get_countries()["data"]
else:
    countries_data = selected_countries

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

selected_dates = None

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
            if indicator not in countries_json:
                countries_json[indicator] = []
            countries_json[indicator].extend(indicator_data)
    
primary_keys = ["survey_date", "country"]
df_countries = read_json(json.dumps(countries_data))
df_dates = read_json(json.dumps(selected_dates))
df = df_countries.merge(df_dates, how="cross")
for indicator in countries_json:
    indicator_data = countries_json[indicator]
    df2 = read_json(json.dumps(indicator_data)).drop(columns=["sample_size", "iso_code", "gid_0"])
    df = df.merge(df2, how="left", on=primary_keys)

df["date"] = to_datetime(df["survey_date"], format="%Y%m%d").dt.date
df = df.drop("survey_date", axis="columns")
rename_dict = {}
for indicator in indicators:
    rename_dict["percent_{old}".format(old=indicator["field"])] = "percent_{new}".format(new=indicator["name"])
    rename_dict["percent_{old}_unw".format(old=indicator["field"])] = "percent_{new}_unw".format(new=indicator["name"])
    rename_dict["{old}_se".format(old=indicator["field"])] = "{new}_se".format(new=indicator["name"])
    rename_dict["{old}_se_unw".format(old=indicator["field"])] = "{new}_se_unw".format(new=indicator["name"])
df = df.rename(rename_dict, axis="columns")
df.to_parquet("s3://castelnajac-open-data/covidstudy/", partition_cols=["date"])
