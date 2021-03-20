import requests
import json


def formatJson(results):
    content = ""
    for record in results["data"]:
        content = "{content}{record}\n".format(content=content, record=record)

    return content


def printJson(results):
    print(json.dumps(results, indent=4, sort_keys=True))


class Client:

    def __init__(self, url="https://covidmap.umd.edu"):
        self.url = url

    def get_countries(self):
        return json.loads(requests.get("{url}/api/country".format(url=self.url)).text)

    def get_regions(self):
        return json.loads(requests.get("{url}/api/region".format(url=self.url)).text)

    def get_dates(self, country=None, region=None):
        assert country
        if region is None:
            return json.loads(requests.get("{url}/api/datesavail?country={country}".format(
                url=self.url,
                country=country,
            )).text)
        else:
            return json.loads(requests.get("{url}/api/datesavail?country={country}&region={region}".format(
                url=self.url,
                country=country,
                region=region,
            )).text)

    def get_indicator(self, indicator, type, country, region=None, daterange=None, formatter=None):
        assert daterange
        if region is None:
            results = json.loads(requests.get("{url}/api/resources?indicator={indicator}&type={type}&country={country}&daterange={daterange}".format(
                url=self.url,
                indicator=indicator,
                type=type,
                country=country,
                daterange=daterange,
            )).text)
        else:
            results = json.loads(requests.get("{url}/api/resources?indicator={indicator}&type={type}&country={country}&region={region}&daterange={daterange}".format(
                url=self.url,
                indicator=indicator,
                type=type,
                country=country,
                daterange=daterange,
                region=region,
            )).text)
        if formatter is None:
            return results
        else:
            return formatter(results)
