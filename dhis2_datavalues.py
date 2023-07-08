
import asyncio
import json
from requests.auth import HTTPBasicAuth
import requests
from datetime import datetime

# Addresses
BASE_URL = 'http://35.247.172.252:8092/'

# authentication
username = 'admin'
password = 'district'

headers = {
'Content-type': 'application/json',
'Authorization': 'Basic YWRtaW46ZGlzdHJpY3Q='
}

async def getDataSets(dataSetIds):
    print("NNNN")
    url = BASE_URL + 'api/dataSets.json?fields=id,name,code,periodType,dataSetElements[dataElement[id,name,code,categoryCombo[categoryOptionCombos[id,name,code]]]]&paging=false&filter=id:in:[' + ".".join(dataSetIds) +']'
    response = requests.get(url, auth=(username,password), verify=False)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

async def formulate_data_values(dataSetElements, dataRowsFromSQL):
    dataValues = []
    for dataSetElement in dataSetElements:
        for categoryOptionCombo in dataSetElement["dataElement"]["categoryCombo"]["categoryOptionCombos"]:
            if "code" in categoryOptionCombo and "code" in dataSetElement["dataElement"]:
                value = await get_value(dataRowsFromSQL,categoryOptionCombo["code"])
                dataValue = {
                    "dataElement": dataSetElement["dataElement"]["code"],
                    "categoryOptionCombo": categoryOptionCombo["code"],
                    "value": value
                }
                dataValues.append(dataValue)
    return dataValues

async def get_value(dataRowsFromSQL, code):
    value = 0
    for dataRow in dataRowsFromSQL:
        if code in dataRow:
            value = dataRow[code]
    return value

async def send_data_to_dhis2(payload):
    response = requests.post(BASE_URL + 'api/dataValueSets.json?idScheme=code&async=true', headers=headers, data=json.dumps(payload))
    return json.loads(response.content.decode('utf-8'))


async def main():
    dataSetsIds = ["svi130bL2zK"]
    dataSetsMetadata = await getDataSets(dataSetsIds)
    if "dataSets" in dataSetsMetadata:
        for dataSet in dataSetsMetadata["dataSets"]:
            print(dataSet)
            # Execute query for this dataset
            dataRowsFromSQL = [{
                "OBS_GYNE_M": 3,
                "PED_M": 2
            }]
            dataValues = await formulate_data_values(dataSet["dataSetElements"],dataRowsFromSQL)
            payload = {
                        "dataSet": dataSet["code"],
                        "completeDate": datetime.today().strftime('%Y-%m-%d'),
                        "period": 20230601,
                        "orgUnit": "DISTRICT",
                        "dataValues": dataValues
                    }
            print(json.dumps(payload))
            # Send data to DHIS2
            response = await send_data_to_dhis2(payload)

asyncio.run(main())