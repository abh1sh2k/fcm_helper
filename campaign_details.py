import json
from pprint import pprint

if __name__ == '__main__':
    with open('ribbon_payload.json') as data_file:
        payload_data = json.load(data_file)

        json_temp = {"description": "engagement campaign test", "name": "engagement campaign test", "campaignid": "10000"}
        campaignmethodid = 10000
        with open('campaign_details_s3.txt','w') as output_file:
            while campaignmethodid  < 10020 :
                payload = payload_data[str(campaignmethodid)]['payload']
                json_temp['campaignmethodid'] = campaignmethodid
                json_temp['payload'] = json.dumps(payload)
                pprint(json_temp)
                output_file.write('%s\n'%json.dumps(json_temp))
                campaignmethodid +=1