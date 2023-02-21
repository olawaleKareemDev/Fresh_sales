from .dataServices import DataExtraction
import requests
import os


class UpdateFreshSales:

    admin_key = os.environ.get("ADMIN_KEY"),

    headers = {
     "content-type": "application/json", 
     "accept": "application/json",
     "Authorization": "Token token=o4gqiTzbjoztYNA0jzKong"
    }

    BundleAlias ="vfdmicrofinancebank.myfreshworks.com/crm/sales"
    BASEURL = f"https://{BundleAlias}/api/"

    def __init__(self) -> None:
        # self.BASEURL = 
        pass


    def create_contacts(self, data, size):

        url = f'{self.BASEURL}/contacts'
        url_upsert = f'{self.BASEURL}/contacts/upsert'

        # print(data, len(data))

        if size == 500:
            print('data within 500 df')
            test_data = data[:500]
            starting_point =1
        else:
            print('data beyond 500 df')
            if  size <= len(data):
                val, rem = divmod(size, 500)
                if val == 1:
                    test_data = data[500:size]
                    starting_point = 500
                if val > 1:
                    starting_point =500 * (val-1)
                    test_data = data[starting_point:size]

            else:
                return False, f"data out of scope. This is the number of data currently available {len(data)}"




        try:

            print(len(test_data), 'This is the number of test_data to processed')
            newly_created = 0
            count = starting_point


            for i in range(len(test_data)):

                payload = {"contact":test_data[i]}
                res = requests.post(url,headers=self.headers,json= payload)

                if res.status_code==200:
                    newly_created += 1
                    print(f'Created {newly_created} records')

                if res.status_code==400 and 'already exists' in res.json()['errors']['message'][0]:
               
                    print(f'updating existing records {count}')
                    count += 1

                    update_payload ={ "unique_identifier": {"emails": payload['contact']['emails']} }
                    payload['contact'].pop('emails')
                                
                    update_payload = {**update_payload, **payload }    

                    update_res = requests.post( url_upsert, headers=self.headers, json=update_payload)
       

            return True, {'status':'SUCCESS', 'Message':f'Successfully loaded {len(test_data)} contact data out of {len(data)}', 'next_count': f'{size + 1}'}

        except Exception as e:
            print(e)
            return False, {'status':'FAIL', 'Message':'Failed to load data', 'next_count': 'None'}
          

if __name__ == "_main_":
    crm_service = UpdateFreshSales()
    crm_service.create_contacts()
