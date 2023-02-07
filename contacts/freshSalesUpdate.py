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


    def create_contacts(self, size):

        url = f'{self.BASEURL}/contacts'
        url_upsert = f'{self.BASEURL}/contacts/upsert'

        is_data, data, total_data_size = DataExtraction().cleanData(size) 

        if not is_data:
            return False, {'status':'FAIL', 'Message':data}

            pass


        try:
            print('i am here')
            print(data)
            print(len(data), 'This is the number of data to processed')
            count = 0


            for i in range(len(data)):

                payload = {"contact":data[i]}
                res = requests.post(url,headers=self.headers,json= payload)

                # print(res.status_code)
                # print(res.json())

                if res.status_code==400 and 'already exists' in res.json()['errors']['message'][0]:
               
                    print(f'updating existing records {count}')
                    count += 1

                    update_payload ={ "unique_identifier": {"emails": payload['contact']['emails']} }
                    payload['contact'].pop('emails')
                                
                    update_payload = {**update_payload, **payload }    

                    update_res = requests.post( url_upsert, headers=self.headers, json=update_payload)
                    # print(update_res.content)
                    # print(update_res.status_code)
       

            return True, {'status':'SUCCESS', 'Message':f'Successfully loaded {len(data)} contact data out of {total_data_size}'}

        except Exception as e:
            print(e)
            return False, {'status':'FAIL', 'Message':'Failed to load data'}
          

if __name__ == "_main_":
    crm_service = UpdateFreshSales()
    crm_service.create_contacts()
