from .dataServices import DataExtraction
import requests


class UpdateFreshSales:

    def __init__(self) -> None:
        pass


    def updateFreshSalesContacts(self):
        pass

    def create_contacts(self):

        url = f'{self.BASEURL}/contacts'
        url_upsert = f'{self.BASEURL}/contacts/upsert'

        # payload = {"contact":{  
        #                         "first_name":"test10",
        #                         "last_name":"check10 (sample)",
        #                         "emails":"test10@gmail.com",
        #                         "mobile_number":"1-926-555-9777"
        #                     }
        #          } 

        # print(payload['contact'])

        try:   
            data = DataExtraction().cleanData() 
        except Exception as e:
            print(e)
            data= 'fake data'

        try:
            for i in range(len(data)):
                payload = {"contact":data[i]}
                res = requests.post(
                    url,
                    headers=self.headers,
                    json= payload
                    )

                print(res.status_code)
                print(res.json())

                if res.status_code==400 and 'already exists' in res.json()['errors']['message'][0]:

                    update_payload ={ "unique_identifier": {"emails": payload['contact']['emails']} }
                    payload['contact'].pop('emails')
                                
                    update_payload = {**update_payload, **payload }    

                    update_res = requests.put( url_upsert, headers=self.headers, json=update_payload)
                    print(update_res.content)
                    print(update_res.status_code)

                if i == 0:
                    break

            return {'status':'SUCCESS', 'Message':'Successfully loaded data'}

        except Exception as e:
            print(e)
          


if __name__ == "_main_":
    crm_service = UpdateFreshSales()
    crm_service.create_contacts()
