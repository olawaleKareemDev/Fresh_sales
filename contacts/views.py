from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework import status
from .serializers import ContactSerializers
import os

from django.http import HttpResponse
from .dataServices import DataExtraction
from .tasks import sleepy, send_email_task, send_email_task_now
from .freshSalesUpdate import UpdateFreshSales

from contacts import models as contact_models
from contacts import utils as contact_utils
from datetime import date, timedelta, datetime

from contacts import redshift_integration

# # test celery
def index(request):
    sleepy.delay(10)
    # try:
    #     send_email_task_now()
    # except Exception as e:
    #     print(e)
    return HttpResponse('<h1>The email is sent</h1>')


class ContactViewSet(viewsets.ViewSet):

    def update_client_cooperate(self,request):
        key = request.headers.get('key')

        batch = int(request.headers.get('batch'))

        if not batch:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify data upload batch' }
            )
        
        if  batch not in [1,2,3,4,5]:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify correct batch type from 1-5' }
            )

        if key == os.environ.get('API_KEY'):

            # check when last MIFOS was pinged
            # is_allow, data = contact_utils.time_checker.check_last_update_cooperate_data()

            # if not is_allow:
            #     return Response(status=status.HTTP_400_BAD_REQUEST, data=data)


            print('begin data update')
            is_data, data = DataExtraction().get_cooperate_data(batch=batch) 

            if is_data:

                try:

                    if contact_models.ContactUpdateHolderCooperate.objects.filter(batch=batch).exists():
                        cooperate_data = contact_models.ContactUpdateHolderCooperate.objects.get(batch=batch)
                        cooperate_data.contact_load_cooperate_client = data
                        cooperate_data.save()

                    else:
                        cooperate_data  = contact_models.ContactUpdateHolderClientIndividual.objects.create(
                            batch=batch,
                            contact_load_cooperate_client = data
                        )

                    # cooperate_data = contact_models.ContactUpdateHolderCooperate.objects.all()
                    # check = False


                    # if cooperate_data.count() == 0 and check == False:
                    #     print('updating business contact for the first time')
                    #     c_contacts = contact_models.ContactUpdateHolderCooperate.objects.create(contact_load_cooperate_client = data)
                    #     c_contacts.save()

                    # if cooperate_data.count() > 0 and check == False:
                    #     print('updating existing business contacts')
                    #     c_contacts = contact_models.ContactUpdateHolderCooperate.objects.all()[0]
                    #     c_contacts.contact_load_cooperate_client = data
                    #     c_contacts.updated_on = datetime.now()  ###
                    #     c_contacts.save()

                    returned_data = {
                        'status': 'SUCCESS',
                        'messages': 'Successfully uploaded cooperate client data',
                        'data count': len(data)
                    }

                    return Response(status=status.HTTP_200_OK, data=returned_data)

                except Exception as e:
                    return Response(
                        status=status.HTTP_400_BAD_REQUEST, 
                        data={'status': 'Failed', 'messages': 'Error occured while creating data' }
                    )

            if not is_data:
                return Response(status=status.HTTP_400_BAD_REQUEST, data=data)


    def update_client_individual(self,request):

        print('in individual')

        key = request.headers.get('key')
        batch = int(request.headers.get('batch'))

        if not batch:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify data upload batch' }
            )
        
        if  batch not in [i for i in range(6)]:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify correct batch type from 1-5' }
            )


        if key == os.environ.get('API_KEY'):

            # check when last MIFOS was pinged
            # is_allow, msg = contact_utils.time_checker.check_last_update_individual_data()

            # print(is_allow, msg)

            # if not is_allow:
            #     return Response(status=status.HTTP_400_BAD_REQUEST, data=msg)

            print('begin data update')
          

            is_data, data = DataExtraction().get_individual_data(batch=batch) 


            if is_data:

                try:
                    if contact_models.ContactUpdateHolderClientIndividual.objects.filter(batch=batch).exists():
                        retail_data = contact_models.ContactUpdateHolderClientIndividual.objects.get(batch=batch)
                        retail_data.contact_load_individual_client = data
                        retail_data.save()

                    else:
                        retail_data  = contact_models.ContactUpdateHolderClientIndividual.objects.create(
                            batch=batch,
                            contact_load_individual_client = data
                        )
                        
                        
                    #     check = False

                    # # check for batches and load data accordingly

                    # if retail_data.count() == 0 and check == False:
                    #     print('updating individual contact for the first time')
                    #     try:
                    #         i_contacts = contact_models.ContactUpdateHolderClientIndividual.objects.create(contact_load_individual_client = data)
                    #         print('done creating')
                    #         # i_contacts.save()
                    #     except Exception as e:
                    #         print(e)
                    #         return Response(
                    #                 status=status.HTTP_400_BAD_REQUEST, 
                    #                 data={'status': 'Failed', 'messages': 'Error occured while creating data' }
                    #             )
                            
                    #         pass

                    #     check = True

                    # if retail_data.count() > 0 and check == False:
                    #     print('updating already existing individual ')
                    #     i_contacts = contact_models.ContactUpdateHolderClientIndividual.objects.all()[0]
                    #     i_contacts.contact_load_individual_client = data
                    #     i_contacts.save()
                    #     print('done updating individual contacts')

                    data = {
                            'status': 'SUCCESS',
                            'Message': 'Individual contacts loaded successfully',
                            'data': f'{len(data)} individual contacts'
                         }

                    return Response(status=status.HTTP_200_OK, data=data)
                        

                except Exception as e:
                    print(e)
                    return Response(
                        status=status.HTTP_400_BAD_REQUEST, 
                        data={'status': 'Failed', 'messages': 'Error occured while creating data' }
                    )

            if not is_data:
                return Response(status=status.HTTP_400_BAD_REQUEST, data=data)
    

    def create_cooperate(self, request):

        print('lodaing cooperate data into freshsales ')


        key = request.headers.get('key')
        size = int(request.headers.get('size'))
        batch = int(request.headers.get('batch'))

        if not batch:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify data upload batch' }
            )
        
        if  batch not in [i for i in range(5)]:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify correct batch type from 1-5' }
            )

        if not size >= 500 :
            size_error_data= {
                        "Status": "FAILED",
                        "Message": "Only process data starting from 500 upward"
                    }
            return Response(status=status.HTTP_400_BAD_REQUEST, data=size_error_data)

        if key != os.environ.get('API_KEY'):
            key_error_data= {
                        "Status": "FAILED",
                        "Message": "Invalid API key. Contact data team to resolve"
                    }
            return Response(status=status.HTTP_400_BAD_REQUEST, data=key_error_data)

       
   
        print('begin data loading')
    
        # cooperate_data = contact_models.ContactUpdateHolderCooperate.objects.all()[0]
        cooperate_data = contact_models.ContactUpdateHolderCooperate.objects.get(batch=batch)
        is_data, data = UpdateFreshSales().create_contacts(
            data=cooperate_data.contact_load_cooperate_client,
            size=size
            )

        if is_data:
            return Response(status=status.HTTP_200_OK, data=data)
        if not is_data:
            return Response(status=status.HTTP_400_BAD_REQUEST, data=data)


    def create_individual(self, request):
        print('begin loading individual contracts')

        key = request.headers.get('key')
        size = int(request.headers.get('size'))
        batch = int(request.headers.get('batch'))

        if not batch:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify data upload batch' }
            )
        
        if  batch not in [i for i in range(5)]:
            return Response(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'status': 'FAIL', 'messages': 'Specify correct batch type from 1-5' }
            )

        if not size >= 500 :
            size_error_data= {
                        "Status": "FAILED",
                        "Message": "Only process data starting from 500 upward"
                    }
            return Response(status=status.HTTP_400_BAD_REQUEST, data=size_error_data)

        if key != os.environ.get('API_KEY'):
            key_error_data= {
                        "Status": "FAILED",
                        "Message": "Invalid API key. Contact data team to resolve"
                    }
            return Response(status=status.HTTP_400_BAD_REQUEST, data=key_error_data)

       
   
        print('begin data loading')
    
        # cooperate_data = contact_models.ContactUpdateHolderClientIndividual.objects.all()[0]
        cooperate_data = contact_models.ContactUpdateHolderClientIndividual.objects.get(batch=batch)
        is_data, data = UpdateFreshSales().create_contacts(
            data=cooperate_data.contact_load_individual_client,
            size=size
            )

        if is_data:
            return Response(status=status.HTTP_200_OK, data=data)
        if not is_data:
            return Response(status=status.HTTP_400_BAD_REQUEST, data=data)





