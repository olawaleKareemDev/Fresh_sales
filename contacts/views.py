from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework import status
from .serializers import ContactSerializers
import os

from django.http import HttpResponse
from .dataServices import DataExtraction
from .tasks import sleepy, send_email_task, send_email_task_now
from .freshSalesUpdate import UpdateFreshSales


# test celery
def index(request):
    sleepy.delay(10)
    # try:
    #     send_email_task_now()
    # except Exception as e:
    #     print(e)
    return HttpResponse('<h1>The email is sent</h1>')


class ContactViewSet(viewsets.ViewSet):

    

    def create(self, request):

       
        try:

            # try:   
            #     data = DataExtraction().cleanData() 
            # except Exception as e:
            #     print(e)
            #     data= 'fake data'
       
            # data= {
            #     "Status": "SUCCESS",
            #     "Message": str(data)
            # }

            try:   
                data = UpdateFreshSales().create_contacts()
            except Exception as e:
                print(e)
                data= 'fake data'

            return Response(status=status.HTTP_200_OK, data=data)
        except Exception as e:
            error_data= {
                "Status": "FAILED",
                "Message": e
            }