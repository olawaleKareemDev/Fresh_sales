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

        key = request.headers.get('key')

        if key == os.environ.get('API_KEY'):
       
   
            print('begin data loading')
        
            is_data, data = UpdateFreshSales().create_contacts()

            if is_data:
                return Response(status=status.HTTP_200_OK, data=data)
            if not is_data:
                return Response(status=status.HTTP_400_BAD_REQUEST, data=data)


        else:

            key_error_data= {
                        "Status": "FAILED",
                        "Message": "Invalid API key. Contact data team to resolve"
                    }
            return Response(status=status.HTTP_400_BAD_REQUEST, data=key_error_data)