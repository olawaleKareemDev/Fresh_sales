from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework import status
from .serializers import ContactSerializers
import os


from dotenv import read_dotenv




class ContactViewSet(viewsets.ViewSet):

    

    def create_contacts(self, request):

       
        try:

            # Todo
                # get the CSV from the mifos service
                # update it one after the other

            data = {
                'db_host': os.environ.get('MIFOS_DB_USER'),
                'os_version': os.environ
            }

       
            data= {
                "Status": "SUCCESS",
                "Message": data
            }
            return Response(status=status.HTTP_200_OK, data=data)
        except Exception as e:
            error_data= {
                "Status": "FAILED",
                "Message": e
            }