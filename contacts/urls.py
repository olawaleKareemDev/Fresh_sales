from django.urls import path
from .views import ContactViewSet


urlpatterns =[
    path('upload-contact', ContactViewSet.as_view({
        'get': 'create_contacts'
    }))
]