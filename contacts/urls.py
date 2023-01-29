from django.urls import path
from .views import ContactViewSet, index


urlpatterns =[
    path('upload-contacts', ContactViewSet.as_view({
        'get': 'create'
    })),
    path('index', index, name= "index")


]