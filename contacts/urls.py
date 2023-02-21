from django.urls import path
from .views import ContactViewSet, index


urlpatterns =[

    path('freshsales-load-contacts-cooperate', ContactViewSet.as_view({
        'get': 'create_cooperate'
    })),

    path('service-update-contacts-cooperate', ContactViewSet.as_view({
        'get': 'update_client_cooperate'
    })),


    path('service-update-contacts-individual', ContactViewSet.as_view({
        'get': 'update_client_individual'
    })),


    path('freshsales-load-contacts-individual', ContactViewSet.as_view({
        'get': 'create_individual'
    })),

    path('index',  index,  name= "index")


]