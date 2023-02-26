from django.urls import path
from .views import ContactViewSet, index


urlpatterns =[

    path('freshsales-load-contacts-cooperate', ContactViewSet.as_view({
        'get': 'create_cooperate'
    }), name='freshsales-load-contacts-cooperate'),

    path('service-update-contacts-cooperate', ContactViewSet.as_view({
        'get': 'update_client_cooperate'
    }), name='service-update-contacts-cooperate'),


    path('service-update-contacts-individual', ContactViewSet.as_view({
        'get': 'update_client_individual'
    }), name='service-update-contacts-individual'),


    path('freshsales-load-contacts-individual', ContactViewSet.as_view({
        'get': 'create_individual'
    }), name='freshsales-load-contacts-individua'),

    path('index',  index,  name= "index")


]