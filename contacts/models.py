from django.db import models
import uuid
from datetime import date

# Create your models here.

class ContactUpdateHolderCooperate(models.Model):
    id = models.UUIDField(
        help_text="Unique cooperate contact batch identifier",
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    contact_load_cooperate_client = models.JSONField(default=dict)
    update_count = models.IntegerField(default=0)
    added_on = models.DateTimeField(auto_now_add=True)
    updated_on = models.DateTimeField(auto_now=True)
    batch = models.IntegerField(default = 1, unique=True)


class ContactUpdateHolderClientIndividual(models.Model):
    id = models.UUIDField(
        help_text="Unique individual contact batch identifier",
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    contact_load_individual_client = models.JSONField(default=dict)
    update_count = models.IntegerField(default=0)
    added_on = models.DateTimeField(auto_now_add=True)
    updated_on = models.DateTimeField(auto_now=True)
    batch = models.IntegerField(default = 1, unique=True)







