# Generated by Django 4.1.5 on 2023-02-25 23:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('contacts', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='contactupdateholderclientindividual',
            name='contact_load_individual_client',
            field=models.JSONField(default=dict),
        ),
        migrations.AlterField(
            model_name='contactupdateholdercooperate',
            name='contact_load_cooperate_client',
            field=models.JSONField(default=dict),
        ),
    ]
