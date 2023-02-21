from contacts import models as contact_models
from datetime import date, timedelta, datetime

import pytz





class PingChecker:

    def check_last_update_cooperate_data(self):
        utc=pytz.UTC
        cooperate_data = contact_models.ContactUpdateHolderCooperate.objects.all()

        if cooperate_data.count() == 0:
            return True, "MIFOS queries allowed. first time seed up"

        if cooperate_data.count() >  0:
            data = contact_models.ContactUpdateHolderCooperate.objects.all()[0]
            update_time = data.updated_on
            next_allowable_update_time = update_time + timedelta(hours=4)
            time_now = pytz.utc.localize(datetime.now())

            if next_allowable_update_time <= time_now:
                return True, "MIFOS queries allowed"

            if next_allowable_update_time > time_now:
                wait_hour = abs(next_allowable_update_time - time_now)
                # type(wait_hour)
                # print(wait_hour.seconds//3600) 

                data= {
                    'status': 'FAILED',
                    'message': f'Cannot update cooperate data from MIFOS now. Next update at {wait_hour} ,{wait_hour.seconds//3600} hrs from now '
                    }

                return False,  data


    def check_last_update_individual_data(self):
        utc=pytz.UTC
        cooperate_data = contact_models.ContactUpdateHolderClientIndividual.objects.all()

        if cooperate_data.count() == 0:
            return True, "MIFOS queries allowed. first time seed up"

        if cooperate_data.count() >  0:
            data = contact_models.ContactUpdateHolderClientIndividual.objects.all()[0]
            update_time = data.updated_on
            next_allowable_update_time = update_time + timedelta(hours=4)
            time_now = pytz.utc.localize(datetime.now())

            if next_allowable_update_time <= time_now:
                return True, "MIFOS queries allowed"

            if next_allowable_update_time > time_now:
                wait_hour = abs(next_allowable_update_time - time_now)

                data= {
                    'status': 'FAILED',
                    'message': f'Cannot update individual data from MIFOS now. Next update at {wait_hour} ,{wait_hour.seconds//3600} hrs from now '
                    }

                return False,  data




time_checker = PingChecker()