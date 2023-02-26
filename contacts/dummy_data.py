import sys

class DummyData:

    def generate_individual_client_data(self):


        d_data = {

            'first_name': "",  
            'last_name': ' ',
            'mobile_number':"",
            'emails':"",

            'custom_field':{
                'cf_client_id': "", 
                'cf_loan_value':"",
                'cf_product_name':"",  
                'cf_account_balance':"",
                'cf_account_officer':"", 
                'cf_account_type':"",
                'cf_account_no':"",
                'cf_maturity_date':"",
                'cf_total_credit_amount':"",
                'cf_total_balance_all_accounts':"",
                'cf_debit_balance':"",

                ## newly
                'cf_last_repayment_date':"",
                'cf_occupation':"",
                'cf_date_of_birth':"",
                'outstanding_loan_amount':"",

            }
            
            }
            
            
        # data = [d_data for i in range(462860)]
        data = [d_data for i in range(500000)]
        # return_data = {'data':data}
        print('The dummy individual data size: ', sys.getsizeof(data))

        # return return_data 
        return data 


    def generate_cooperate_client_data(self):


        d_data = {

            'first_name': "",  
            'last_name': ' ',
            'mobile_number':"",
            'emails':"",

            'custom_field':{
                'cf_client_id': "", 
                'cf_loan_value':"",
                'cf_product_name':"",  
                'cf_account_balance':"",
                'cf_account_officer':"", 
                'cf_account_type':"",
                'cf_account_no':"",
                'cf_maturity_date':"",
                'cf_total_credit_amount':"",
                'cf_total_balance_all_accounts':"",
                'cf_debit_balance':"",

                ## newly
                'cf_last_repayment_date':"",
                'cf_occupation':"",
                'cf_date_of_birth':"",
                'outstanding_loan_amount':"",

            }
            
            }
            
            
        data = [d_data for i in range(462860)]
        print('The dummy cooperate data size: ', sys.getsizeof(data))

        return data
    


dummydata = DummyData()