## IMPORTS 
import requests 
from packages_imports import *
import requests
import datetime
import json
import pandas as pd
from packages_imports import *
# from pandas import 
from big_query_credentials import *
from datetime import datetime, timedelta


##FETCHING TODAYS_DATE 
now = datetime.now()
today_date = (now.strftime('%d-%m-%Y'))

## FOR ANY APPLICATIONS THAT REQUIRES TO SEND SPECIFIC CUSTOMERS OF AN APPLICATION COWIN SLOT AVAILIBILTY ON THE BASIS OF DISTRICT_ID and MIN AGE
### FOR ANY USE CASE GET CUSTOMERID , DISTRICTID FOR USERS 

##EXAMPLE OF DATAFRAME WHICH CAN RETRIVED FOR AN APPLICATION WITH CUSTOMERS  WHERE CUSTOMERS PROVIDE 
1 DISTRICT_ID
2 AGE FOR WHICH THEY WANT VACCINE (AVAILABLE VALUES ARE AS UNDER 18+,45+)
## EXAMPLE OF A DF WHICH HAS ABOVE VALUES FOR A PARTICULAR CUSTOMER OF AN APPLICATION
df = pd.DataFrame({"customerId":[1234567,234567],"district_id":[392,395],"age":[18+,45+]})


## UNIQUE DISTRICTS OF CUSTOMERS WHOSE VALUE WE NEED TO CHECK 
district_id_list = list(df.district_id.astype(int).unique())

## OVERALL LOGIC 
final_df = pd.DataFrame({})
start_time = time.time()
api_hits = 0
for i in district_id_list:
    print(i)
    headers  = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36 Edg/90.0.818.51','Content-type':'application/json'}
    try:
        ##HITING THE API FOR ALL DISTRICTS ID THAT ARE NEEDED AND TODAY'S DATE GIVES SLOTS DATA UPTO NEXT 7 DAYS
        response = requests.get("https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id={0}&date={1}".format(i,today_date),headers = headers)
    except Exception as err:
        print(err)
    print("error if any ")
    print(response.status_code)
    
    if response.status_code !=200:
        sleep(1)
        continue
        print("api not working")
        ##send mail if api stops working 
        ## HERE SEND_MAIL IS A FUNCTION THAT CAN USED TO SEND MAIL IN CASE API FAILS 
        #send_mail("COWIN api not working (status code != 200) and status_code is {0} ".format(response.status_code),"Cowin api status ")
    
    json_data = json.loads(response.text)

        
    api_hits += 1
    
    ### we have  A LIMIT OF 100 REQUESTS WITHIN 5 MINS SO ON REACHNG 90 HITS WE USE SLEEP FOR 3 MINUTES 
    if (len(district_id_list)) >= 100 and api_hits == 90:
        sleep(180)
    
    dd = json_data['centers']
    if len(dd)== 0:
        continue
    df2 = pd.DataFrame.from_records(dd)
    ##FETCHING SESSIONS DATA WITH AVAILABILITY  ,MIN AGE ETC
    df3 = pd.concat([pd.DataFrame(x) for x in df2['sessions']], keys=df2['name']).reset_index(level=1, drop=True).reset_index()
    ## Getting available capacity greater than zero
    df3_filter = df3[df3.available_capacity >= 1]
    ## GETTING MIN DATE OF SLOT AVAILIBILITY FOR TWO VALUES (18+,45+)
    age_45_date = df3_filter.date.min()
    age_18_date = df3_filter[df3.min_age_limit >=18].date.min()
    main_df = pd.DataFrame({"district_id":i,"age18":age_18_date,"age45":age_45_date},index = range(1,2))
    end = pd.concat([main_df,final_df])
    final_df = end
#     sleep(2)
print(final_df)

## HERE FINAL DATA IS IN DATAFRAME AND THEN CAN BE USED TO SEND DATA 



##THIS PART MERGERS SLOT DATA AND CUSTOMERID SO THAT IT CAN USED TO SEND DATA TO CUSTOMERS OF AN APPLICATION

main_merge = pd.merge(all_df2,final_df,how = 'left', on = 'district_id')


## MAIN MERGE WILL BE AS UNDER  AS EVERY CUSTOMER WITH ITS DISTRICT_ID,MIN AGE, 
##AGE18- MIN DATE FOR SLOT AVAAILIBILITY FOR THIS DISTRICT FOR 18+
##AGE45- MIN DATE FOR SLOT AVAAILIBILITY FOR THIS DISTRICT FOR 45 +

# OVERALL OUTPUT IS IN TABULAR FORMAT WHICH IS NOT JUST MORE EASIERTO UNDERSTAND BUT CAN BE SHARED EASILY WITH CUSTOMERS OF AN APPLICATIONS AS PER CUSTOMERID
#IN FORM OF PUSH NOTIFICATION OR MESSAGE
user_id	     district_id	  age   age18	      age45
0	12345678	  775	           18+	08-05-2021	08-05-2021
1	3456789	    188	           18+	08-05-2021	08-05-2021


