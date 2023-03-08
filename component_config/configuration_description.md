1. Application Authorization   
        - By authorizing the application, KBC will safely communicate with QuickBooks API to handle the final authorization for API requests.  
           
2. Company ID   
        - To obtain Company ID:  
            QuickBooks Login -> Settings(Top right corner) -> "Account and Setting" -> "Billing & Subscription"  
    **Note: Please do not input and skip the spaces in between the Company ID**   

3. Data Request   
    - User has to specify the application's endpoint
    - **Note: Please ensure signed-in user has the required privileges to access the endpoints**

    #### Constraints
    - Quickbooks Extractor is unable to parse the listed reports below generically. The JSON returns of the requested reports will be output as *ONE* cell. The priamry key for these tables are start_date and end_date which enable users to run the component incrementally.
    - Records cannot parse
        1. CashFlow
        2. GeneralLedger
        3. ProfitAndLossDetail
        4. TransactionList
        5. TrialBalance
    
4. Date Parameters
    - If start_date and end_date are not specified, component will request the API with the endpoint's default parameter, Fiscal Year to Date
    - Required format: YYYY-MM-DD

### Docker Version: 0.2.0 ###
