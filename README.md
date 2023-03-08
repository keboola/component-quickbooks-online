# README #

This repository is a collection of configurations needed to register Keboola Generic Extractor as a branded QuickBooks KBC Extractor.
Extractor's task is to help user to extract the data from QuickBooks online to Keboola Connection Platform (KBC). 

## API documentation ##
[QuickBooks API documentation](https://developer.intuit.com/docs/0100_quickbooks_online/0300_references/0000_programming_guide/0000_rest_api_quick_reference)  

## Configuration ##
  
  1. Application Authorization   
        - By authorizing the application, KBC will safely communicate with QuickBooks API to handle the final authorization for API requests.  
           
  2. Company ID   
        - To obtain Company ID:  
            QuickBooks Login -> Settings(Top right corner) -> "Account and Setting" -> "Billing & Subscription"  
    **Note: Please ignore the spaces in between the Company ID**   

  3. Data Request   
        - User has to specify the application's endpoint
    **Note: Please ensure signed-in user has the required privileges to access the endpoints**

  4. Date Parameters
        - If start_date and end_date are not specified, component will request the API with the endpoint's default parameter, Fiscal Year to Date
        - Required format: YYYY-MM-DD

## Available Endpoints: ##
        
### Accounting Endpoints ###
        1. Account
        2. Bill
        3. BillPayment
        4. Budget
        5. Class
        6. Customer
        7. Deposit
        8. Invoice
        9. Item
        10. JournalEntry
        11. Payment
        12. Purchase
        13. PurchaseOrder
        14. TaxCode
        15. TaxRate
        16. Transfer
        17. Vendor
### Report Endpoints ###
        1. BalanceSheet
        2. CashFlow
        3. GeneralLedger
        4. ProfitAndLoss
        5. ProfitAndLossDetail
        6. TransactionList
        7. TrialBalance

### Constraints ##
        - Quickbooks Extractor is unable to parse the listed reports below generically. The JSON returns of the requested reports will be output as *ONE* cell. The priamry key for these tables are start_date and end_date which enable users to run the component incrementally.
        - Records cannot parse:
            1. CashFlow
            2. GeneralLedger
            3. ProfitAndLossDetail
            4. TransactionList
            5. TrialBalance

### Accounting Types ##
        - Based on different business models, some clients are required to report on differnet accounting types: Cash or Accrual.
        - For reports below, component will perform 2 requests with 1 request against cash accounting type while the other against accrual accounting type
            1. BalanceSheet
            2. GeneralLedger
            3. ProfitAndLoss
            4. ProfitAndLossDetail
    

## Support ##
If the component is missing the endpoints or reports you are looking for, please submit a support ticket or feel free to contact me directly. 
         

## Contact Info ##
Leo Chan  
Vancouver, Canada (PST time)   
Email: leo@keboola.com  
Private: cleojanten@hotmail.com   