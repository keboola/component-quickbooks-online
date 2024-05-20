from src.report_mapping import ReportMapping
import json

with open('../data/out/tables/ProfitAndLoss.json', 'r') as f:
    data = json.load(f)
    ReportMapping(endpoint='ProfitAndLoss', data=data, accounting_type="accrual")
