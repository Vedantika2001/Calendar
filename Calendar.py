# calendar.py

from Connection import read_sheet, write_sheet
from NSE_Nifty_Weekly_Expiry import apply_weekly_expiry
from NSE_Nifty_Montly_Expiry import apply_nifty_monthly_expiry
from NSE_BankNifty_Weekly_Expiry import apply_banknifty_weekly_expiry
from NSE_BankNifty_Monthly_Expiry import apply_banknifty_monthly_expiry
from Trading_Day import update_trading_day
from NiftyFifty_Close_Price import NiftyFifty_Close_Price
from Close import update_today_nifty_close
# Step 1: Read Google Sheet
sheet_name = "Calendar"     # Replace with your actual sheet name
worksheet_name = "sample3"                 # Replace with your sheet tab name
close_sheet_name = "sample2"
df, sheet_main = read_sheet(sheet_name, worksheet_name)
df_close, _ = read_sheet(sheet_name, close_sheet_name, expect_calendar_date=False)

# df, sheet = read_sheet(sheet_name, worksheet_name)

# Step 2: Apply both expiry logic functions
df = update_today_nifty_close()
df = NiftyFifty_Close_Price(df, df_close)
df = update_trading_day(df)  # 🔹 Use the function here

df = apply_weekly_expiry(df)
df = apply_nifty_monthly_expiry(df)
df = apply_banknifty_weekly_expiry(df)
df = apply_banknifty_monthly_expiry(df)


# Step 3: Write updated DataFrame back to Google Sheet
write_sheet(df, sheet_main)

print("✅ Weekly & Monthly Expiry Columns Updated Successfully!")
