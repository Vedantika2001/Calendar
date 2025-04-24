import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ─────────────────────────────────────────────────────────────
# Fetch and update the last available Nifty50 close price
# ─────────────────────────────────────────────────────────────

def update_latest_nifty_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    nifty = yf.download("^NSEI", start=start_date, end=end_date)
    if nifty.empty:
        print("⚠️ No Nifty data found.")
        return df

    valid_data = nifty[nifty.index.date < today]
    if valid_data.empty:
        print("⚠️ No past data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    # close_price = float(valid_data["Close"].iloc[-1])
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using data from {last_date}: Close = {close_price}")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "Nifty50 Close Price"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["Nifty50 Close Price"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with close price: {close_price}")

    return df

# ──────────────────────────────
# Update Trading Day Column
# ──────────────────────────────

def update_trading_day(df):
    df["Nifty50 Close Price"] = pd.to_numeric(df["Nifty50 Close Price"], errors="coerce")
    last_trading_day_date = df.loc[df["Trading Day"] == 1, "Calendar Date"].max()
    condition = df["Calendar Date"] > last_trading_day_date
    df.loc[condition, "Trading Day"] = df.loc[condition, "Nifty50 Close Price"].notna().astype(int)
    return df

# ──────────────────────────────
# NSE Nifty Weekly Expiry Logic
# ──────────────────────────────

def apply_weekly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dropna().dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    launch_date = datetime(2019, 2, 11)
    change_date = datetime(2025, 4, 4)
    end_date = df["Calendar Date"].max()

    expiry_data = []
    current_date = launch_date

    while current_date <= end_date:
        weekday_target = 3 if current_date.date() < change_date.date() else 0  # Thursday to Monday
        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7
        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_data.append(actual_expiry)
        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_data)
    df["NSE Nifty Weekly Expiry"] = df["Calendar Date"].dt.date.apply(lambda x: 1 if x in expiry_dates_set else 0)

    return df

# ───────────────────────────────────────────────
# NSE Nifty Monthly Expiry with Shift Logic
# ───────────────────────────────────────────────

def apply_nifty_monthly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"])
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dt.date)

    monthly_expiry_start_date = datetime(2000, 6, 12).date()
    expiry_shift_date = datetime(2025, 4, 4).date()

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    def get_last_weekday(year, month, weekday_target):
        if month == 12:
            first_day_next_month = datetime(year + 1, 1, 1)
        else:
            first_day_next_month = datetime(year, month + 1, 1)
        last_day = first_day_next_month - timedelta(days=1)

        while last_day.weekday() != weekday_target:
            last_day -= timedelta(days=1)

        return last_day.date()

    def get_monthly_expiry_status(date_input):
        if isinstance(date_input, pd.Timestamp):
            date = date_input.date()
        elif isinstance(date_input, str):
            try:
                date = datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                return 0
        else:
            return 0

        if date < monthly_expiry_start_date:
            return 0

        weekday_target = 3 if date < expiry_shift_date else 0  # Thursday or Monday
        scheduled_expiry = get_last_weekday(date.year, date.month, weekday_target)
        actual_expiry = get_previous_trading_day(scheduled_expiry)

        return 1 if date == actual_expiry else 0

    df["NSE Nifty Monthly Expiry"] = df["Calendar Date"].map(get_monthly_expiry_status)
    return df


# ───────────────────────────────────────────────
# NSE BankNifty Weekly Expiry with Shift Logic
# ───────────────────────────────────────────────
def apply_banknifty_weekly_expiry(df):
    trading_days = set(pd.to_datetime(df[df["Trading Day"] == 1.0]["Calendar Date"]).dt.date)

    launch_date = datetime(2016, 5, 27)
    shift_date = datetime(2023, 9, 6)
    expiry_discontinue_date = datetime(2024, 11, 13)  # Last valid expiry date

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    end_date = pd.to_datetime(df["Calendar Date"]).max()
    expiry_dates = []

    current_date = launch_date
    while current_date <= end_date:
        # Stop adding expiries after the discontinuation date
        if current_date > expiry_discontinue_date:
            break

        weekday_target = 3 if current_date.date() < shift_date.date() else 2  # Thursday (3) before shift, Wednesday (2) after
        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7

        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        # Ensure the actual expiry is on or before the discontinuation date
        if actual_expiry <= expiry_discontinue_date.date():
            expiry_dates.append(actual_expiry)

        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_dates)
    df["NSE BankNifty Weekly Expiry"] = pd.to_datetime(df["Calendar Date"]).dt.date.apply(
        lambda x: 1 if x in expiry_dates_set else 0
    )

    return df

# ───────────────────────────────────────────────
# NSE BankNifty Monthly Expiry
# ───────────────────────────────────────────────

def apply_banknifty_monthly_expiry(df):
    trading_days = set(pd.to_datetime(df[df["Trading Day"] == 1.0]["Calendar Date"]).dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    def get_last_weekday(year, month, weekday_target):
        last_day = datetime(year, month, 1) + pd.offsets.MonthEnd(0)
        while last_day.weekday() != weekday_target:
            last_day -= timedelta(days=1)
        return last_day.date()

    def get_expiry_status(date_input):
        if isinstance(date_input, pd.Timestamp):
            date = date_input.date()
        elif isinstance(date_input, str):
            try:
                date = datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                return 0
        else:
            return 0

        if date < datetime(2005, 6, 13).date():
            return 0

        year, month = date.year, date.month

        if datetime(2024, 3, 1).date() <= date <= datetime(2024, 12, 31).date():
            expiry_weekday = 2  # Wednesday
        else:
            expiry_weekday = 3  # Thursday

        expiry_date = get_last_weekday(year, month, expiry_weekday)
        expiry_date = get_previous_trading_day(expiry_date)

        return 1 if date == expiry_date else 0

    df["NSE BankNifty Monthly Expiry"] = df["Calendar Date"].map(get_expiry_status)
    return df


# ───────────────────────────────────────────────
# FinNifty Weekly Expiry
# ───────────────────────────────────────────────

def apply_FinNifty_weekly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dropna().dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    # FinNifty weekly expiry launch and change dates
    launch_date = datetime(2021, 1, 11)
    tuesday_start = datetime(2021, 10, 14)
    monday_start = datetime(2025, 4, 4)
    end_date = df["Calendar Date"].max()

    expiry_data = []
    current_date = launch_date

    while current_date <= end_date:
        if current_date < tuesday_start:
            weekday_target = 3  # Thursday
        elif current_date < monday_start:
            weekday_target = 1  # Tuesday
        else:
            weekday_target = 0  # Monday

        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7
        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_data.append(actual_expiry)
        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_data)
    df["NSE FinNifty Weekly Expiry"] = df["Calendar Date"].dt.date.apply(lambda x: 1 if x in expiry_dates_set else 0)

    return df

# ───────────────────────────────────────────────
# FinNifty Monthly Expiry
# ───────────────────────────────────────────────
def apply_finnifty_monthly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"])
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dt.date)

    monthly_expiry_start_date = datetime(2021, 1, 11).date()
    tuesday_start = datetime(2021, 10, 14).date()
    monday_start = datetime(2025, 4, 4).date()

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    def get_last_weekday(year, month, weekday_target):
        if month == 12:
            first_day_next_month = datetime(year + 1, 1, 1)
        else:
            first_day_next_month = datetime(year, month + 1, 1)
        last_day = first_day_next_month - timedelta(days=1)

        while last_day.weekday() != weekday_target:
            last_day -= timedelta(days=1)

        return last_day.date()

    def get_monthly_expiry_status(date_input):
        if isinstance(date_input, pd.Timestamp):
            date = date_input.date()
        elif isinstance(date_input, str):
            try:
                date = datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                return 0
        else:
            return 0

        if date < monthly_expiry_start_date:
            return 0

        if date < tuesday_start:
            weekday_target = 3  # Thursday
        elif date < monday_start:
            weekday_target = 1  # Tuesday
        else:
            weekday_target = 0  # Monday

        scheduled_expiry = get_last_weekday(date.year, date.month, weekday_target)
        actual_expiry = get_previous_trading_day(scheduled_expiry)

        return 1 if date == actual_expiry else 0

    df["NSE FinNifty Monthly Expiry"] = df["Calendar Date"].map(get_monthly_expiry_status)
    return df

# ───────────────────────────────────────────────
# Bse Sensex Weekly Expiry
# ───────────────────────────────────────────────
def apply_bse_sensex_weekly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dropna().dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    # Expiry transition dates
    launch_date = datetime(2020, 6, 29)         # Monday
    friday_start = datetime(2023, 5, 15)        # Friday
    tuesday_start = datetime(2025, 1, 2)        # Tuesday
    end_date = df["Calendar Date"].max()

    expiry_data = []
    current_date = launch_date

    while current_date <= end_date:
        if current_date < friday_start:
            weekday_target = 0  # Monday
        elif current_date < tuesday_start:
            weekday_target = 4  # Friday
        else:
            weekday_target = 1  # Tuesday

        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7
        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_data.append(actual_expiry)
        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_data)
    df["BSE Sensex Weekly Expiry"] = df["Calendar Date"].dt.date.apply(lambda x: 1 if x in expiry_dates_set else 0)

    return df

# ───────────────────────────────────────────────
# BSE Sensex Weekly Expiry
# ───────────────────────────────────────────────

def apply_bse_sensex_monthly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"])
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dt.date)

    monthly_expiry_start_date = datetime(2000, 6, 9).date()
    friday_start = datetime(2023, 5, 15).date()
    tuesday_start = datetime(2025, 1, 1).date()

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    def get_last_weekday(year, month, weekday_target):
        if month == 12:
            first_day_next_month = datetime(year + 1, 1, 1)
        else:
            first_day_next_month = datetime(year, month + 1, 1)
        last_day = first_day_next_month - timedelta(days=1)

        while last_day.weekday() != weekday_target:
            last_day -= timedelta(days=1)

        return last_day.date()

    def get_monthly_expiry_status(date_input):
        if isinstance(date_input, pd.Timestamp):
            date = date_input.date()
        elif isinstance(date_input, str):
            try:
                date = datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                return 0
        else:
            return 0

        if date < monthly_expiry_start_date:
            return 0

        if date < friday_start:
            weekday_target = 3  # Thursday
        elif date < tuesday_start:
            weekday_target = 4  # Friday
        else:
            weekday_target = 1  # Tuesday

        scheduled_expiry = get_last_weekday(date.year, date.month, weekday_target)
        actual_expiry = get_previous_trading_day(scheduled_expiry)

        return 1 if date == actual_expiry else 0

    df["BSE Sensex Monthly Expiry"] = df["Calendar Date"].map(get_monthly_expiry_status)
    return df

# ──────────────────────
# BSE sensex50 weekly expiry
# ──────────────────────
def bse_sensex50_weekly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dropna().dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    launch_date = datetime(2018, 10, 26) # Friday
    change_date = datetime(2025, 1, 1) # Tuesday
    end_date = df["Calendar Date"].max()

    expiry_data = []
    current_date = launch_date

    while current_date <= end_date:
        weekday_target = 4 if current_date.date() < change_date.date() else 2  # Thursday to Monday
        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7
        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_data.append(actual_expiry)
        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_data)
    df["BSE Sensex50 Weekly Expiry"] = df["Calendar Date"].dt.date.apply(lambda x: 1 if x in expiry_dates_set else 0)

    return df

# ──────────────────────
# Bankex monthly expiry
# ──────────────────────

def bse_bankex_monthly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    df = df.sort_values("Calendar Date")  # Ensure chronological order
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dropna().dt.date)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    # Define expiry rule change dates
    launch_date = datetime(2023, 5, 15)
    friday_start = datetime(2023, 10, 16)
    tuesday_start = datetime(2025, 1, 1)
    end_date = df["Calendar Date"].max()

    expiry_data = []

    current = launch_date.replace(day=1)
    while current <= end_date:
        if current < friday_start:
            weekday_target = 4  # Friday
        elif current < tuesday_start:
            weekday_target = 0  # Monday
        else:
            weekday_target = 1  # Tuesday

        # Get last day of the current month
        if current.month == 12:
            next_month = current.replace(year=current.year+1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month+1, day=1)

        last_day_of_month = next_month - timedelta(days=1)

        # Find last target weekday in the month
        days_back = (last_day_of_month.weekday() - weekday_target + 7) % 7
        scheduled_expiry = last_day_of_month - timedelta(days=days_back)

        # Adjust to previous trading day if needed
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_data.append(actual_expiry)

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year+1, month=1, day=1)
        else:
            current = current.replace(month=current.month+1, day=1)

    expiry_dates_set = set(expiry_data)
    df["BSE Bankex Monthly Expiry"] = df["Calendar Date"].dt.date.apply(lambda x: 1 if x in expiry_dates_set else 0)

    return df

# ──────────────────────
# Bankex weekly expiry
# ──────────────────────


def bse_bankex_weekly_expiry(df):
    trading_days = set(pd.to_datetime(df[df["Trading Day"] == 1.0]["Calendar Date"]).dt.date)

    launch_date = datetime(2023, 5, 15)
    shift_date = datetime(2025, 1, 1)

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    end_date = pd.to_datetime(df["Calendar Date"]).max()
    expiry_dates = []

    current_date = launch_date
    while current_date <= end_date:
        weekday_target = 4 if current_date.date() < shift_date.date() else 1  # Friday to Tuesday

        days_ahead = (weekday_target - current_date.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead else 7
        scheduled_expiry = current_date + timedelta(days=days_ahead)
        actual_expiry = get_previous_trading_day(scheduled_expiry.date())

        expiry_dates.append(actual_expiry)
        current_date = scheduled_expiry + timedelta(days=1)

    expiry_dates_set = set(expiry_dates)
    df["BSE Bankex Weekly Expiry"] = pd.to_datetime(df["Calendar Date"]).dt.date.apply(
        lambda x: 1 if x in expiry_dates_set else 0
    )

    return df

# ──────────────────────
# Sensex 50 Montly expiry
# ──────────────────────

def apply_sensex50_monthly_expiry(df):
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"])
    trading_days = set(df[df["Trading Day"] == 1.0]["Calendar Date"].dt.date)

    # Updated start and shift dates
    monthly_expiry_start_date = datetime(2017, 3, 14).date()
    expiry_shift_date = datetime(2025, 1, 1).date()

    def get_previous_trading_day(date):
        while date not in trading_days:
            date -= timedelta(days=1)
        return date

    def get_last_weekday(year, month, weekday_target):
        if month == 12:
            first_day_next_month = datetime(year + 1, 1, 1)
        else:
            first_day_next_month = datetime(year, month + 1, 1)
        last_day = first_day_next_month - timedelta(days=1)

        while last_day.weekday() != weekday_target:
            last_day -= timedelta(days=1)

        return last_day.date()

    def get_monthly_expiry_status(date_input):
        if isinstance(date_input, pd.Timestamp):
            date = date_input.date()
        elif isinstance(date_input, str):
            try:
                date = datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                return 0
        else:
            return 0

        if date < monthly_expiry_start_date:
            return 0

        # Weekday change: Friday (4) before 2025-01-01, Tuesday (1) after
        weekday_target = 4 if date < expiry_shift_date else 1
        scheduled_expiry = get_last_weekday(date.year, date.month, weekday_target)
        actual_expiry = get_previous_trading_day(scheduled_expiry)

        return 1 if date == actual_expiry else 0

    df["BSE Sensex50 Monthly Expiry"] = df["Calendar Date"].map(get_monthly_expiry_status)
    return df

# ───────────────────────────────────────────────
# Banknifty Close Price
# ───────────────────────────────────────────────
def update_latest_banknifty_close(df):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%d-%m-%Y")
    yesterday_str_web = yesterday.strftime("%b %d, %Y")

    options = Options()
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/113.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)


    driver = webdriver.Chrome(options=options)
    driver.get("https://in.investing.com/indices/bank-nifty-historical-data")

    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except:
        pass

    table = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    bank_close = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                bank_close = float(close_price)
                print(f"✅Found BankNifty close for {yesterday_str_web}: {bank_close}")
                break

    driver.quit()

    if bank_close:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "Bank Nifty Close Price"] = bank_close
            print("✅Bank Nifty close price updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find FinNifty close price on website.")

    return df

# ───────────────────────────────────────────────
# Fin Nifty Close Price
# ───────────────────────────────────────────────
def update_finnifty_close_price(df):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%d-%m-%Y")
    yesterday_str_web = yesterday.strftime("%b %d, %Y")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/113.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    driver.get("https://in.investing.com/indices/cnx-finance-historical-data")

    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except:
        pass

    table = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    fin_close = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                fin_close = float(close_price)
                print(f"✅Found FinNifty close for {yesterday_str_web}: {fin_close}")
                break

    driver.quit()

    if fin_close:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "Fin Nifty Close Price"] = fin_close
            print("✅Fin Nifty close price updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find FinNifty close price on website.")

    return df

# ───────────────────────────────────────────────
# VIX
# ───────────────────────────────────────────────
def update_vix(df):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%d-%m-%Y")
    yesterday_str_web = yesterday.strftime("%b %d, %Y")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/113.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    driver.get("https://in.investing.com/indices/india-vix-historical-data")

    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except:
        pass

    table = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    fin_close = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                fin_close = float(close_price)
                print(f"✅Found VIX for {yesterday_str_web}: {fin_close}")
                break

    driver.quit()

    if fin_close:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "VIX"] = fin_close
            print("✅VIX updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find VIX on website.")

    return df


# ───────────────────────────────────────────────
# Sensex
# ───────────────────────────────────────────────


def update_latest_sensex_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download Sensex (BSE 30) data
    sensex = yf.download("^BSESN", start=start_date, end=end_date)
    if sensex.empty:
        print("⚠️ No Sensex data found.")
        return df

    valid_data = sensex[sensex.index.date < today]
    if valid_data.empty:
        print("⚠️ No past Sensex data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using Sensex data from {last_date}: Close = {close_price}")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "SENSEX"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["SENSEX"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with Sensex close: {close_price}")

    return df

# ───────────────────────────────────────────────
# Gold USD Price
# ───────────────────────────────────────────────

def update_latest_gold_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download Gold Futures in USD
    gold = yf.download("GC=F", start=start_date, end=end_date)
    if gold.empty:
        print("⚠️ No Gold data found.")
        return df

    valid_data = gold[gold.index.date < today]
    if valid_data.empty:
        print("⚠️ No past Gold data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using Gold data from {last_date}: Close = {close_price} USD")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "Gold USD Price"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["Gold USD Price"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with Gold close: {close_price} USD")

    return df


# ───────────────────────────────────────────────
# USD/INR
# ───────────────────────────────────────────────

def update_latest_usdinr_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download USD/INR exchange rate data
    usdinr = yf.download("USDINR=X", start=start_date, end=end_date)
    if usdinr.empty:
        print("⚠️ No USD/INR data found.")
        return df

    valid_data = usdinr[usdinr.index.date < today]
    if valid_data.empty:
        print("⚠️ No past USD/INR data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using USD/INR data from {last_date}: Close = {close_price} INR")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "USD/INR"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["USD/INR"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with USD/INR close: {close_price} INR")

    return df

# ───────────────────────────────────────────────
# EUR/INR
# ───────────────────────────────────────────────


def update_latest_eurinr_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download EUR to INR exchange rate data
    eurinr = yf.download("EURINR=X", start=start_date, end=end_date)
    if eurinr.empty:
        print("⚠️ No EUR/INR data found.")
        return df

    valid_data = eurinr[eurinr.index.date < today]
    if valid_data.empty:
        print("⚠️ No past EUR/INR data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using EUR/INR data from {last_date}: Close = {close_price} INR")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "EUR/INR"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["EUR/INR"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with EUR/INR close: {close_price} INR")

    return df


# ──────────────────────
# India 10 Y Bond Yield
# ──────────────────────

def india_10_y_bond_yield(df):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%d-%m-%Y")
    yesterday_str_web = yesterday.strftime("%b %d, %Y")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/113.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    driver.get("https://in.investing.com/rates-bonds/india-10-year-bond-yield-historical-data")

    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except:
        pass

    table = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    india_bond_yield = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                india_bond_yield = float(close_price)
                print(f"✅Found India 10 Y Bond Yield for {yesterday_str_web}: {india_bond_yield}")
                break

    driver.quit()

    if india_bond_yield:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "India 10 Y Bond Yield"] = india_bond_yield
            print("✅India 10 Y Bond Yield updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find India 10 Y Bond Yield on website.")

    return df


# ──────────────────────
# US 10 Y Bond Yield
# ──────────────────────

def us_10_y_bond_yield(df):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%d-%m-%Y")
    yesterday_str_web = yesterday.strftime("%b %d, %Y")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/113.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    driver.get("https://in.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data")

    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except:
        pass

    table = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    us_bond_yield = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                us_bond_yield = float(close_price)
                print(f"✅Found US 10 Y Bond Yield for {yesterday_str_web}: {us_bond_yield}")
                break

    driver.quit()

    if us_bond_yield:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "US 10 Y Bond Yield"] = us_bond_yield
            print("✅US 10 Y Bond Yield updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find US 10 Y Bond Yield on website.")

    return df


# ────────────────
# Dollar Index
# ────────────────

def update_latest_dollar_index_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download US Dollar Index (DXY) data
    dxy = yf.download("DX-Y.NYB", start=start_date, end=end_date)
    if dxy.empty:
        print("⚠️ No Dollar Index data found.")
        return df

    valid_data = dxy[dxy.index.date < today]
    if valid_data.empty:
        print("⚠️ No past Dollar Index data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using Dollar Index data from {last_date}: Close = {close_price}")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "Dollar Index"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["Dollar Index"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with Dollar Index close: {close_price}")

    return df


# ────────────────
# Crude Oil
# ────────────────

def update_latest_crudeoil_close(df):
    today = datetime.today().date()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)

    # Download WTI Crude Oil Futures data
    crude = yf.download("CL=F", start=start_date, end=end_date)
    if crude.empty:
        print("⚠️ No Crude Oil data found.")
        return df

    valid_data = crude[crude.index.date < today]
    if valid_data.empty:
        print("⚠️ No past Crude Oil data available before today.")
        return df

    last_date = valid_data.index[-1].date()
    close_price = valid_data["Close"].iloc[-1].item()
    print(f"✅Using Crude Oil data from {last_date}: Close = ${close_price}")

    updated = False
    for idx, row in df.iterrows():
        if pd.to_datetime(row["Calendar Date"]).date() == last_date:
            df.at[idx, "Crude Oil"] = close_price
            updated = True
            break

    if not updated:
        new_row = {col: "" for col in df.columns}
        new_row["Calendar Date"] = last_date
        new_row["Crude Oil"] = close_price
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"➕ Added new row for {last_date} with Crude Oil close: ${close_price}")

    return df
# ────────────────
# Main logic
# ────────────────
def main():
    input_file = "Calendar1_Updated3.csv"
    df = pd.read_csv(input_file)
    
    # Ensure correct date formatting
    df["Calendar Date"] = pd.to_datetime(df["Calendar Date"], errors="coerce")
    
    
    df = update_latest_nifty_close(df)
    print("✅Nifty50 CLose price Column Updated Successfully!")
    df = update_trading_day(df)
    print("✅Trading Day Column Updated Successfully!")
    df = apply_weekly_expiry(df)
    print("✅NSE Nifty Weekly Expiry Column Updated Successfully!")
    df = apply_nifty_monthly_expiry(df)
    print("✅NSE Nifty Monthly Expiry Column Updated Successfully!")
    df = apply_banknifty_weekly_expiry(df)
    print("✅NSE BankNifty Weekly Expiry Column Updated Successfully!")
    df = apply_banknifty_monthly_expiry(df)
    print("✅NSE BankNifty Monthly Expiry Column Updated Successfully!")
    df = apply_FinNifty_weekly_expiry(df)
    print("✅NSE FinNifty Weekly Expiry Column Updated Successfully!")
    df = apply_finnifty_monthly_expiry(df)
    print("✅NSE FinNifty Monthly Expiry Column Updated Successfully!")
    df = apply_bse_sensex_weekly_expiry(df)
    print("✅BSE Sensex Weekly Expiry Column Updated Successfully!")
    df = apply_bse_sensex_monthly_expiry(df)
    print("✅BSE Sensex Monthly Expiry Column Updated Successfully!")
    df = apply_sensex50_monthly_expiry(df)
    print("✅BSE Sensex50 Monthly Expiry Columns Updated Successfully!")
    df = bse_bankex_weekly_expiry(df)
    print("✅BSE Bankex Weekly Expiry Columns Updated Successfully!")
    df = bse_bankex_monthly_expiry(df)
    print("✅BSE Bankex Monthly Expiry Columns Updated Successfully!")
    df = bse_sensex50_weekly_expiry(df)
    print("✅BSE Sensex50 Weekly Expiry Columns Updated Successfully!")
    df = update_latest_banknifty_close(df)
    print("✅NSE BankNifty Close price Column Updated Successfully!")
    df = update_finnifty_close_price(df)
    print("✅FinNifty Close price Column Updated Successfully!")
    df = update_vix(df)
    print("✅VIX Column Updated Successfully!")
    df = update_latest_sensex_close(df)
    print("✅Sensex Column Updated Successfully!")
    df = update_latest_gold_close(df)
    print("✅Gold USD Price Column Updated Successfully!")
    df = update_latest_usdinr_close(df)
    print("✅USD/INR Column Updated Successfully!")
    df = update_latest_eurinr_close(df)
    print("✅EUR/INR Column Updated Successfully!")
    df = india_10_y_bond_yield(df)
    print("✅India 10 Y Bond Yield Column Updated Successfully!")
    df = us_10_y_bond_yield(df)
    print("✅US 10 Y Bond Yield Column Updated Successfully!")
    df= update_latest_dollar_index_close(df)
    print("✅Dollar Index Column Updated Successfully!")
    df = update_latest_crudeoil_close(df)
    print("✅Crude Oil Column Updated Successfully!")



    # Save to new CSV
    df.to_csv("Calendar1_Updated3.csv", index=False)
    print("✅ All updates applied and saved to 'Calendar1.csv'")


if __name__ == "__main__":
    main()
