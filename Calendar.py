import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re


# Ensure correct date formatting
    

def setup_driver():
    """Sets up the Chrome driver in headless mode."""
    options = Options()
    options.add_argument("--headless")  # Run in background (headless)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0")
    return webdriver.Chrome(options=options)

def clean_date(s):
    """Cleans and standardizes the date string."""
    return re.sub(r'\W+', '', s).lower()

yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%d-%m-%Y")
yesterday_str_web = yesterday.strftime("%b %d, %Y")
date_web = yesterday.strftime("%b %d, %Y")
date_csv = yesterday.strftime("%d-%m-%Y")
# ─────────────────────────────────────────────────────────────
# Fetch and update the last available Nifty50 close price
# ─────────────────────────────────────────────────────────────

def update_latest_nifty_close(df):
    # driver = webdriver.Chrome(options=options)
    driver = setup_driver()
    driver.get("https://in.investing.com/indices/s-p-cnx-nifty-historical-data")

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
    nifty_close = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            nifty_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                nifty_close = float(nifty_price)
                print(f"📅Found Nifty50 close for {yesterday_str_web}: {nifty_close}")
                break

    driver.quit()

    if nifty_close:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "Nifty50 Close Price"] = nifty_close
            print("✅Nifty50 close price updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find Nifty50 close price on website.")

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
    driver = setup_driver()

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
                print(f"📅Found BankNifty close for {yesterday_str_web}: {bank_close}")
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
    driver = setup_driver()
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
                print(f"📅Found FinNifty close for {yesterday_str_web}: {fin_close}")
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

    driver = setup_driver()
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
    vix_close = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                vix_close = float(close_price)
                print(f"📅Found VIX for {yesterday_str_web}: {vix_close}")
                break

    driver.quit()

    if vix_close:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "VIX"] = vix_close
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

    driver = setup_driver()
    driver.get("https://in.investing.com/indices/sensex-historical-data")

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
    sensex = None

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) >= 2:
            date_text = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")
            if date_text == yesterday_str_web:
                sensex = float(close_price)
                print(f"📅Found SENSEX for {yesterday_str_web}: {sensex}")
                break

    driver.quit()

    if sensex:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == yesterday_str
        if mask.any():
            df.loc[mask, "SENSEX"] = sensex
            print("✅SENSEX updated in DataFrame.")
        else:
            print("⚠️ Yesterday's date not found in DataFrame.")
    else:
        print("❌ Could not find SENSEX on website.")

    return df



# ───────────────────────────────────────────────
# Gold USD Price
# ───────────────────────────────────────────────

def update_latest_gold_close(df):
   
    def fetch_gold_price(date_str_web):
        """Fetches the gold close price from the investing.com historical data page."""
        try:
            driver = setup_driver()
            driver.get("https://in.investing.com/currencies/xau-usd-historical-data")
            # print("🌐 Opened investing.com page...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                ).click()
                # print("🍪 Accepted cookies.")
            except:
                print("✅ No cookie popup or already accepted.")

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
            )

            rows = driver.find_elements(By.XPATH, "//table//tbody/tr")

            for _ in range(10):
                if any(row.text.strip() for row in rows):
                    break
                # print("⌛ Waiting for data to populate...")
                time.sleep(1)

            for idx, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_price = cols[1].text.strip().replace(",", "")
                    if clean_date(date_text) == clean_date(date_str_web):
                        driver.quit()
                        return float(close_price), date_text

            driver.quit()
            return None, None

        except Exception as e:
            # print(f"❌ Error while scraping: {e}")
            return None, None

    # ──────────────────────────────
    # Actual update logic
    # ──────────────────────────────

    price, matched_date = fetch_gold_price(date_web)

    if price and matched_date:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == date_csv
        if mask.any():
            df.loc[mask, "Gold USD Price"] = price
            print(f"✅ Found Gold USD close for {matched_date}: {price}")
            print("✅ Gold USD Close Price updated in DataFrame.")
        else:
            print("⚠️ Date not found in DataFrame.")
    else:
        print("❌ Could not fetch gold close price.")

    return df


# ───────────────────────────────────────────────
# USD/INR
# ───────────────────────────────────────────────

def update_latest_usdinr_close(df):
   
    def fetch_usdinr_price(date_str_web):
        """Fetches the gold close price from the investing.com historical data page."""
        try:
            driver = setup_driver()
            driver.get("https://in.investing.com/currencies/usd-inr-historical-data")

            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                ).click()
                # print("🍪Accepted cookies.")
            except:
                print("✅No cookie popup or already accepted.")

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
            )

            rows = driver.find_elements(By.XPATH, "//table//tbody/tr")

            for _ in range(10):
                if any(row.text.strip() for row in rows):
                    break
                print("⌛Waiting for data to populate...")
                time.sleep(1)

            for idx, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_price = cols[1].text.strip().replace(",", "")
                    if clean_date(date_text) == clean_date(date_str_web):
                        driver.quit()
                        return float(close_price), date_text

            driver.quit()
            return None, None

        except Exception as e:
            print(f"❌ Error while scraping: {e}")
            return None, None

    # ──────────────────────────────
    # Actual update logic
    # ──────────────────────────────

    price, matched_date = fetch_usdinr_price(date_web)

    if price and matched_date:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == date_csv
        if mask.any():
            df.loc[mask, "USD/INR"] = price
            print(f"✅Found USD/INR close for {matched_date}: {price}")
            print("✅USD/INR Close Price updated in DataFrame.")
        else:
            print("⚠️ Date not found in DataFrame.")
    else:
        print("❌ Could not fetch USD/INR close price.")

    return df

# ───────────────────────────────────────────────
# EUR/INR
# ───────────────────────────────────────────────

def update_latest_eurinr_close(df):

    def fetch_eurinr_price(date_str_web):
        """Fetches the gold close price from the investing.com historical data page."""
        try:
            driver = setup_driver()
            driver.get("https://in.investing.com/currencies/eur-inr-historical-data")
            # print("🌐 Opened investing.com page...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                ).click()
                print("🍪Accepted cookies.")
            except:
                print("✅No cookie popup or already accepted.")

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
            )

            rows = driver.find_elements(By.XPATH, "//table//tbody/tr")

            for _ in range(10):
                if any(row.text.strip() for row in rows):
                    break
                print("⌛Waiting for data to populate...")
                time.sleep(1)

            for idx, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_price = cols[1].text.strip().replace(",", "")
                    if clean_date(date_text) == clean_date(date_str_web):
                        driver.quit()
                        return float(close_price), date_text

            driver.quit()
            return None, None

        except Exception as e:
            print(f"❌ Error while scraping: {e}")
            return None, None
            

    price, matched_date = fetch_eurinr_price(date_web)

    if price and matched_date:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == date_csv
        if mask.any():
            df.loc[mask, "EUR/INR"] = price
            print(f"✅Found EUR/INR close for {matched_date}: {price}")
            print("✅EUR/INR Close Price updated in DataFrame.")
        else:
            print("⚠️ Date not found in DataFrame.")
    else:
        print("❌ Could not fetch gold close price.")

    return df


# ──────────────────────
# India 10 Y Bond Yield
# ──────────────────────

def india_10_y_bond_yield(df):
    driver = setup_driver()
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

    driver = setup_driver()
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

    def fetch_dollar_price(date_str_web):
        """Fetches the gold close price from the investing.com historical data page."""
        try:
            driver = setup_driver()
            driver.get("https://in.investing.com/indices/usdollar-historical-data")
            # print("🌐 Opened investing.com page...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                ).click()
                print("🍪Accepted cookies.")
            except:
                print("✅No cookie popup or already accepted.")

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
            )

            rows = driver.find_elements(By.XPATH, "//table//tbody/tr")

            for _ in range(10):
                if any(row.text.strip() for row in rows):
                    break
                print("⌛Waiting for data to populate...")
                time.sleep(1)

            for idx, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_price = cols[1].text.strip().replace(",", "")
                    if clean_date(date_text) == clean_date(date_str_web):
                        driver.quit()
                        return float(close_price), date_text

            driver.quit()
            return None, None

        except Exception as e:
            print(f"❌ Error while scraping: {e}")
            return None, None
            

    price, matched_date = fetch_dollar_price(date_web)

    if price and matched_date:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == date_csv
        if mask.any():
            df.loc[mask, "Dollar Index"] = price
            print(f"✅Found Dollar Index close for {matched_date}: {price}")
            print("✅Dollar Index Close Price updated in DataFrame.")
        else:
            print("⚠️ Date not found in DataFrame.")
    else:
        print("❌ Could not fetch Dollar Index close price.")

    return df

# ────────────────
# Crude Oil
# ────────────────

def update_latest_crudeoil_close(df):

    def fetch_crude_price(date_str_web):
        """Fetches the gold close price from the investing.com historical data page."""
        try:
            driver = setup_driver()
            driver.get("https://in.investing.com/commodities/crude-oil-historical-data")
            # print("🌐 Opened investing.com page...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                ).click()
                print("🍪Accepted cookies.")
            except:
                print("✅No cookie popup or already accepted.")

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
            )

            rows = driver.find_elements(By.XPATH, "//table//tbody/tr")

            for _ in range(10):
                if any(row.text.strip() for row in rows):
                    break
                print("⌛Waiting for data to populate...")
                time.sleep(1)

            for idx, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_price = cols[1].text.strip().replace(",", "")
                    if clean_date(date_text) == clean_date(date_str_web):
                        driver.quit()
                        return float(close_price), date_text

            driver.quit()
            return None, None

        except Exception as e:
            print(f"❌ Error while scraping: {e}")
            return None, None
  

    price, matched_date = fetch_crude_price(date_web)

    if price and matched_date:
        mask = df["Calendar Date"].dt.strftime("%d-%m-%Y") == date_csv
        if mask.any():
            df.loc[mask, "Crude Oil"] = price
            print(f"✅Found Crude Oil close for {matched_date}: {price}")
            print("✅Crude Oil Close Price updated in DataFrame.")
        else:
            print("⚠️ Date not found in DataFrame.")
    else:
        print("❌ Could not fetch Crude Oil close price.")

    return df

# ────────────────
# Main logic
# ────────────────
def main():
    input_file = "Calendar.csv"
    df = pd.read_csv(input_file)
    
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
    df.to_csv("Calendar.csv", index=False)
    print("✅ All updates applied and saved to 'Calendar1.csv'")


if __name__ == "__main__":
    main()
