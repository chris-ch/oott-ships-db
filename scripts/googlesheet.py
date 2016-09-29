import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

"""
Access managed from https://console.developers.google.com/project.
"""

def open_secret(secret_name):
    scope = ['https://spreadsheets.google.com/feeds']
    full_path = os.path.abspath(secret_name)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(full_path, scope)
    return gspread.authorize(credentials)


def main():
    secret_name = 'IBrokers ETF database-fb65211483c3.json'
    google = open_secret(secret_name)
    IBROKERS_ETF_DB_KEY = '1EubrBpGAJClPRVMMYwpjcUnJIWX7L29T59T6jnAsu88'
    worksheet = google.open_by_key(IBROKERS_ETF_DB_KEY).get_worksheet(0)
    worksheet.update_cell(1, 2, 'Bingo!')

if __name__ == '__main__':
    main()
