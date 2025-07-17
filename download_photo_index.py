import os
import csv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']
ROOT_FOLDER_ID = '15TAAvPHbjBtv56u83dQ_v0cVpusTuzAG'


def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)


def list_folders(service, parent_id):
    query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])


def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])


def extract_article(filename):
    return filename.split('_')[0][:5]


def main():
    service = authenticate()
    data = []

    folders = list_folders(service, ROOT_FOLDER_ID)
    for folder in folders:
        brand = folder['name'].strip("[]")
        folder_id = folder['id']
        files = list_files_in_folder(service, folder_id)

        for file in files:
            filename = file['name']
            article = extract_article(filename)
            data.append({'Бренд': brand, 'Назва файла': filename, 'Артикул': article})

    with open('bank_photo.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Бренд', 'Назва файла', 'Артикул']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"📁 bank_photo.csv збережено: {len(data)} записів")


if __name__ == '__main__':
    main()
