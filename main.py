import os
import pandas as pd
import requests
import mimetypes
import time
import random

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

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


def safe_name(text):
    return ''.join(c for c in text if c.isalnum() or c in ' ._-').strip()


def get_or_create_folder(service, name, parent_id):
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed = false"
    )
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    folders = response.get('files', [])
    if folders:
        return folders[0]['id']
    else:
        metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(body=metadata, fields='id').execute()
        return folder['id']


def file_exists(service, folder_id, filename):
    query = (
        f"name = '{filename}' and "
        f"'{folder_id}' in parents and "
        f"mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    )
    response = service.files().list(q=query, fields='files(id)', spaces='drive').execute()
    return bool(response.get('files'))



def upload_file(service, folder_id, url, filename_base):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ Помилка при завантаженні: {url}")
            return False

        ext = mimetypes.guess_extension(response.headers.get('content-type', '').split(';')[0]) or '.jpg'
        full_filename = f"{filename_base}{ext}"

        if file_exists(service, folder_id, full_filename):
            print(f"⏭️ Пропущено (вже існує): {full_filename}")
            return False

        with open(full_filename, 'wb') as f:
            f.write(response.content)

        media = MediaFileUpload(full_filename, resumable=True)
        metadata = {'name': full_filename, 'parents': [folder_id]}

        for attempt in range(3):
            try:
                service.files().create(body=metadata, media_body=media, fields='id').execute()
                print(f"✅ Завантажено: {full_filename}")
                os.remove(full_filename)
                time.sleep(1)
                return True
            except HttpError as error:
                if error.resp.status in [403, 429]:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    print(f"⏳ Обмеження API, спроба {attempt + 1}, чекаємо {wait:.1f} сек...")
                    time.sleep(wait)
                else:
                    raise

        os.remove(full_filename)
        return False
    except Exception as e:
        print(f"❌ Помилка: {e} для {filename_base}")
        return False


def main():
    service = authenticate()
    df = pd.read_csv('products.csv', delimiter=';')
    uploaded_count = 0

    for index, row in df.iterrows():
        brand = safe_name(str(row['Бренд']).strip())
        article = safe_name(str(row['Артикул']).strip())
        name = safe_name(str(row['Назва']).strip())
        photo_url = str(row['Посилання на фото']).strip()

        if not (brand and article and name and photo_url):
            print(f"⚠️ Пропущено порожній рядок №{index + 1}")
            continue

        folder_name = f"[{brand}]"
        folder_id = get_or_create_folder(service, folder_name, ROOT_FOLDER_ID)

        filename_base = f"{article}_{name}_{brand}"
        if upload_file(service, folder_id, photo_url, filename_base):
            uploaded_count += 1

    print(f"\n📦 Успішно завантажено {uploaded_count} файл(ів)")


if __name__ == '__main__':
    main()