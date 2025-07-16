import os
import time
import random
import re
import mimetypes
import pandas as pd
import requests

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# --- Налаштування ---
SCOPES = ['https://www.googleapis.com/auth/drive']
ROOT_FOLDER_ID = '15TAAvPHbjBtv56u83dQ_v0cVpusTuzAG'  # ID папки "Фотобанк Jerelia"

# --- Авторизація ---
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

# --- Безпечне ім’я папки для запиту ---
def escape_for_query(name):
    return name.replace("'", "\\'").replace('"', '')

# --- Пошук або створення папки бренду ---
def get_or_create_folder(service, name, parent_id):
    safe_name = escape_for_query(name)
    query = (
        f"name='{safe_name}' and mimeType='application/vnd.google-apps.folder' "
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

# --- Перевірка чи файл уже існує ---
def file_exists(service, folder_id, filename):
    query = (
        f"name='{escape_for_query(filename)}' and '{folder_id}' in parents and trashed = false"
    )
    response = service.files().list(q=query, fields='files(id)', spaces='drive').execute()
    return bool(response.get('files'))

# --- Завантаження фото і створення файлу на Google Drive ---
def upload_image(service, folder_id, url, filename_base):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ Помилка при завантаженні: {url}")
            return

        extension = mimetypes.guess_extension(response.headers.get('content-type', '').split(';')[0]) or '.jpg'
        full_filename = f"{filename_base}{extension}"

        if file_exists(service, folder_id, full_filename):
            print(f"⏭️ Пропущено (вже існує): {full_filename}")
            return

        with open(full_filename, 'wb') as f:
            f.write(response.content)

        media = MediaFileUpload(full_filename, resumable=True)
        metadata = {'name': full_filename, 'parents': [folder_id]}

        for attempt in range(3):
            try:
                service.files().create(body=metadata, media_body=media, fields='id').execute()
                print(f"✅ Завантажено: {full_filename}")
                break
            except HttpError as error:
                if error.resp.status in [403, 429]:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    print(f"⏳ Обмеження API, спроба {attempt + 1}, чекаємо {wait:.1f} сек...")
                    time.sleep(wait)
                else:
                    raise

        os.remove(full_filename)
        time.sleep(1)

    except Exception as e:
        print(f"❌ Помилка: {e} для {filename_base}")

# --- Основна логіка ---
def main():
    service = authenticate()
    df = pd.read_csv('products.csv', delimiter=';')

    for index, row in df.iterrows():
        brand = str(row['Бренд']).strip()
        article = str(row['Артикул']).strip()
        name = str(row['Назва']).strip()
        photo_url = str(row['Посилання на фото']).strip()

        if not (brand and article and name and photo_url):
            print(f"⚠️ Пропущено порожній рядок №{index + 1}")
            continue

        folder_name = f"[{brand}]"
        folder_id = get_or_create_folder(service, folder_name, ROOT_FOLDER_ID)

        filename_base = f"{article}_{name}_{brand}"
        upload_image(service, folder_id, photo_url, filename_base)

if __name__ == '__main__':
    main()
