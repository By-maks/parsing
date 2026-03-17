import os
import time
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TelegramToVKPublisher:
    def __init__(self):
        # Токены и ID
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_channel = os.getenv('TELEGRAM_CHANNEL_ID')  # @channel_name или -100...
        self.vk_token = os.getenv('VK_GROUP_TOKEN')
        self.vk_group_id = os.getenv('VK_GROUP_ID')
        
        # Файл для хранения ID обработанных постов
        self.processed_ids_file = 'processed_posts.json'
        self.processed_ids = self.load_processed_ids()
        
        # Временная папка для медиа
        self.temp_dir = 'temp_media'
        os.makedirs(self.temp_dir, exist_ok=True)

    def load_processed_ids(self) -> set:
        """Загрузка ID обработанных постов"""
        try:
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Очищаем старые записи (старше 7 дней)
                    cutoff = datetime.now() - timedelta(days=7)
                    data = {msg_id: timestamp for msg_id, timestamp in data.items() 
                           if datetime.fromisoformat(timestamp) > cutoff}
                    return set(data.keys())
        except Exception as e:
            logger.error(f"Ошибка загрузки processed_ids: {e}")
        return set()

    def save_processed_id(self, message_id: str):
        """Сохранение ID обработанного поста"""
        try:
            data = {}
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            data[message_id] = datetime.now().isoformat()
            
            # Оставляем только последние 1000 записей
            if len(data) > 1000:
                # Сортируем по времени и оставляем последние 1000
                sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)
                data = dict(sorted_items[:1000])
            
            with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            self.processed_ids.add(message_id)
        except Exception as e:
            logger.error(f"Ошибка сохранения processed_id: {e}")

    def get_telegram_posts(self, limit: int = 10) -> List[Dict]:
        """Получение последних постов из Telegram"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/getUpdates"
        
        params = {
            'timeout': 30,
            'limit': limit,
            'allowed_updates': ['channel_post']
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            posts = []
            if data.get('ok'):
                for update in data.get('result', []):
                    if 'channel_post' in update:
                        post = update['channel_post']
                        posts.append({
                            'id': str(update['update_id']),  # Уникальный ID обновления
                            'message_id': str(post['message_id']),
                            'chat_id': str(post['chat']['id']),
                            'text': post.get('text', ''),
                            'caption': post.get('caption', ''),
                            'date': post['date'],
                            'media': self.extract_media(post)
                        })
            return posts
        except Exception as e:
            logger.error(f"Ошибка получения постов из Telegram: {e}")
            return []

    def extract_media(self, post: Dict) -> List[Dict]:
        """Извлечение медиа из поста"""
        media = []
        
        # Проверяем разные типы медиа
        if 'photo' in post:
            # Берем самую большую версию фото
            photos = post['photo']
            if photos:
                file_id = photos[-1]['file_id']
                media.append({'type': 'photo', 'file_id': file_id})
        
        if 'video' in post:
            media.append({'type': 'video', 'file_id': post['video']['file_id']})
            
        if 'document' in post:
            media.append({'type': 'document', 'file_id': post['document']['file_id']})
            
        # Проверяем медиагруппы (альбомы)
        if 'media_group_id' in post:
            media[0]['group_id'] = post['media_group_id']
            
        return media

    def download_telegram_file(self, file_id: str) -> str:
        """Скачивание файла из Telegram"""
        # Получаем путь к файлу
        url = f"https://api.telegram.org/bot{self.telegram_token}/getFile"
        response = requests.get(url, params={'file_id': file_id})
        data = response.json()
        
        if data.get('ok'):
            file_path = data['result']['file_path']
            file_url = f"https://api.telegram.org/file/bot{self.telegram_token}/{file_path}"
            
            # Скачиваем файл
            local_filename = os.path.join(self.temp_dir, file_path.split('/')[-1])
            response = requests.get(file_url)
            
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            
            return local_filename
        return None

    def upload_to_vk(self, file_path: str, file_type: str) -> str:
        """Загрузка медиа в VK"""
        # Получаем URL для загрузки
        if file_type == 'photo':
            url = 'https://api.vk.com/method/photos.getWallUploadServer'
            params = {
                'group_id': self.vk_group_id,
                'access_token': self.vk_token,
                'v': '5.131'
            }
        else:  # video, document
            # Для видео нужна отдельная логика
            url = 'https://api.vk.com/method/video.save'
            params = {
                'group_id': self.vk_group_id,
                'access_token': self.vk_token,
                'v': '5.131'
            }
        
        response = requests.get(url, params=params)
        upload_url = response.json()['response']['upload_url']
        
        # Загружаем файл
        with open(file_path, 'rb') as f:
            files = {'file': f}
            upload_response = requests.post(upload_url, files=files)
        
        # Сохраняем загруженный файл
        if file_type == 'photo':
            save_url = 'https://api.vk.com/method/photos.saveWallPhoto'
            data = upload_response.json()
            params = {
                'group_id': self.vk_group_id,
                'photo': data['photo'],
                'server': data['server'],
                'hash': data['hash'],
                'access_token': self.vk_token,
                'v': '5.131'
            }
        else:
            # Для видео
            save_url = 'https://api.vk.com/method/video.save'
            params = {
                'group_id': self.vk_group_id,
                'video_id': upload_response.json()['response']['video_id'],
                'access_token': self.vk_token,
                'v': '5.131'
            }
        
        save_response = requests.post(save_url, params=params)
        media_id = save_response.json()['response'][0]['id']
        
        # Формируем attachment строку
        return f"{file_type}{self.vk_group_id}_{media_id}"

    def publish_to_vk(self, text: str, attachments: List[str] = None):
        """Публикация поста в VK"""
        url = 'https://api.vk.com/method/wall.post'
        
        params = {
            'owner_id': f'-{self.vk_group_id}',
            'from_group': 1,
            'message': text[:10000],  # VK ограничение
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        if attachments:
            params['attachments'] = ','.join(attachments)
        
        response = requests.post(url, params=params)
        return response.json()

    def process_new_posts(self):
        """Основная логика обработки новых постов"""
        logger.info("Начинаем проверку новых постов...")
        
        # Получаем посты из Telegram
        posts = self.get_telegram_posts(limit=10)
        
        # Фильтруем новые посты
        new_posts = [p for p in posts if p['id'] not in self.processed_ids]
        
        if not new_posts:
            logger.info("Новых постов не найдено")
            return
        
        logger.info(f"Найдено {len(new_posts)} новых постов")
        
        # Обрабатываем каждый новый пост
        for post in new_posts:
            try:
                attachments = []
                
                # Скачиваем и загружаем медиа
                for media in post['media']:
                    file_path = self.download_telegram_file(media['file_id'])
                    if file_path:
                        attachment = self.upload_to_vk(file_path, media['type'])
                        attachments.append(attachment)
                        # Удаляем временный файл
                        os.remove(file_path)
                
                # Текст поста (если есть caption, используем его)
                text = post.get('caption') or post.get('text') or ''
                
                # Публикуем в VK
                result = self.publish_to_vk(text, attachments)
                
                if 'error' not in result:
                    # Сохраняем ID обработанного поста
                    self.save_processed_id(post['id'])
                    logger.info(f"Пост {post['id']} успешно опубликован")
                else:
                    logger.error(f"Ошибка публикации: {result}")
                    
                # Небольшая задержка между постами
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Ошибка обработки поста {post['id']}: {e}")
                
        # Очищаем временную папку
        self.cleanup_temp_files()

    def cleanup_temp_files(self):
        """Очистка временных файлов"""
        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                logger.error(f"Ошибка удаления {file_path}: {e}")

if __name__ == "__main__":
    publisher = TelegramToVKPublisher()
    publisher.process_new_posts()
