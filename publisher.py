import os
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class TelegramToVKPublisher:
    def __init__(self):
        # Токены и ID
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_channel = os.getenv('TELEGRAM_CHANNEL_ID')  # @channel_name или -100...
        self.vk_token = os.getenv('VK_GROUP_TOKEN')
        self.vk_group_id = os.getenv('VK_GROUP_ID')
        
        # Проверка наличия всех переменных
        if not all([self.telegram_token, self.telegram_channel, self.vk_token, self.vk_group_id]):
            raise ValueError("Отсутствуют необходимые переменные окружения!")
        
        # Файл для хранения ID обработанных постов
        self.processed_ids_file = 'processed_posts.json'
        self.processed_ids = self.load_processed_ids()
        
        # Временная папка для медиа
        self.temp_dir = 'temp_media'
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Для отслеживания медиагрупп
        self.media_groups = {}
        
        logger.info(f"Инициализация завершена. Канал: {self.telegram_channel}")

    def load_processed_ids(self) -> Set[str]:
        """Загрузка ID обработанных постов с проверкой на дубликаты"""
        try:
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Очищаем старые записи (старше 30 дней)
                    cutoff = datetime.now() - timedelta(days=30)
                    
                    # Поддерживаем два формата: старый (простой список) и новый (словарь)
                    if isinstance(data, list):
                        # Конвертируем старый формат в новый
                        processed = {msg_id: datetime.now().isoformat() for msg_id in data}
                    else:
                        processed = data
                    
                    # Фильтруем старые записи
                    processed = {
                        msg_id: timestamp 
                        for msg_id, timestamp in processed.items() 
                        if datetime.fromisoformat(timestamp) > cutoff
                    }
                    
                    # Сохраняем обновленный список
                    with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                        json.dump(processed, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"Загружено {len(processed)} обработанных постов")
                    return set(processed.keys())
        except Exception as e:
            logger.error(f"Ошибка загрузки processed_ids: {e}")
        return set()

    def save_processed_id(self, message_id: str, message_hash: str = None):
        """Сохранение ID обработанного поста с хешем содержимого"""
        try:
            data = {}
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            # Сохраняем с временной меткой
            data[message_id] = {
                'timestamp': datetime.now().isoformat(),
                'hash': message_hash
            }
            
            # Оставляем только последние 1000 записей
            if len(data) > 1000:
                # Сортируем по времени и оставляем последние 1000
                sorted_items = sorted(
                    data.items(), 
                    key=lambda x: x[1]['timestamp'], 
                    reverse=True
                )
                data = dict(sorted_items[:1000])
            
            with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.processed_ids.add(message_id)
            logger.debug(f"Сохранен ID {message_id}")
        except Exception as e:
            logger.error(f"Ошибка сохранения processed_id: {e}")

    def get_channel_id(self) -> str:
        """Получение числового ID канала из username"""
        if self.telegram_channel.startswith('@'):
            # Публичный канал - нужно получить его ID через getChat
            url = f"https://api.telegram.org/bot{self.telegram_token}/getChat"
            params = {'chat_id': self.telegram_channel}
            
            try:
                response = requests.get(url, params=params)
                data = response.json()
                if data.get('ok'):
                    chat_id = str(data['result']['id'])
                    logger.info(f"Получен ID канала: {chat_id}")
                    return chat_id
                else:
                    logger.error(f"Не удалось получить ID канала: {data}")
            except Exception as e:
                logger.error(f"Ошибка получения ID канала: {e}")
        
        return self.telegram_channel

    def get_telegram_posts(self, limit: int = 10) -> List[Dict]:
        """Получение последних постов ТОЛЬКО из указанного канала"""
        channel_id = self.get_channel_id()
        
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
                        
                        # Проверяем, что пост именно из нашего канала
                        post_chat_id = str(post['chat']['id'])
                        post_chat_username = post['chat'].get('username', '')
                        
                        # Сравниваем с настроенным каналом
                        is_our_channel = (
                            post_chat_id == channel_id or 
                            f"@{post_chat_username}" == self.telegram_channel
                        )
                        
                        if is_our_channel:
                            # Создаем уникальный ID для поста
                            unique_id = f"{post_chat_id}_{post['message_id']}"
                            
                            # Создаем хеш содержимого для дополнительной защиты от дублей
                            content_hash = self.create_content_hash(post)
                            
                            posts.append({
                                'id': unique_id,
                                'message_id': str(post['message_id']),
                                'chat_id': post_chat_id,
                                'chat_username': post_chat_username,
                                'text': post.get('text', ''),
                                'caption': post.get('caption', ''),
                                'date': post['date'],
                                'media_group_id': post.get('media_group_id'),
                                'media': self.extract_media(post),
                                'hash': content_hash
                            })
                            
                            logger.debug(f"Найден пост {unique_id} из канала {post_chat_id}")
            
            # Группируем посты из одной медиагруппы
            posts = self.group_media_posts(posts)
            
            logger.info(f"Получено {len(posts)} постов из канала {self.telegram_channel}")
            return posts
            
        except Exception as e:
            logger.error(f"Ошибка получения постов из Telegram: {e}")
            return []

    def create_content_hash(self, post: Dict) -> str:
        """Создание хеша содержимого поста для защиты от дублей"""
        content = ""
        
        # Добавляем текст
        content += post.get('text', '') or post.get('caption', '')
        
        # Добавляем информацию о медиа
        if 'photo' in post:
            content += f"photo_{len(post['photo'])}"
        if 'video' in post:
            content += f"video_{post['video']['file_id'][:10]}"
        if 'document' in post:
            content += f"doc_{post['document']['file_id'][:10]}"
        
        # Создаем хеш
        return hashlib.md5(content.encode()).hexdigest()

    def group_media_posts(self, posts: List[Dict]) -> List[Dict]:
        """Группировка постов из одной медиагруппы"""
        grouped = {}
        standalone = []
        
        for post in posts:
            if post.get('media_group_id'):
                group_id = post['media_group_id']
                if group_id not in grouped:
                    grouped[group_id] = post
                    grouped[group_id]['media'] = []
                # Собираем все медиа из группы
                grouped[group_id]['media'].extend(post['media'])
            else:
                standalone.append(post)
        
        # Объединяем результаты
        return standalone + list(grouped.values())

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
            media.append({'type': 'doc', 'file_id': post['document']['file_id']})
        
        # Проверяем на наличие подписи к медиа
        if 'caption' in post and post['caption']:
            media[0]['caption'] = post['caption'] if media else None
        
        return media

    def is_duplicate(self, post: Dict) -> bool:
        """Проверка на дубликат поста"""
        # Проверка по ID
        if post['id'] in self.processed_ids:
            logger.info(f"Пост {post['id']} уже был опубликован (проверка по ID)")
            return True
        
        # Проверка по хешу содержимого (для постов с одинаковым контентом)
        if post['hash']:
            try:
                if os.path.exists(self.processed_ids_file):
                    with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for saved_id, saved_data in data.items():
                            if isinstance(saved_data, dict) and saved_data.get('hash') == post['hash']:
                                # Проверяем, не слишком ли старый пост
                                timestamp = datetime.fromisoformat(saved_data['timestamp'])
                                if datetime.now() - timestamp < timedelta(days=7):
                                    logger.info(f"Пост {post['id']} дублирует содержимое поста {saved_id}")
                                    return True
            except Exception as e:
                logger.error(f"Ошибка проверки дубликата по хешу: {e}")
        
        return False

    def download_telegram_file(self, file_id: str) -> str:
        """Скачивание файла из Telegram"""
        try:
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
        except Exception as e:
            logger.error(f"Ошибка скачивания файла {file_id}: {e}")
        return None

    def upload_to_vk(self, file_path: str, file_type: str) -> str:
        """Загрузка медиа в VK"""
        try:
            if file_type == 'photo':
                # Получаем URL для загрузки фото
                url = 'https://api.vk.com/method/photos.getWallUploadServer'
                params = {
                    'group_id': self.vk_group_id,
                    'access_token': self.vk_token,
                    'v': '5.131'
                }
            elif file_type == 'video':
                url = 'https://api.vk.com/method/video.save'
                params = {
                    'group_id': self.vk_group_id,
                    'name': os.path.basename(file_path),
                    'access_token': self.vk_token,
                    'v': '5.131'
                }
            else:  # document
                url = 'https://api.vk.com/method/docs.getWallUploadServer'
                params = {
                    'group_id': self.vk_group_id,
                    'access_token': self.vk_token,
                    'v': '5.131'
                }
            
            response = requests.get(url, params=params)
            response_data = response.json()
            
            if 'error' in response_data:
                logger.error(f"Ошибка VK API: {response_data['error']}")
                return None
            
            upload_url = response_data['response']['upload_url']
            
            # Загружаем файл
            with open(file_path, 'rb') as f:
                files = {'file': f}
                upload_response = requests.post(upload_url, files=files)
            
            upload_data = upload_response.json()
            
            # Сохраняем загруженный файл
            if file_type == 'photo':
                save_url = 'https://api.vk.com/method/photos.saveWallPhoto'
                params = {
                    'group_id': self.vk_group_id,
                    'photo': upload_data['photo'],
                    'server': upload_data['server'],
                    'hash': upload_data['hash'],
                    'access_token': self.vk_token,
                    'v': '5.131'
                }
                save_response = requests.post(save_url, params=params)
                save_data = save_response.json()
                
                if 'error' not in save_data:
                    photo = save_data['response'][0]
                    return f"photo{photo['owner_id']}_{photo['id']}"
                    
            elif file_type == 'video':
                return f"video{upload_data['response']['owner_id']}_{upload_data['response']['video_id']}"
                
            else:  # document
                save_url = 'https://api.vk.com/method/docs.save'
                params = {
                    'file': upload_data['file'],
                    'access_token': self.vk_token,
                    'v': '5.131'
                }
                save_response = requests.post(save_url, params=params)
                save_data = save_response.json()
                
                if 'error' not in save_data:
                    doc = save_data['response'][0]
                    return f"doc{doc['owner_id']}_{doc['id']}"
            
        except Exception as e:
            logger.error(f"Ошибка загрузки в VK: {e}")
        
        return None

    def publish_to_vk(self, text: str, attachments: List[str] = None) -> Dict:
        """Публикация поста в VK"""
        url = 'https://api.vk.com/method/wall.post'
        
        # Обрезаем текст до лимита VK (10,000 символов)
        if len(text) > 10000:
            text = text[:9997] + "..."
        
        params = {
            'owner_id': f'-{self.vk_group_id}',
            'from_group': 1,
            'message': text,
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        if attachments:
            params['attachments'] = ','.join(attachments)
        
        try:
            response = requests.post(url, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка публикации в VK: {e}")
            return {'error': str(e)}

    def process_new_posts(self):
        """Основная логика обработки новых постов"""
        logger.info("=" * 50)
        logger.info("Начинаем проверку новых постов...")
        logger.info(f"Канал: {self.telegram_channel}")
        logger.info(f"Группа VK: {self.vk_group_id}")
        logger.info("=" * 50)
        
        # Получаем посты из Telegram
        posts = self.get_telegram_posts(limit=15)  # Увеличил лимит для надежности
        
        if not posts:
            logger.info("Постов не найдено")
            return
        
        # Фильтруем новые посты
        new_posts = [p for p in posts if not self.is_duplicate(p)]
        
        if not new_posts:
            logger.info("Новых постов не найдено")
            return
        
        logger.info(f"Найдено {len(new_posts)} новых постов из {len(posts)} полученных")
        
        published_count = 0
        # Обрабатываем каждый новый пост
        for i, post in enumerate(new_posts, 1):
            try:
                logger.info(f"Обработка поста {i}/{len(new_posts)} (ID: {post['id']})")
                
                attachments = []
                
                # Скачиваем и загружаем медиа
                for media_item in post['media']:
                    logger.info(f"Скачивание медиа: {media_item['type']}")
                    file_path = self.download_telegram_file(media_item['file_id'])
                    
                    if file_path:
                        attachment = self.upload_to_vk(file_path, media_item['type'])
                        if attachment:
                            attachments.append(attachment)
                            logger.info(f"Медиа загружено в VK: {attachment}")
                        
                        # Удаляем временный файл
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                # Текст поста (если есть caption, используем его)
                text = post.get('caption') or post.get('text') or ''
                
                # Если есть подпись к медиа, добавляем её
                if post['media'] and 'caption' in post['media'][0]:
                    text = post['media'][0]['caption']
                
                # Публикуем в VK
                if text or attachments:  # Публикуем только если есть текст или медиа
                    logger.info(f"Публикация в VK...")
                    result = self.publish_to_vk(text, attachments)
                    
                    if 'error' not in result:
                        # Сохраняем ID обработанного поста
                        self.save_processed_id(post['id'], post.get('hash'))
                        published_count += 1
                        logger.info(f"✅ Пост успешно опубликован!")
                        
                        # Добавляем ссылку на пост в лог
                        if 'response' in result:
                            post_id = result['response']['post_id']
                            logger.info(f"   Ссылка: https://vk.com/wall-{self.vk_group_id}_{post_id}")
                    else:
                        logger.error(f"❌ Ошибка публикации: {result['error']}")
                else:
                    logger.warning(f"Пост {post['id']} пустой, пропускаем")
                    
                # Небольшая задержка между постами
                if i < len(new_posts):
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки поста {post['id']}: {e}")
                continue
        
        logger.info("=" * 50)
        logger.info(f"Обработка завершена. Опубликовано: {published_count}/{len(new_posts)}")
        logger.info("=" * 50)
        
        # Очищаем временную папку
        self.cleanup_temp_files()

    def cleanup_temp_files(self):
        """Очистка временных файлов"""
        try:
            for file in os.listdir(self.temp_dir):
                file_path = os.path.join(self.temp_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    logger.error(f"Ошибка удаления {file_path}: {e}")
            logger.debug("Временные файлы очищены")
        except Exception as e:
            logger.error(f"Ошибка очистки временной папки: {e}")

if __name__ == "__main__":
    try:
        publisher = TelegramToVKPublisher()
        publisher.process_new_posts()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
