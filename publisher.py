import os
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
import logging
from functools import wraps

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def retry_on_failure(max_retries=3, delay=5):
    """Декоратор для повторных попыток при ошибках"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Ошибка после {max_retries} попыток: {e}")
                        raise
                    logger.warning(f"Попытка {attempt + 1} не удалась: {e}. Повтор через {delay}с")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

class TelegramToVKPublisher:
    def __init__(self):
        # Токены и ID
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_channel = os.getenv('TELEGRAM_CHANNEL_ID')
        self.vk_token = os.getenv('VK_GROUP_TOKEN')
        self.vk_group_id = os.getenv('VK_GROUP_ID')
        
        # Проверка наличия всех переменных
        missing_vars = []
        if not self.telegram_token:
            missing_vars.append('TELEGRAM_BOT_TOKEN')
        if not self.telegram_channel:
            missing_vars.append('TELEGRAM_CHANNEL_ID')
        if not self.vk_token:
            missing_vars.append('VK_GROUP_TOKEN')
        if not self.vk_group_id:
            missing_vars.append('VK_GROUP_ID')
            
        if missing_vars:
            raise ValueError(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        
        # Файл для хранения ID обработанных постов
        self.processed_ids_file = 'processed_posts.json'
        self.processed_ids = self.load_processed_ids()
        
        # Временная папка для медиа
        self.temp_dir = 'temp_media'
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Статистика
        self.stats = {
            'processed': 0,
            'published': 0,
            'errors': 0,
            'duplicates': 0
        }
        
        logger.info(f"✅ Инициализация завершена. Канал: {self.telegram_channel}, Группа VK: {self.vk_group_id}")

    def load_processed_ids(self) -> Set[str]:
        """Загрузка ID обработанных постов"""
        try:
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Очищаем старые записи (старше 30 дней)
                    cutoff = datetime.now() - timedelta(days=30)
                    
                    # Конвертируем старый формат в новый если нужно
                    if isinstance(data, list):
                        processed = {msg_id: {'timestamp': datetime.now().isoformat()} 
                                   for msg_id in data}
                    else:
                        processed = data
                    
                    # Фильтруем старые записи
                    processed = {
                        msg_id: info 
                        for msg_id, info in processed.items() 
                        if datetime.fromisoformat(info['timestamp']) > cutoff
                    }
                    
                    # Сохраняем обновленный список
                    with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                        json.dump(processed, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"📚 Загружено {len(processed)} обработанных постов")
                    return set(processed.keys())
        except json.JSONDecodeError:
            logger.warning("Файл processed_posts.json поврежден, создаем новый")
        except Exception as e:
            logger.error(f"Ошибка загрузки processed_ids: {e}")
        
        return set()

    def save_processed_id(self, message_id: str, message_hash: str = None):
        """Сохранение ID обработанного поста"""
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
                sorted_items = sorted(
                    data.items(), 
                    key=lambda x: x[1]['timestamp'], 
                    reverse=True
                )
                data = dict(sorted_items[:1000])
            
            with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.processed_ids.add(message_id)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения processed_id: {e}")

    @retry_on_failure(max_retries=3)
    def get_channel_info(self) -> Dict:
        """Получение информации о канале"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/getChat"
        params = {'chat_id': self.telegram_channel}
        
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if not data.get('ok'):
            raise Exception(f"Telegram API error: {data}")
        
        return data['result']

    def get_channel_id(self) -> str:
        """Получение числового ID канала"""
        if self.telegram_channel.startswith('@'):
            try:
                channel_info = self.get_channel_info()
                chat_id = str(channel_info['id'])
                logger.info(f"📢 Получен ID канала: {chat_id}")
                return chat_id
            except Exception as e:
                logger.error(f"Не удалось получить ID канала: {e}")
        
        return self.telegram_channel

    @retry_on_failure(max_retries=3)
    def get_telegram_updates(self, limit: int = 15) -> List[Dict]:
        """Получение обновлений из Telegram"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/getUpdates"
        
        params = {
            'timeout': 30,
            'limit': limit,
            'allowed_updates': ['channel_post']
        }
        
        response = requests.get(url, params=params, timeout=35)
        data = response.json()
        
        if not data.get('ok'):
            raise Exception(f"Telegram API error: {data}")
        
        return data.get('result', [])

    def get_telegram_posts(self, limit: int = 15) -> List[Dict]:
        """Получение последних постов ТОЛЬКО из указанного канала"""
        channel_id = self.get_channel_id()
        
        try:
            updates = self.get_telegram_updates(limit)
            
            posts = []
            for update in updates:
                if 'channel_post' not in update:
                    continue
                    
                post = update['channel_post']
                
                # Проверяем, что пост именно из нашего канала
                post_chat_id = str(post['chat']['id'])
                post_chat_username = post['chat'].get('username', '')
                
                is_our_channel = (
                    post_chat_id == channel_id or 
                    f"@{post_chat_username}" == self.telegram_channel
                )
                
                if is_our_channel:
                    unique_id = f"{post_chat_id}_{post['message_id']}"
                    content_hash = self.create_content_hash(post)
                    
                    post_data = {
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
                    }
                    
                    posts.append(post_data)
                    logger.debug(f"Найден пост {unique_id}")
            
            # Группируем посты из одной медиагруппы
            posts = self.group_media_posts(posts)
            
            logger.info(f"📥 Получено {len(posts)} постов из канала {self.telegram_channel}")
            return posts
            
        except Exception as e:
            logger.error(f"Ошибка получения постов из Telegram: {e}")
            return []

    def create_content_hash(self, post: Dict) -> str:
        """Создание хеша содержимого поста"""
        content_parts = []
        
        # Добавляем текст
        content_parts.append(post.get('text', '') or post.get('caption', ''))
        
        # Добавляем информацию о медиа
        if 'photo' in post:
            content_parts.append(f"photo_{len(post['photo'])}")
        if 'video' in post:
            content_parts.append(f"video_{post['video']['file_id'][:20]}")
        if 'document' in post:
            content_parts.append(f"doc_{post['document']['file_id'][:20]}")
        
        content = ''.join(content_parts)
        return hashlib.md5(content.encode()).hexdigest() if content else None

    def group_media_posts(self, posts: List[Dict]) -> List[Dict]:
        """Группировка постов из одной медиагруппы"""
        grouped = {}
        standalone = []
        
        for post in posts:
            if post.get('media_group_id'):
                group_id = post['media_group_id']
                if group_id not in grouped:
                    grouped[group_id] = post.copy()
                    grouped[group_id]['media'] = []
                # Собираем все медиа из группы
                grouped[group_id]['media'].extend(post.get('media', []))
            else:
                standalone.append(post)
        
        # Удаляем дубликаты медиа в группах
        for group in grouped.values():
            unique_media = []
            seen_files = set()
            for media in group['media']:
                if media['file_id'] not in seen_files:
                    seen_files.add(media['file_id'])
                    unique_media.append(media)
            group['media'] = unique_media
        
        return standalone + list(grouped.values())

    def extract_media(self, post: Dict) -> List[Dict]:
        """Извлечение медиа из поста"""
        media = []
        
        try:
            # Фото
            if 'photo' in post and post['photo']:
                # Берем самую большую версию фото
                file_id = post['photo'][-1]['file_id']
                media.append({'type': 'photo', 'file_id': file_id})
            
            # Видео
            if 'video' in post:
                media.append({
                    'type': 'video', 
                    'file_id': post['video']['file_id']
                })
            
            # Документы
            if 'document' in post:
                media.append({
                    'type': 'doc', 
                    'file_id': post['document']['file_id']
                })
            
            # Подпись к медиа
            if 'caption' in post and post['caption'] and media:
                media[0]['caption'] = post['caption']
                
        except Exception as e:
            logger.error(f"Ошибка извлечения медиа: {e}")
        
        return media

    def is_duplicate(self, post: Dict) -> bool:
        """Проверка на дубликат поста"""
        # Проверка по ID
        if post['id'] in self.processed_ids:
            logger.info(f"⏭️ Пост {post['id']} уже был опубликован (ID)")
            self.stats['duplicates'] += 1
            return True
        
        # Проверка по хешу содержимого
        if post.get('hash'):
            try:
                if os.path.exists(self.processed_ids_file):
                    with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for saved_id, saved_data in data.items():
                            if isinstance(saved_data, dict) and saved_data.get('hash') == post['hash']:
                                timestamp = datetime.fromisoformat(saved_data['timestamp'])
                                if datetime.now() - timestamp < timedelta(days=7):
                                    logger.info(f"⏭️ Пост {post['id']} дублирует содержимое поста {saved_id}")
                                    self.stats['duplicates'] += 1
                                    return True
            except Exception as e:
                logger.error(f"Ошибка проверки дубликата по хешу: {e}")
        
        return False

    @retry_on_failure(max_retries=3)
    def download_telegram_file(self, file_id: str) -> Optional[str]:
        """Скачивание файла из Telegram"""
        # Получаем путь к файлу
        url = f"https://api.telegram.org/bot{self.telegram_token}/getFile"
        response = requests.get(url, params={'file_id': file_id}, timeout=30)
        data = response.json()
        
        if not data.get('ok'):
            raise Exception(f"Failed to get file info: {data}")
        
        file_path = data['result']['file_path']
        file_url = f"https://api.telegram.org/file/bot{self.telegram_token}/{file_path}"
        
        # Скачиваем файл
        local_filename = os.path.join(self.temp_dir, file_path.split('/')[-1])
        
        # Проверяем, не скачан ли уже файл
        if os.path.exists(local_filename):
            return local_filename
        
        response = requests.get(file_url, timeout=60)
        response.raise_for_status()
        
        with open(local_filename, 'wb') as f:
            f.write(response.content)
        
        logger.debug(f"📎 Скачан файл: {local_filename}")
        return local_filename

    @retry_on_failure(max_retries=3)
    def upload_photo_to_vk(self, file_path: str) -> Optional[str]:
        """Загрузка фото в VK"""
        # Получаем URL для загрузки
        url = 'https://api.vk.com/method/photos.getWallUploadServer'
        params = {
            'group_id': self.vk_group_id,
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"VK API error: {data['error']}")
        
        upload_url = data['response']['upload_url']
        
        # Загружаем файл
        with open(file_path, 'rb') as f:
            files = {'photo': f}
            upload_response = requests.post(upload_url, files=files, timeout=60)
        
        upload_data = upload_response.json()
        
        # Сохраняем фото
        save_url = 'https://api.vk.com/method/photos.saveWallPhoto'
        params = {
            'group_id': self.vk_group_id,
            'photo': upload_data['photo'],
            'server': upload_data['server'],
            'hash': upload_data['hash'],
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        save_response = requests.post(save_url, params=params, timeout=30)
        save_data = save_response.json()
        
        if 'error' in save_data:
            raise Exception(f"Failed to save photo: {save_data['error']}")
        
        photo = save_data['response'][0]
        return f"photo{photo['owner_id']}_{photo['id']}"

    @retry_on_failure(max_retries=3)
    def upload_video_to_vk(self, file_path: str) -> Optional[str]:
        """Загрузка видео в VK"""
        url = 'https://api.vk.com/method/video.save'
        params = {
            'group_id': self.vk_group_id,
            'name': os.path.basename(file_path),
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"VK API error: {data['error']}")
        
        upload_url = data['response']['upload_url']
        
        # Загружаем видео
        with open(file_path, 'rb') as f:
            files = {'video_file': f}
            upload_response = requests.post(upload_url, files=files, timeout=300)  # 5 минут на видео
        
        upload_data = upload_response.json()
        
        return f"video{upload_data['owner_id']}_{upload_data['video_id']}"

    @retry_on_failure(max_retries=3)
    def upload_doc_to_vk(self, file_path: str) -> Optional[str]:
        """Загрузка документа в VK"""
        url = 'https://api.vk.com/method/docs.getWallUploadServer'
        params = {
            'group_id': self.vk_group_id,
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"VK API error: {data['error']}")
        
        upload_url = data['response']['upload_url']
        
        # Загружаем документ
        with open(file_path, 'rb') as f:
            files = {'file': f}
            upload_response = requests.post(upload_url, files=files, timeout=60)
        
        upload_data = upload_response.json()
        
        # Сохраняем документ
        save_url = 'https://api.vk.com/method/docs.save'
        params = {
            'file': upload_data['file'],
            'access_token': self.vk_token,
            'v': '5.131'
        }
        
        save_response = requests.post(save_url, params=params, timeout=30)
        save_data = save_response.json()
        
        if 'error' in save_data:
            raise Exception(f"Failed to save doc: {save_data['error']}")
        
        doc = save_data['response'][0]
        return f"doc{doc['owner_id']}_{doc['id']}"

    def upload_to_vk(self, file_path: str, file_type: str) -> Optional[str]:
        """Загрузка медиа в VK"""
        uploaders = {
            'photo': self.upload_photo_to_vk,
            'video': self.upload_video_to_vk,
            'doc': self.upload_doc_to_vk
        }
        
        if file_type not in uploaders:
            logger.error(f"Неподдерживаемый тип файла: {file_type}")
            return None
        
        try:
            attachment = uploaders[file_type](file_path)
            if attachment:
                logger.debug(f"✅ Загружено в VK: {attachment}")
            return attachment
        except Exception as e:
            logger.error(f"Ошибка загрузки {file_type}: {e}")
            return None

    @retry_on_failure(max_retries=3)
    def publish_to_vk(self, text: str, attachments: List[str] = None) -> Dict:
        """Публикация поста в VK"""
        url = 'https://api.vk.com/method/wall.post'
        
        # Обрезаем текст до лимита VK
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
        
        response = requests.post(url, params=params, timeout=30)
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"VK API error: {data['error']}")
        
        return data

    def process_new_posts(self):
        """Основная логика обработки новых постов"""
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("🚀 НАЧАЛО ПРОВЕРКИ НОВЫХ ПОСТОВ")
        logger.info("=" * 60)
        logger.info(f"📱 Канал Telegram: {self.telegram_channel}")
        logger.info(f"👥 Группа VK: {self.vk_group_id}")
        logger.info("=" * 60)
        
        # Получаем посты из Telegram
        posts = self.get_telegram_posts(limit=15)
        
        if not posts:
            logger.info("📭 Постов не найдено")
            return
        
        # Фильтруем новые посты
        new_posts = [p for p in posts if not self.is_duplicate(p)]
        
        if not new_posts:
            logger.info("📭 Новых постов не найдено")
            self.print_stats(start_time)
            return
        
        logger.info(f"📊 Найдено {len(new_posts)} новых постов из {len(posts)} полученных")
        
        # Обрабатываем каждый новый пост
        for i, post in enumerate(new_posts, 1):
            try:
                logger.info(f"\n{'─' * 40}")
                logger.info(f"📝 Обработка поста {i}/{len(new_posts)}")
                logger.info(f"🆔 ID: {post['id']}")
                
                attachments = []
                
                # Скачиваем и загружаем медиа
                for media_item in post.get('media', []):
                    logger.info(f"📎 Скачивание {media_item['type']}...")
                    file_path = self.download_telegram_file(media_item['file_id'])
                    
                    if file_path:
                        logger.info(f"☁️ Загрузка в VK...")
                        attachment = self.upload_to_vk(file_path, media_item['type'])
                        if attachment:
                            attachments.append(attachment)
                        
                        # Удаляем временный файл
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                # Определяем текст поста
                text = post.get('caption') or post.get('text') or ''
                
                # Публикуем в VK
                if text or attachments:
                    logger.info(f"📤 Публикация в VK...")
                    result = self.publish_to_vk(text, attachments)
                    
                    if 'response' in result:
                        self.save_processed_id(post['id'], post.get('hash'))
                        self.stats['published'] += 1
                        
                        post_id = result['response']['post_id']
                        vk_url = f"https://vk.com/wall-{self.vk_group_id}_{post_id}"
                        logger.info(f"✅ Пост опубликован: {vk_url}")
                    else:
                        self.stats['errors'] += 1
                        logger.error(f"❌ Ошибка публикации: {result}")
                else:
                    logger.warning(f"⚠️ Пост пустой, пропускаем")
                
                self.stats['processed'] += 1
                
                # Задержка между постами
                if i < len(new_posts):
                    time.sleep(3)
                
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"❌ Ошибка обработки поста {post['id']}: {e}")
                continue
        
        self.print_stats(start_time)
        self.cleanup_temp_files()

    def print_stats(self, start_time: float):
        """Вывод статистики"""
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 СТАТИСТИКА ВЫПОЛНЕНИЯ")
        logger.info("=" * 60)
        logger.info(f"⏱️  Время выполнения: {minutes} мин {seconds} сек")
        logger.info(f"📊 Обработано постов: {self.stats['processed']}")
        logger.info(f"✅ Опубликовано: {self.stats['published']}")
        logger.info(f"⏭️  Дубликатов: {self.stats['duplicates']}")
        logger.info(f"❌ Ошибок: {self.stats['errors']}")
        logger.info("=" * 60)

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
            
            if os.path.exists(self.temp_dir):
                os.rmdir(self.temp_dir)
                logger.debug("🧹 Временные файлы очищены")
        except Exception as e:
            logger.error(f"Ошибка очистки временной папки: {e}")

def main():
    """Основная функция"""
    try:
        publisher = TelegramToVKPublisher()
        publisher.process_new_posts()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
