import os
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
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
        self.telegram_channel = os.getenv('TELEGRAM_CHANNEL_ID')
        self.vk_token = os.getenv('VK_GROUP_TOKEN')
        self.vk_group_id = os.getenv('VK_GROUP_ID')
        
        # Проверка наличия всех переменных
        if not all([self.telegram_token, self.telegram_channel, self.vk_token, self.vk_group_id]):
            raise ValueError("Отсутствуют необходимые переменные окружения!")
        
        # Файл для хранения ID обработанных постов
        self.processed_ids_file = 'processed_posts.json'
        self.load_processed_ids()
        
        # Временная папка для медиа
        self.temp_dir = 'temp_media'
        os.makedirs(self.temp_dir, exist_ok=True)
        
        logger.info(f"✅ Инициализация завершена. Канал: {self.telegram_channel}")

    def load_processed_ids(self):
        """Загрузка ID обработанных постов"""
        try:
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    self.processed_data = json.load(f)
            else:
                self.processed_data = {}
        except Exception as e:
            logger.error(f"Ошибка загрузки processed_ids: {e}")
            self.processed_data = {}

    def save_processed_id(self, message_id: str, message_hash: str = None):
        """Сохранение ID обработанного поста"""
        try:
            # Сохраняем с временной меткой
            self.processed_data[message_id] = {
                'timestamp': datetime.now().isoformat(),
                'hash': message_hash
            }
            
            # Оставляем только последние 1000 записей
            if len(self.processed_data) > 1000:
                sorted_items = sorted(
                    self.processed_data.items(), 
                    key=lambda x: x[1]['timestamp'], 
                    reverse=True
                )
                self.processed_data = dict(sorted_items[:1000])
            
            with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"💾 Сохранен ID {message_id}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения processed_id: {e}")

    def get_channel_id(self) -> str:
        """Получение числового ID канала"""
        if self.telegram_channel.startswith('@'):
            url = f"https://api.telegram.org/bot{self.telegram_token}/getChat"
            params = {'chat_id': self.telegram_channel}
            
            try:
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                if data.get('ok'):
                    chat_id = str(data['result']['id'])
                    logger.info(f"📢 ID канала: {chat_id}")
                    return chat_id
            except Exception as e:
                logger.error(f"Ошибка получения ID канала: {e}")
        
        return self.telegram_channel

    def get_telegram_posts(self, limit: int = 15) -> List[Dict]:
        """Получение последних постов ТОЛЬКО из указанного канала"""
        channel_id = self.get_channel_id()
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/getUpdates"
        
        params = {
            'timeout': 30,
            'limit': limit,
            'allowed_updates': ['channel_post']
        }
        
        try:
            response = requests.get(url, params=params, timeout=35)
            data = response.json()
            
            posts_dict = {}  # Используем словарь для уникальности по message_id
            
            if data.get('ok'):
                for update in data.get('result', []):
                    if 'channel_post' in update:
                        post = update['channel_post']
                        
                        # Проверяем, что пост именно из нашего канала
                        post_chat_id = str(post['chat']['id'])
                        post_chat_username = post['chat'].get('username', '')
                        
                        is_our_channel = (
                            post_chat_id == channel_id or 
                            f"@{post_chat_username}" == self.telegram_channel
                        )
                        
                        if is_our_channel:
                            # ВАЖНО: Используем message_id как основной идентификатор!
                            message_id = str(post['message_id'])
                            chat_id = post_chat_id
                            
                            # Создаем уникальный ID на основе chat_id и message_id
                            unique_id = f"{chat_id}_{message_id}"
                            
                            # Если такой пост уже есть в словаре, пропускаем
                            if unique_id in posts_dict:
                                continue
                            
                            # Создаем хеш содержимого
                            content_hash = self.create_content_hash(post)
                            
                            posts_dict[unique_id] = {
                                'id': unique_id,
                                'message_id': message_id,
                                'chat_id': chat_id,
                                'chat_username': post_chat_username,
                                'text': post.get('text', ''),
                                'caption': post.get('caption', ''),
                                'date': post['date'],
                                'media_group_id': post.get('media_group_id'),
                                'media': self.extract_media(post),
                                'hash': content_hash
                            }
                            
                            logger.debug(f"📝 Найден пост {unique_id}")
            
            # Преобразуем словарь в список
            posts = list(posts_dict.values())
            
            # Группируем посты из одной медиагруппы
            posts = self.group_media_posts(posts)
            
            # Сортируем по дате (новые сверху)
            posts.sort(key=lambda x: x['date'], reverse=True)
            
            logger.info(f"📥 Получено {len(posts)} уникальных постов")
            return posts
            
        except Exception as e:
            logger.error(f"Ошибка получения постов из Telegram: {e}")
            return []

    def create_content_hash(self, post: Dict) -> str:
        """Создание хеша содержимого поста"""
        content = ""
        
        # Добавляем текст
        content += post.get('text', '') or post.get('caption', '')
        
        # Добавляем информацию о медиа
        if 'photo' in post:
            content += f"photo_{len(post['photo'])}"
        if 'video' in post:
            content += f"video_{post['video']['file_id']}"
        if 'document' in post:
            content += f"doc_{post['document']['file_id']}"
        
        return hashlib.md5(content.encode()).hexdigest() if content else ""

    def group_media_posts(self, posts: List[Dict]) -> List[Dict]:
        """Группировка постов из одной медиагруппы"""
        groups = {}
        standalone = []
        
        for post in posts:
            if post.get('media_group_id'):
                group_id = post['media_group_id']
                if group_id not in groups:
                    # Берем первый пост как основной
                    groups[group_id] = post.copy()
                    groups[group_id]['media'] = post.get('media', []).copy()
                    groups[group_id]['media_group_posts'] = [post['id']]
                else:
                    # Добавляем медиа из дополнительных постов
                    groups[group_id]['media'].extend(post.get('media', []))
                    groups[group_id]['media_group_posts'].append(post['id'])
            else:
                standalone.append(post)
        
        # Объединяем
        result = standalone + list(groups.values())
        
        # Удаляем дубликаты медиа в группах
        for item in result:
            if 'media' in item:
                # Удаляем дубликаты по file_id
                seen = set()
                unique_media = []
                for media in item['media']:
                    if media['file_id'] not in seen:
                        seen.add(media['file_id'])
                        unique_media.append(media)
                item['media'] = unique_media
        
        return result

    def extract_media(self, post: Dict) -> List[Dict]:
        """Извлечение медиа из поста"""
        media = []
        
        if 'photo' in post and post['photo']:
            file_id = post['photo'][-1]['file_id']
            media.append({'type': 'photo', 'file_id': file_id})
        
        if 'video' in post:
            media.append({'type': 'video', 'file_id': post['video']['file_id']})
        
        if 'document' in post:
            media.append({'type': 'doc', 'file_id': post['document']['file_id']})
        
        return media

    def is_duplicate(self, post: Dict) -> bool:
        """Проверка на дубликат поста"""
        # Основная проверка по ID
        if post['id'] in self.processed_data:
            logger.info(f"⏭️ Пост {post['id']} уже был опубликован (ID)")
            return True
        
        # Проверка по хешу (для одинакового контента)
        if post.get('hash'):
            for saved_id, saved_data in self.processed_data.items():
                if isinstance(saved_data, dict) and saved_data.get('hash') == post['hash']:
                    # Проверяем, что пост не слишком старый
                    timestamp = datetime.fromisoformat(saved_data['timestamp'])
                    if datetime.now() - timestamp < timedelta(days=1):
                        logger.info(f"⏭️ Пост {post['id']} дублирует содержимое {saved_id}")
                        return True
        
        return False

    def download_telegram_file(self, file_id: str) -> Optional[str]:
        """Скачивание файла из Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/getFile"
            response = requests.get(url, params={'file_id': file_id}, timeout=30)
            data = response.json()
            
            if data.get('ok'):
                file_path = data['result']['file_path']
                file_url = f"https://api.telegram.org/file/bot{self.telegram_token}/{file_path}"
                
                local_filename = os.path.join(self.temp_dir, file_path.split('/')[-1])
                
                # Проверяем, не скачан ли уже файл
                if os.path.exists(local_filename):
                    return local_filename
                
                response = requests.get(file_url, timeout=60)
                response.raise_for_status()
                
                with open(local_filename, 'wb') as f:
                    f.write(response.content)
                
                return local_filename
        except Exception as e:
            logger.error(f"Ошибка скачивания файла: {e}")
        return None

    def upload_photo_to_vk(self, file_path: str) -> Optional[str]:
        """Загрузка фото в VK"""
        try:
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
                logger.error(f"Ошибка VK API: {data['error']}")
                return None
            
            upload_url = data['response']['upload_url']
            
            # Загружаем фото
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
            
            if 'error' not in save_data:
                photo = save_data['response'][0]
                return f"photo{photo['owner_id']}_{photo['id']}"
            
        except Exception as e:
            logger.error(f"Ошибка загрузки фото: {e}")
        return None

    def upload_to_vk(self, file_path: str, file_type: str) -> Optional[str]:
        """Загрузка медиа в VK"""
        if file_type == 'photo':
            return self.upload_photo_to_vk(file_path)
        else:
            logger.warning(f"Тип {file_type} пока не поддерживается")
            return None

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
        
        try:
            response = requests.post(url, params=params, timeout=30)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка публикации: {e}")
            return {'error': str(e)}

    def process_new_posts(self):
        """Основная логика обработки новых постов"""
        logger.info("=" * 60)
        logger.info("🚀 НАЧАЛО ПРОВЕРКИ НОВЫХ ПОСТОВ")
        logger.info("=" * 60)
        
        # Получаем посты из Telegram
        posts = self.get_telegram_posts(limit=15)
        
        if not posts:
            logger.info("📭 Постов не найдено")
            return
        
        # Фильтруем новые посты
        new_posts = []
        for post in posts:
            if not self.is_duplicate(post):
                new_posts.append(post)
            else:
                logger.debug(f"Пост {post['id']} пропущен (дубликат)")
        
        if not new_posts:
            logger.info("📭 Новых постов не найдено")
            return
        
        logger.info(f"📊 Найдено {len(new_posts)} новых постов")
        
        published_count = 0
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
                            logger.info(f"✅ Медиа загружено: {attachment}")
                        
                        # Удаляем временный файл
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                # Текст поста
                text = post.get('caption') or post.get('text') or ''
                
                # Публикуем в VK
                if text or attachments:
                    logger.info(f"📤 Публикация в VK...")
                    result = self.publish_to_vk(text, attachments)
                    
                    if 'response' in result:
                        # Сохраняем ID обработанного поста
                        self.save_processed_id(post['id'], post.get('hash'))
                        published_count += 1
                        
                        post_id = result['response']['post_id']
                        vk_url = f"https://vk.com/wall-{self.vk_group_id}_{post_id}"
                        logger.info(f"✅ Пост опубликован: {vk_url}")
                    else:
                        logger.error(f"❌ Ошибка публикации: {result}")
                else:
                    logger.warning(f"⚠️ Пост пустой, пропускаем")
                
                # Задержка между постами
                if i < len(new_posts):
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки поста: {e}")
                continue
        
        logger.info("=" * 60)
        logger.info(f"✅ Обработка завершена. Опубликовано: {published_count}/{len(new_posts)}")
        logger.info("=" * 60)

if __name__ == "__main__":
    try:
        publisher = TelegramToVKPublisher()
        publisher.process_new_posts()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
