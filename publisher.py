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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
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
        
        # Типы контента, которые игнорируем
        self.ignored_content_types = ['sticker', 'animation', 'voice', 'audio', 'poll']
        
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

    def is_ignored_content(self, post: Dict) -> bool:
        """Проверка, является ли контент игнорируемым (стикеры, голосовые и т.д.)"""
        # Проверяем наличие игнорируемых типов контента
        for content_type in self.ignored_content_types:
            if content_type in post:
                logger.info(f"🚫 Пропускаем {content_type} (ID: {post.get('message_id')})")
                return True
        
        # Проверяем, есть ли в посте только игнорируемый контент
        has_media = any(key in post for key in ['photo', 'video', 'document'])
        has_ignored = any(key in post for key in self.ignored_content_types)
        
        # Если есть только игнорируемый контент и нет текста - пропускаем
        if has_ignored and not has_media and not post.get('text') and not post.get('caption'):
            logger.info(f"🚫 Пост содержит только игнорируемый контент")
            return True
            
        return False

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
            
            posts_dict = {}
            
            if data.get('ok'):
                for update in data.get('result', []):
                    if 'channel_post' in update:
                        post = update['channel_post']
                        
                        # Пропускаем игнорируемый контент
                        if self.is_ignored_content(post):
                            continue
                        
                        # Проверяем, что пост именно из нашего канала
                        post_chat_id = str(post['chat']['id'])
                        post_chat_username = post['chat'].get('username', '')
                        
                        is_our_channel = (
                            post_chat_id == channel_id or 
                            f"@{post_chat_username}" == self.telegram_channel
                        )
                        
                        if is_our_channel:
                            message_id = str(post['message_id'])
                            chat_id = post_chat_id
                            unique_id = f"{chat_id}_{message_id}"
                            
                            if unique_id in posts_dict:
                                continue
                            
                            # Извлекаем медиа
                            media = self.extract_media(post)
                            
                            # Если нет медиа и нет текста - пропускаем
                            if not media and not post.get('text') and not post.get('caption'):
                                continue
                            
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
                                'media': media,
                                'hash': content_hash
                            }
            
            posts = list(posts_dict.values())
            posts = self.group_media_posts(posts)
            posts.sort(key=lambda x: x['date'], reverse=True)
            
            logger.info(f"📥 Получено {len(posts)} постов (игнорируемый контент исключен)")
            return posts
            
        except Exception as e:
            logger.error(f"Ошибка получения постов из Telegram: {e}")
            return []

    def create_content_hash(self, post: Dict) -> str:
        """Создание хеша содержимого поста"""
        content = post.get('text', '') or post.get('caption', '')
        
        # Добавляем информацию о медиа
        if 'photo' in post:
            content += f"photo_{len(post['photo'])}"
        if 'video' in post:
            content += f"video_{post['video']['file_id'][:20]}"
        if 'document' in post:
            # Для документов добавляем имя файла если есть
            file_name = post['document'].get('file_name', '')
            content += f"doc_{file_name}"
        
        return hashlib.md5(content.encode()).hexdigest() if content else ""

    def group_media_posts(self, posts: List[Dict]) -> List[Dict]:
        """Группировка постов из одной медиагруппы"""
        groups = {}
        standalone = []
        
        for post in posts:
            if post.get('media_group_id'):
                group_id = post['media_group_id']
                if group_id not in groups:
                    groups[group_id] = post.copy()
                    groups[group_id]['media'] = post.get('media', []).copy()
                else:
                    # Добавляем новые медиа
                    existing_files = {m['file_id'] for m in groups[group_id]['media']}
                    for media in post.get('media', []):
                        if media['file_id'] not in existing_files:
                            groups[group_id]['media'].append(media)
                    
                    # Объединяем текст
                    if post.get('caption') and not groups[group_id].get('caption'):
                        groups[group_id]['caption'] = post['caption']
                    if post.get('text') and not groups[group_id].get('text'):
                        groups[group_id]['text'] = post['text']
            else:
                standalone.append(post)
        
        return standalone + list(groups.values())

    def extract_media(self, post: Dict) -> List[Dict]:
        """Извлечение медиа из поста"""
        media = []
        
        # Фото
        if 'photo' in post and post['photo']:
            # Берем самую большую версию
            file_id = post['photo'][-1]['file_id']
            media.append({'type': 'photo', 'file_id': file_id})
        
        # Видео
        if 'video' in post:
            media.append({
                'type': 'video', 
                'file_id': post['video']['file_id']
            })
        
        # Документы (только изображения и видео, не архивы и т.д.)
        if 'document' in post:
            mime_type = post['document'].get('mime_type', '')
            # Загружаем только изображения и видео
            if mime_type.startswith(('image/', 'video/')):
                media.append({
                    'type': 'doc',
                    'file_id': post['document']['file_id'],
                    'mime_type': mime_type
                })
            else:
                logger.info(f"📄 Пропускаем документ {mime_type}")
        
        return media

    def is_duplicate(self, post: Dict) -> bool:
        """Проверка на дубликат поста"""
        # Проверка по ID
        if post['id'] in self.processed_data:
            return True
        
        # Проверка по хешу (только для постов за последний час)
        if post.get('hash'):
            for saved_id, saved_data in self.processed_data.items():
                if isinstance(saved_data, dict) and saved_data.get('hash') == post['hash']:
                    timestamp = datetime.fromisoformat(saved_data['timestamp'])
                    if datetime.now() - timestamp < timedelta(hours=1):
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
                
                # Создаем уникальное имя файла
                file_ext = os.path.splitext(file_path)[1]
                local_filename = os.path.join(self.temp_dir, f"{file_id}{file_ext}")
                
                # Проверяем, не скачан ли уже файл
                if os.path.exists(local_filename):
                    return local_filename
                
                response = requests.get(file_url, timeout=60)
                response.raise_for_status()
                
                with open(local_filename, 'wb') as f:
                    f.write(response.content)
                
                logger.debug(f"📎 Скачан файл: {local_filename}")
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
                attachment = f"photo{photo['owner_id']}_{photo['id']}"
                logger.debug(f"✅ Фото загружено: {attachment}")
                return attachment
            
        except Exception as e:
            logger.error(f"Ошибка загрузки фото: {e}")
        return None

    def upload_video_to_vk(self, file_path: str) -> Optional[str]:
        """Загрузка видео в VK"""
        try:
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
                logger.error(f"Ошибка VK API: {data['error']}")
                return None
            
            upload_url = data['response']['upload_url']
            
            # Загружаем видео
            with open(file_path, 'rb') as f:
                files = {'video_file': f}
                upload_response = requests.post(upload_url, files=files, timeout=300)
            
            upload_data = upload_response.json()
            
            attachment = f"video{upload_data['owner_id']}_{upload_data['video_id']}"
            logger.debug(f"✅ Видео загружено: {attachment}")
            return attachment
            
        except Exception as e:
            logger.error(f"Ошибка загрузки видео: {e}")
        return None

    def upload_to_vk(self, file_path: str, file_type: str, mime_type: str = None) -> Optional[str]:
        """Загрузка медиа в VK"""
        # Определяем тип по MIME если нужно
        if file_type == 'doc' and mime_type:
            if mime_type.startswith('image/'):
                file_type = 'photo'
            elif mime_type.startswith('video/'):
                file_type = 'video'
        
        # Загружаем соответствующий тип
        if file_type == 'photo':
            return self.upload_photo_to_vk(file_path)
        elif file_type == 'video':
            return self.upload_video_to_vk(file_path)
        else:
            logger.warning(f"⚠️ Тип {file_type} не поддерживается для загрузки")
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
        
        if attachments and len(attachments) > 0:
            # В VK максимум 10 вложений
            if len(attachments) > 10:
                attachments = attachments[:10]
            params['attachments'] = ','.join(attachments)
            logger.info(f"📎 Вложения: {len(attachments)} шт.")
        
        try:
            response = requests.post(url, params=params, timeout=30)
            data = response.json()
            
            if 'error' in data:
                logger.error(f"Ошибка VK API при публикации: {data['error']}")
                return data
            
            logger.info(f"✅ Пост опубликован в VK")
            return data
            
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
                
                attachments = []
                
                # Скачиваем и загружаем медиа
                for media_item in post.get('media', []):
                    logger.info(f"📎 Скачивание {media_item['type']}...")
                    file_path = self.download_telegram_file(media_item['file_id'])
                    
                    if file_path:
                        logger.info(f"☁️ Загрузка в VK...")
                        attachment = self.upload_to_vk(
                            file_path, 
                            media_item['type'],
                            media_item.get('mime_type')
                        )
                        
                        if attachment:
                            attachments.append(attachment)
                            logger.info(f"✅ {media_item['type']} загружено")
                        
                        # Удаляем временный файл
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                # Текст поста
                text = post.get('caption') or post.get('text') or ''
                
                # Публикуем в VK только если есть текст или вложения
                if text or attachments:
                    result = self.publish_to_vk(text, attachments)
                    
                    if 'response' in result:
                        # Сохраняем ID обработанного поста
                        self.save_processed_id(post['id'], post.get('hash'))
                        published_count += 1
                        
                        post_id = result['response']['post_id']
                        vk_url = f"https://vk.com/wall-{self.vk_group_id}_{post_id}"
                        logger.info(f"✅ Готово: {vk_url}")
                    else:
                        logger.error(f"❌ Ошибка публикации")
                else:
                    logger.warning(f"⚠️ Пост пустой, пропускаем")
                
                # Задержка между постами
                if i < len(new_posts):
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки поста: {e}")
                continue
        
        logger.info("=" * 60)
        logger.info(f"✅ Обработано: {published_count}/{len(new_posts)}")
        logger.info("=" * 60)

if __name__ == "__main__":
    try:
        publisher = TelegramToVKPublisher()
        publisher.process_new_posts()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
