# message_handler.py
from telethon import TelegramClient, events
import os
import re
import logging
import traceback
from typing import Optional, BinaryIO, Dict, List, Any, Tuple
from tempfile import NamedTemporaryFile
import asyncio
from datetime import datetime, timedelta, time
from telegram import error as telegram_error
from locales import get_text

class MyMessageHandler:
    def __init__(self, db, client: TelegramClient, bot):
        self.db = db
        self.client = client
        self.bot = bot
        # 用于跟踪临时文件
        self.temp_files = {}
        # 启动清理任务
        self.cleanup_task = None
        # 媒体缓存
        self.media_cache = {}
        # 已处理的媒体组
        self.processed_media_groups = set()
        # Cache successful target-channel validation to reduce repeated API calls.
        self.channel_validation_cache = {}
        self.channel_validation_ttl = timedelta(minutes=5)
        # Enable timing footer by default; can be disabled with SHOW_TIMING_FOOTER=0.
        self.show_timing_footer = os.getenv("SHOW_TIMING_FOOTER", "1").strip().lower() not in ("0", "false", "off", "no")
        # Enable reply timing footer by default; can be disabled with SHOW_REPLY_TIMING=0.
        self.show_reply_timing = os.getenv("SHOW_REPLY_TIMING", "1").strip().lower() not in ("0", "false", "off", "no")
        # High parallel forwarding (no per-channel serial queue).
        self.max_parallel_forwards = int(os.getenv("MAX_PARALLEL_FORWARDS", "20"))
        self.forward_semaphore = asyncio.Semaphore(self.max_parallel_forwards)
        # Retry settings for transient Telegram API failures.
        self.max_forward_retries = 3
        self.max_retry_backoff_seconds = 8

    async def start_cleanup_task(self):
        """启动定期清理任务"""
        if self.cleanup_task is None:
            self.cleanup_task = asyncio.create_task(self.cleanup_old_files())

    async def cleanup_old_files(self):
        """定期清理过期的临时文件"""
        while True:
            try:
                current_time = datetime.now()
                files_to_remove = []

                # 检查所有临时文件
                for file_path, timestamp in list(self.temp_files.items()):
                    if current_time - timestamp > timedelta(hours=1):  # 1小时后清理
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                logging.info(get_text('en', 'file_cleanup_success', file_path=file_path))
                            except Exception as e:
                                logging.error(get_text('en', 'file_cleanup_error', file_path=file_path, error=str(e)))
                        files_to_remove.append(file_path)

                # 从跟踪列表中移除已清理的文件
                for file_path in files_to_remove:
                    self.temp_files.pop(file_path, None)

                # 清理媒体缓存
                media_ids_to_remove = []
                for media_id, media_info in list(self.media_cache.items()):
                    if current_time - media_info.get('timestamp', current_time) > timedelta(minutes=10):  # 10分钟后清理
                        media_ids_to_remove.append(media_id)

                # 从缓存中移除过期的媒体
                for media_id in media_ids_to_remove:
                    media_info = self.media_cache.pop(media_id, None)
                    if not media_info:
                        continue
                    file_path = media_info.get('file_path')
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            logging.info(get_text('en', 'file_cleanup_success', file_path=file_path))
                        except Exception as e:
                            logging.error(get_text('en', 'file_cleanup_error', file_path=file_path, error=str(e)))
                    self.temp_files.pop(file_path, None)

                # 清理已处理的媒体组
                self.processed_media_groups.clear()

                # Remove expired target-channel validation cache.
                expired_channel_ids = [
                    channel_id
                    for channel_id, expire_at in self.channel_validation_cache.items()
                    if expire_at <= current_time
                ]
                for channel_id in expired_channel_ids:
                    self.channel_validation_cache.pop(channel_id, None)

            except Exception as e:
                logging.error(get_text('en', 'cleanup_task_error', error=str(e)))

            # 每小时运行一次
            await asyncio.sleep(3600)

    async def clear_media_cache(self, media_id, delay_seconds=600):
        """延迟清理媒体缓存"""
        await asyncio.sleep(delay_seconds)
        media_info = self.media_cache.pop(media_id, None)
        if not media_info:
            return

        file_path = media_info.get('file_path')
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(get_text('en', 'file_cleanup_success', file_path=file_path))
            except Exception as e:
                logging.error(get_text('en', 'file_cleanup_error', file_path=file_path, error=str(e)))
        self.temp_files.pop(file_path, None)
        logging.info(f"媒体缓存已清理: {media_id}")

    def _is_cached_media_path(self, file_path: str) -> bool:
        """Check whether a local path is still referenced by media cache."""
        if not file_path:
            return False
        for media_info in self.media_cache.values():
            if media_info.get('file_path') == file_path:
                return True
        return False

    async def _validate_forward_channel(self, channel_id: int) -> bool:
        """Validate target channel with a short TTL cache."""
        current_time = datetime.now()
        cached_expire_at = self.channel_validation_cache.get(channel_id)
        if cached_expire_at and cached_expire_at > current_time:
            return True

        try:
            chat = await self.bot.get_chat(channel_id)
            if not chat:
                logging.error(f"频道 {channel_id} 不存在或机器人无权访问，请检查权限或频道ID")
                return False
            self.channel_validation_cache[channel_id] = current_time + self.channel_validation_ttl
            return True
        except telegram_error.BadRequest as e:
            if "Chat not found" in str(e):
                logging.error(f"频道 {channel_id} 不存在或机器人无权访问，请检查权限或频道ID")
                return False
            logging.warning(f"验证频道失败: {str(e)}")
            return True
        except Exception as e:
            logging.warning(f"验证频道失败: {str(e)}")
            return True

    def get_media_id(self, message) -> str:
        """获取媒体文件的唯一标识"""
        try:
            # 尝试不同的属性来生成唯一ID
            if hasattr(message.media, 'photo'):
                photo = message.media.photo
                return f"photo_{photo.id}_{photo.access_hash}"
            elif hasattr(message.media, 'document'):
                doc = message.media.document
                return f"document_{doc.id}_{doc.access_hash}"
            elif hasattr(message.media, 'video'):
                video = message.media.video
                return f"video_{video.id}_{video.access_hash}"
            else:
                # 如果无法获取特定属性，使用消息的唯一标识
                return f"media_{message.chat_id}_{message.id}"
        except Exception as e:
            logging.error(f"获取媒体ID时出错: {str(e)}")
            # 如果出错，使用消息的唯一标识
            return f"media_{message.chat_id}_{message.id}"

    def get_media_type(self, message) -> str:
        """获取媒体类型"""
        if hasattr(message.media, 'photo'):
            return 'photo'
        elif hasattr(message.media, 'document'):
            # 检查是否是贴图
            if hasattr(message.media.document, 'attributes'):
                for attr in message.media.document.attributes:
                    if hasattr(attr, 'CONSTRUCTOR_ID') and attr.CONSTRUCTOR_ID == 0x6319d612:  # DocumentAttributeSticker
                        return 'sticker'
                    elif hasattr(attr, '__class__') and 'DocumentAttributeSticker' in str(attr.__class__):
                        return 'sticker'
                    # 检查是否是视频
                    elif hasattr(attr, 'CONSTRUCTOR_ID') and attr.CONSTRUCTOR_ID == 0x0ef02ce6:  # DocumentAttributeVideo
                        return 'video'
                    elif hasattr(attr, '__class__') and 'DocumentAttributeVideo' in str(attr.__class__):
                        return 'video'
                    # 检查是否是音频
                    elif hasattr(attr, 'CONSTRUCTOR_ID') and attr.CONSTRUCTOR_ID == 0x9852f9c6:  # DocumentAttributeAudio
                        return 'audio'
                    elif hasattr(attr, '__class__') and 'DocumentAttributeAudio' in str(attr.__class__):
                        return 'audio'
                    # 检查是否是动画
                    elif hasattr(attr, 'CONSTRUCTOR_ID') and attr.CONSTRUCTOR_ID == 0x0051792c:  # DocumentAttributeAnimated
                        return 'animation'
                    elif hasattr(attr, '__class__') and 'DocumentAttributeAnimated' in str(attr.__class__):
                        return 'animation'

            # 检查MIME类型
            if hasattr(message.media.document, 'mime_type'):
                mime_type = message.media.document.mime_type
                if mime_type:
                    if mime_type.startswith('video/'):
                        return 'video'
                    elif mime_type.startswith('audio/'):
                        return 'audio'
                    elif mime_type.startswith('image/'):
                        if mime_type == 'image/gif':
                            return 'animation'
                        return 'photo'

            return 'document'
        elif hasattr(message.media, 'video'):
            return 'video'
        else:
            return 'unknown'

    async def handle_channel_message(self, event):
        """处理频道消息"""
        try:
            message = event.message
            if not message:
                return

            chat = await event.get_chat()
            channel_info = self.db.get_channel_info(chat.id)

            if not channel_info or not channel_info.get('is_active'):
                return

            # 获取所有转发频道
            forward_channels = self.db.get_all_forward_channels(chat.id)
            if not forward_channels:
                return

            # 获取消息内容用于过滤
            content = ""
            if getattr(message, 'text', None):
                content = message.text
            elif getattr(message, 'caption', None):
                content = message.caption

            # 获取媒体类型
            media_type = 'text'  # 默认为文本
            if getattr(message, 'media', None):
                media_type = self.get_media_type(message)
                logging.info(f"检测到媒体类型: {media_type}")

            # 获取当前时间
            current_time = datetime.now()
            current_time_str = current_time.strftime('%H:%M')
            current_weekday = current_time.weekday() + 1  # 周一为1，周日为7

            pending_forwards = []
            for channel in forward_channels:
                try:
                    monitor_id = chat.id
                    forward_id = channel.get('channel_id')

                    # 检查时间段过滤
                    if not self.check_time_filter(monitor_id, forward_id, current_time_str, current_weekday):
                        logging.info(f"消息被时间段过滤器拦截: 监控频道={monitor_id}, 转发频道={forward_id}")
                        continue

                    # 检查内容过滤
                    if content:
                        logging.info(f"开始检查内容过滤器: 监控频道={monitor_id}, 转发频道={forward_id}")
                        content_preview = content[:100] + "..." if len(content) > 100 else content
                        logging.info(f"消息内容预览: '{content_preview}'")

                        if not self.check_content_filter(monitor_id, forward_id, content):
                            logging.info(f"消息被内容过滤器拦截: 监控频道={monitor_id}, 转发频道={forward_id}")
                            continue
                        else:
                            logging.info(f"消息通过内容过滤器: 监控频道={monitor_id}, 转发频道={forward_id}")

                    # 检查媒体类型过滤
                    if not self.check_media_filter(monitor_id, forward_id, media_type):
                        logging.info(f"消息被媒体类型过滤器拦截: 监控频道={monitor_id}, 转发频道={forward_id}, 媒体类型={media_type}")
                        continue

                    # 通过所有过滤器，按限并发策略转发消息
                    pending_forwards.append((
                        forward_id,
                        self._forward_with_limit(message, chat, channel)
                    ))
                except Exception as e:
                    logging.error(get_text('en', 'forward_channel_error',
                                         channel_id=channel.get('channel_id'),
                                         error=str(e)))
                    continue

            if pending_forwards:
                results = await asyncio.gather(
                    *(job for _, job in pending_forwards),
                    return_exceptions=True
                )
                for (forward_id, _), result in zip(pending_forwards, results):
                    if isinstance(result, Exception):
                        logging.error(get_text('en', 'forward_channel_error',
                                             channel_id=forward_id,
                                             error=str(result)))
        except Exception as e:
            logging.error(get_text('en', 'message_handler_error', error=str(e)))
            logging.error(get_text('en', 'error_details', details=traceback.format_exc()))

    async def _forward_with_limit(self, message, from_chat, channel):
        """Forward one message with bounded parallelism."""
        async with self.forward_semaphore:
            await self._forward_with_retry(message, from_chat, channel)

    async def _forward_with_retry(self, message, from_chat, channel):
        """Retry transient send failures to reduce message loss."""
        for attempt in range(1, self.max_forward_retries + 2):
            try:
                await self.handle_forward_message(message, from_chat, channel)
                return
            except Exception as e:
                if attempt > self.max_forward_retries or not self._is_retryable_forward_error(e):
                    raise

                wait_seconds = self._get_retry_wait_seconds(e, attempt)
                logging.warning(
                    f"转发重试: channel={channel.get('channel_id')}, attempt={attempt}, "
                    f"wait={wait_seconds}s, error={e}"
                )
                await asyncio.sleep(wait_seconds)

    def _is_retryable_forward_error(self, err: Exception) -> bool:
        """Only retry errors that are likely transient."""
        retryable_types = (
            telegram_error.RetryAfter,
            telegram_error.TimedOut,
            telegram_error.NetworkError,
        )
        if isinstance(err, retryable_types):
            return True
        # Defensive fallback when wrapped exceptions only expose message text.
        text = str(err).lower()
        retryable_keywords = (
            "timed out",
            "timeout",
            "temporary",
            "connection reset",
            "network",
            "too many requests",
            "retry after",
            "flood",
        )
        return any(keyword in text for keyword in retryable_keywords)

    def _get_retry_wait_seconds(self, err: Exception, attempt: int) -> int:
        """Compute retry backoff with RetryAfter priority."""
        if isinstance(err, telegram_error.RetryAfter):
            retry_after = getattr(err, "retry_after", 1)
            try:
                return max(1, int(retry_after))
            except Exception:
                return 1
        return min(2 ** attempt, self.max_retry_backoff_seconds)

    def _format_ts(self, dt_obj) -> str:
        """Format datetime safely in local timezone."""
        if not dt_obj:
            return "-"
        try:
            if getattr(dt_obj, "tzinfo", None) is not None:
                dt_obj = dt_obj.astimezone()
            return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(dt_obj)

    def _build_timing_footer(self, source_time: datetime, forward_time: datetime, arrival_time: datetime) -> str:
        """Build bottom timing footer text."""
        if not self.show_timing_footer:
            return ""
        return (
            "\n\n────────────\n"
            f"源消息时间: {self._format_ts(source_time)}\n"
            f"转发时间: {self._format_ts(forward_time)}\n"
            f"到达时间: {self._format_ts(arrival_time)}"
        )

    def _build_reply_timing_footer(self, event_time: datetime = None) -> str:
        """Build timing footer for bot reply messages (edited/deleted notifications)."""
        if not self.show_reply_timing:
            return ""
        current_time = datetime.now()
        return (
            "\n\n────────────\n"
            f"事件时间: {self._format_ts(event_time) if event_time else '-'}\n"
            f"通知时间: {self._format_ts(current_time)}"
        )

    def check_time_filter(self, monitor_id: int, forward_id: int, current_time: str, current_weekday: int) -> bool:
        """检查时间段过滤器"""
        try:
            # 标准化频道ID
            monitor_id = self._normalize_channel_id(monitor_id)
            forward_id = self._normalize_channel_id(forward_id)

            # 生成pair_id
            pair_id = f"{monitor_id}:{forward_id}"
            logging.info(f"生成频道配对ID(时间过滤): {pair_id}, 当前时间: {current_time}, 星期: {current_weekday}")

            # 获取时间段过滤器
            time_filters = self.db.get_time_filters(pair_id=pair_id)
            logging.info(f"获取到时间过滤规则: {len(time_filters)} 条")

            # 如果没有过滤器，允许所有时间
            if not time_filters:
                logging.info("没有时间过滤规则，允许所有时间")
                return True

            # 检查每个时间段过滤器
            for filter_rule in time_filters:
                # 检查当前星期是否在过滤器的星期范围内
                days_of_week = filter_rule.get('days_of_week', '').split(',')
                logging.info(f"检查星期范围: {days_of_week}, 当前星期: {current_weekday}")

                # 处理星期范围，支持如 1-5 的范围表示
                weekday_match = False
                for day_item in days_of_week:
                    if '-' in day_item:
                        # 如果是范围，如 1-5
                        try:
                            start_day, end_day = map(int, day_item.split('-'))
                            if start_day <= current_weekday <= end_day:
                                weekday_match = True
                                break
                        except ValueError:
                            logging.warning(f"无效的星期范围格式: {day_item}")
                    else:
                        # 如果是单个星期，如 1
                        try:
                            if int(day_item.strip()) == current_weekday:
                                weekday_match = True
                                break
                        except ValueError:
                            logging.warning(f"无效的星期格式: {day_item}")

                if days_of_week and not weekday_match:
                    logging.info(f"当前星期 {current_weekday} 不在过滤器的星期范围内 {days_of_week}")
                    continue

                # 检查当前时间是否在过滤器的时间范围内
                start_time = filter_rule.get('start_time')
                end_time = filter_rule.get('end_time')
                logging.info(f"检查时间范围: {start_time}-{end_time}, 当前时间: {current_time}")

                if start_time and end_time:
                    # 如果当前时间在范围内
                    in_time_range = start_time <= current_time <= end_time

                    # 根据模式决定是否允许
                    mode = filter_rule.get('mode', 'ALLOW')
                    if mode == 'ALLOW' and in_time_range:
                        logging.info(f"当前时间在允许范围内，允许转发")
                        return True
                    elif mode == 'BLOCK' and in_time_range:
                        logging.info(f"当前时间在拦截范围内，拦截转发")
                        return False

            # 如果没有匹配的规则，默认允许
            logging.info("没有匹配的时间过滤规则，默认允许")
            return True

        except Exception as e:
            logging.error(f"检查时间段过滤器时出错: {e}")
            logging.error(f"错误详情: {traceback.format_exc()}")
            # 出错时默认允许
            return True

    def check_content_filter(self, monitor_id: int, forward_id: int, content: str) -> bool:
        """检查内容过滤器"""
        try:
            # 标准化频道ID
            monitor_id = self._normalize_channel_id(monitor_id)
            forward_id = self._normalize_channel_id(forward_id)

            # 生成pair_id
            pair_id = f"{monitor_id}:{forward_id}"
            logging.info(f"生成频道配对ID: {pair_id}")

            # 获取过滤规则
            filter_rules = self.db.get_filter_rules(pair_id=pair_id)
            logging.info(f"获取到过滤规则: {len(filter_rules)} 条")

            # 如果没有规则，允许所有内容
            if not filter_rules:
                logging.info("没有过滤规则，允许所有内容")
                return True

            # 分类规则
            whitelist_rules = []
            blacklist_rules = []

            for rule in filter_rules:
                if rule.get('rule_type') == 'WHITELIST':
                    whitelist_rules.append(rule)
                elif rule.get('rule_type') == 'BLACKLIST':
                    blacklist_rules.append(rule)

            logging.info(f"白名单规则: {len(whitelist_rules)} 条, 黑名单规则: {len(blacklist_rules)} 条")

            # 如果有白名单规则，必须匹配至少一条才允许
            if whitelist_rules:
                whitelist_match = False
                for rule in whitelist_rules:
                    logging.info(f"检查白名单规则: {rule.get('pattern')}")
                    if self.match_rule(rule, content):
                        whitelist_match = True
                        logging.info(f"白名单规则匹配成功: {rule.get('pattern')}")
                        break

                if not whitelist_match:
                    logging.info("没有匹配任何白名单规则，拦截消息")
                    return False

            # 如果有黑名单规则，匹配任一条则拒绝
            for rule in blacklist_rules:
                logging.info(f"检查黑名单规则: {rule.get('pattern')}")
                if self.match_rule(rule, content):
                    logging.info(f"黑名单规则匹配成功: {rule.get('pattern')}，拦截消息")
                    return False

            # 通过所有过滤
            logging.info("消息通过所有内容过滤器")
            return True

        except Exception as e:
            logging.error(f"检查内容过滤器时出错: {e}")
            logging.error(f"错误详情: {traceback.format_exc()}")
            # 出错时默认允许
            return True

    def _normalize_channel_id(self, channel_id: int) -> int:
        """标准化频道ID格式，确保存储时不带-100前缀"""
        str_id = str(channel_id)
        if str_id.startswith('-100'):
            return int(str_id[4:])
        elif str_id.startswith('-'):
            return int(str_id[1:])
        return int(str_id)

    def check_media_filter(self, monitor_id: int, forward_id: int, media_type: str) -> bool:
        """检查媒体类型过滤器"""
        try:
            # 标准化频道ID
            monitor_id = self._normalize_channel_id(monitor_id)
            forward_id = self._normalize_channel_id(forward_id)

            # 生成pair_id
            pair_id = f"{monitor_id}:{forward_id}"
            logging.info(f"生成频道配对ID(媒体过滤): {pair_id}, 媒体类型: {media_type}")

            # 获取媒体过滤规则
            media_filters = self.db.get_media_filters(pair_id=pair_id)
            logging.info(f"获取到媒体过滤规则: {len(media_filters)} 条")

            # 如果没有规则，允许所有媒体类型
            if not media_filters:
                logging.info(f"没有媒体过滤规则，允许所有媒体类型: {media_type}")
                return True

            # 检查是否有匹配当前媒体类型的规则
            for filter_rule in media_filters:
                if filter_rule.get('media_type') == media_type:
                    action = filter_rule.get('action', 'ALLOW')
                    logging.info(f"找到媒体类型过滤规则: {media_type}, 动作: {action}")
                    # 如果动作是拦截，返回False
                    if action == 'BLOCK':
                        logging.info(f"媒体类型 {media_type} 被拦截")
                        return False
                    # 如果动作是允许，返回True
                    elif action == 'ALLOW':
                        logging.info(f"媒体类型 {media_type} 被允许")
                        return True

            # 如果没有匹配的规则，默认允许
            logging.info(f"没有找到匹配的媒体类型规则，默认允许: {media_type}")
            return True

        except Exception as e:
            logging.error(f"检查媒体类型过滤器时出错: {e}")
            logging.error(f"错误详情: {traceback.format_exc()}")
            # 出错时默认允许
            return True

    def match_rule(self, rule: dict, content: str) -> bool:
        """检查内容是否匹配规则"""
        try:
            pattern = rule.get('pattern', '')
            if not pattern:
                logging.info("规则模式为空，跳过")
                return False

            filter_mode = rule.get('filter_mode', 'KEYWORD')
            rule_type = rule.get('rule_type', '')
            logging.info(f"开始匹配规则: 类型={rule_type}, 模式={filter_mode}, 内容='{pattern}'")

            if filter_mode == 'KEYWORD':
                # 关键词模式，将整个关键词作为一个整体进行匹配
                pattern_lower = pattern.lower()
                content_lower = content.lower()

                # 记录内容的一部分用于调试
                content_preview = content_lower[:100] + "..." if len(content_lower) > 100 else content_lower
                logging.info(f"内容预览: '{content_preview}'")

                # 直接检查内容是否包含关键词
                match_result = pattern_lower in content_lower
                if match_result:
                    logging.info(f"关键词匹配成功: '{pattern}' 在消息中被找到")
                else:
                    logging.info(f"关键词匹配失败: '{pattern}' 不在消息中")
                return match_result
            elif filter_mode == 'REGEX':
                # 正则表达式模式
                try:
                    match_result = bool(re.search(pattern, content, re.IGNORECASE))
                    if match_result:
                        logging.info(f"正则表达式匹配成功: '{pattern}'")
                    else:
                        logging.info(f"正则表达式匹配失败: '{pattern}'")
                    return match_result
                except re.error as regex_error:
                    logging.error(f"正则表达式错误: {regex_error}, 模式: '{pattern}'")
                    return False

            logging.info(f"未知的过滤模式: {filter_mode}")
            return False

        except Exception as e:
            logging.error(f"匹配规则时出错: {e}")
            logging.error(f"错误详情: {traceback.format_exc()}")
            return False

    async def handle_media_send(self, message, channel_id, media_type: str = None, reply_to_message_id: int = None, from_chat = None):
        """处理媒体发送并确保清理"""
        # 下载媒体文件
        media_info = await self.download_media_file(message, media_type)
        if not media_info:
            logging.error("媒体文件下载失败")
            return

        file_path = media_info.get('file_path')
        media_type = media_info.get('media_type')

        try:
            # 只有在没有回复消息时才添加说明文字
            caption = None
            if not reply_to_message_id and from_chat:
                # 构建用户名部分
                username = f"(@{from_chat.username})" if getattr(from_chat, 'username', None) else ""

                # 使用简化的模板作为媒体文件的标题
                caption = f"📨 转发自 {getattr(from_chat, 'title', 'Unknown Channel')} {username}"
            elif message.text or message.caption:
                caption = message.text or message.caption

            # 发送文件
            with open(file_path, 'rb') as media_file:
                try:
                    file_data = media_file.read()
                    send_kwargs = {
                        'chat_id': channel_id,
                        'caption': caption,
                        'read_timeout': 1800,
                        'write_timeout': 1800
                    }

                    if reply_to_message_id:
                        send_kwargs['reply_to_message_id'] = reply_to_message_id

                    if media_type == 'photo':
                        send_kwargs['photo'] = file_data
                        await self.bot.send_photo(**send_kwargs)
                    elif media_type == 'video':
                        send_kwargs.update({
                            'video': file_data,
                            'supports_streaming': True
                        })

                        # 添加视频参数
                        if 'width' in media_info:
                            send_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            send_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']

                        # 如果有缩略图
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            with open(media_info['thumb_path'], 'rb') as thumb_file:
                                send_kwargs['thumb'] = thumb_file.read()

                        await self.bot.send_video(**send_kwargs)

                        # 清理缩略图
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            os.remove(media_info['thumb_path'])
                    elif media_type == 'audio':
                        send_kwargs.update({
                            'audio': file_data
                        })

                        # 添加音频参数
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']
                        if 'performer' in media_info:
                            send_kwargs['performer'] = media_info['performer']
                        if 'title' in media_info:
                            send_kwargs['title'] = media_info['title']

                        await self.bot.send_audio(**send_kwargs)
                    elif media_type == 'animation':
                        send_kwargs.update({
                            'animation': file_data
                        })

                        # 添加动画参数
                        if 'width' in media_info:
                            send_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            send_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']

                        await self.bot.send_animation(**send_kwargs)
                    elif media_type == 'document':
                        send_kwargs['document'] = file_data
                        if 'filename' in media_info:
                            send_kwargs['filename'] = media_info['filename']
                        await self.bot.send_document(**send_kwargs)
                    elif media_type == 'sticker':
                        # 发送贴图
                        await self.bot.send_sticker(
                            chat_id=channel_id,
                            sticker=file_data,
                            reply_to_message_id=reply_to_message_id
                        )

                    logging.info(f"文件发送成功: {media_type}" +
                           (f" (回复到消息: {reply_to_message_id})" if reply_to_message_id else ""))
                finally:
                    del file_data  # 释放内存

            # 发送成功后清理文件
            await self.cleanup_file(file_path)
            return True

        except Exception as e:
            logging.error(f"处理媒体文件时出错: {str(e)}")
            if file_path:
                await self.cleanup_file(file_path)
            return False

    async def handle_forward_message(self, message, from_chat, to_channel):
        """处理消息转发"""
        if not message or not from_chat or not to_channel:
            logging.error(get_text('en', 'missing_parameters'))
            return

        try:
            channel_id = to_channel.get('channel_id')
            if not channel_id:
                logging.error(get_text('en', 'invalid_channel_id'))
                return

            # 手动添加 -100 前缀
            original_channel_id = channel_id
            channel_id = int("-100" + str(channel_id))
            logging.info(f"处理频道ID: 原始值={original_channel_id}, 处理后={channel_id}")

            # 检查是否是回复消息
            reply_to_message_id = None
            original_reply_message = None
            reply_info = None

            if hasattr(message, 'reply_to_msg_id') and message.reply_to_msg_id:
                try:
                    # 获取原始回复消息
                    original_reply_message = await self.client.get_messages(from_chat.id, ids=message.reply_to_msg_id)
                    if original_reply_message:
                        # 保存回复消息信息以便后续使用
                        reply_content = original_reply_message.text or original_reply_message.caption or ""
                        reply_info = {
                            'id': original_reply_message.id,
                            'content': reply_content,
                            'short_content': reply_content[:50] + "..." if len(reply_content) > 50 else reply_content
                        }

                    # 在数据库中查找这条消息是否已经转发过
                    forwarded_reply = self.db.get_forwarded_message(from_chat.id, message.reply_to_msg_id, channel_id)
                    if forwarded_reply:
                        # 如果找到了转发的回复消息，使用其ID作为回复ID
                        reply_to_message_id = forwarded_reply['forwarded_message_id']
                        logging.info(f"找到原始回复消息的转发记录，将使用原生回复: {reply_to_message_id}")
                except Exception as e:
                    logging.warning(f"获取原始回复消息失败: {e}")

            forwarded_msg = None

            # 不使用直接转发，始终使用处理过的转发
            logging.info("按要求不使用直接转发，将使用处理过的转发方式")

            # Validate target channel before sending.
            if not await self._validate_forward_channel(channel_id):
                return

            # 如果直接转发失败，处理文本消息
            if getattr(message, 'text', None) or getattr(message, 'caption', None):
                content = message.text or message.caption
                # 获取频道类型
                chat_type_key = 'chat_type_channel'  # 默认类型
                if hasattr(from_chat, 'type'):
                    if from_chat.type == 'channel':
                        if getattr(from_chat, 'username', None):
                            chat_type_key = 'chat_type_public_channel'
                        else:
                            chat_type_key = 'chat_type_private_channel'
                    elif from_chat.type == 'group':
                        chat_type_key = 'chat_type_group'
                    elif from_chat.type == 'supergroup':
                        chat_type_key = 'chat_type_supergroup'
                    elif from_chat.type == 'gigagroup':
                        chat_type_key = 'chat_type_gigagroup'

                # 获取用户语言
                lang = self.db.get_user_language(to_channel.get('channel_id', 0)) or 'en'

                # 获取频道类型显示文本
                chat_type = get_text(lang, chat_type_key)

                # 获取当前时间
                forward_time_dt = datetime.now()
                current_time = forward_time_dt.strftime('%Y-%m-%d %H:%M:%S')

                # 构建用户名部分
                username = f"(@{from_chat.username})" if getattr(from_chat, 'username', None) else ""

                # 检查是否是回复消息
                reply_text = ""
                # 如果有回复消息但无法使用原生回复，则在消息中添加回复信息
                if reply_info and not reply_to_message_id:
                    reply_text = get_text(lang, 'reply_to_message', text=reply_info['short_content']) + "\n"
                    logging.info(f"添加回复信息到消息中: {reply_info['short_content']}")

                # 确保频道标题和类型正确显示
                channel_title = getattr(from_chat, 'title', None)
                if not channel_title:
                    channel_title = getattr(from_chat, 'first_name', 'Unknown Channel')

                # 使用新的消息模板
                forwarded_text = get_text(lang, 'forwarded_message_template',
                                         title=channel_title,
                                         username=username,
                                         chat_type=chat_type,
                                         time=current_time,
                                         content=reply_text + content)
                # Append timing footer (enabled by default).
                forwarded_text += self._build_timing_footer(
                    source_time=getattr(message, 'date', None),
                    forward_time=forward_time_dt,
                    arrival_time=datetime.now()
                )

                # 记录转发信息以便调试
                logging.info(f"转发消息信息: 标题={channel_title}, 类型={chat_type}, 用户名={username}")

                # 检查是否有自定义表情
                has_custom_emoji = await self.handle_custom_emoji(message, channel_id)

                # 发送文本消息，如果有自定义表情则禁用Markdown格式
                try:
                    # 如果有原生回复消息的ID，使用原生回复
                    send_kwargs = {
                        'chat_id': channel_id,
                        'text': forwarded_text,
                        'parse_mode': None if has_custom_emoji else 'Markdown',
                        'disable_web_page_preview': True
                    }

                    # 检查是否有回复消息ID，并安全地添加
                    if reply_to_message_id:
                        # 直接添加回复ID，如果消息不存在，后面会捕获并处理错误
                        send_kwargs['reply_to_message_id'] = reply_to_message_id
                        logging.info(f"尝试使用原生回复: {reply_to_message_id}")

                        # 确保回复文本已添加到消息中，以防回复失败
                        if reply_info and reply_text not in forwarded_text:
                            send_kwargs['text'] = reply_text + forwarded_text

                    # 尝试发送消息
                    try:
                        forwarded_msg = await self.bot.send_message(**send_kwargs)
                        # 保存转发关系
                        self.db.save_forwarded_message(from_chat.id, message.id, channel_id, forwarded_msg.message_id)
                    except telegram_error.BadRequest as br_error:
                        # 处理特定的错误
                        if "Message to be replied not found" in str(br_error):
                            # 回复的消息不存在，移除回复ID后重试
                            logging.warning("回复的消息不存在，移除回复ID后重试")
                            if 'reply_to_message_id' in send_kwargs:
                                del send_kwargs['reply_to_message_id']
                            forwarded_msg = await self.bot.send_message(**send_kwargs)
                            self.db.save_forwarded_message(from_chat.id, message.id, channel_id, forwarded_msg.message_id)
                        elif "can't parse entities" in str(br_error).lower():
                            # 实体解析错误，尝试使用纯文本
                            logging.warning(f"实体解析错误，尝试使用纯文本: {br_error}")
                            send_kwargs['parse_mode'] = None
                            forwarded_msg = await self.bot.send_message(**send_kwargs)
                            self.db.save_forwarded_message(from_chat.id, message.id, channel_id, forwarded_msg.message_id)
                        else:
                            # 其他BadRequest错误，重新抛出
                            raise

                except Exception as e:
                    # 如果发送失败，尝试使用纯文本
                    logging.warning(f"发送消息失败，尝试使用纯文本: {e}")
                    send_kwargs = {
                        'chat_id': channel_id,
                        'text': forwarded_text,
                        'parse_mode': None,
                        'disable_web_page_preview': True
                    }

                    # 不添加回复ID，避免可能的错误
                    forwarded_msg = await self.bot.send_message(**send_kwargs)

                    # 保存转发关系
                    self.db.save_forwarded_message(from_chat.id, message.id, channel_id, forwarded_msg.message_id)

                logging.info(get_text('en', 'text_send_success', channel_id=channel_id))

            # 异步处理媒体消息
            if getattr(message, 'media', None) and forwarded_msg:
                logging.info("检测到媒体消息，开始异步处理")

                # 检查是否是媒体组
                if hasattr(message, 'grouped_id') and message.grouped_id:
                    # 异步处理媒体组
                    logging.info("检测到媒体组，开始异步处理媒体组")
                    asyncio.create_task(self.handle_media_group(
                        message=message,
                        channel_id=channel_id,
                        forwarded_msg=forwarded_msg,  # 传递已转发的消息对象
                        from_chat=from_chat
                    ))
                    return

                # 确定媒体类型
                media_type = self.get_media_type(message)
                logging.info(f"媒体类型: {media_type}")

                # 如果是贴图，使用特殊处理
                if media_type == 'sticker':
                    asyncio.create_task(self.handle_sticker_send(
                        message=message,
                        channel_id=channel_id,
                        from_chat=from_chat,
                        reply_to_message_id=forwarded_msg.message_id
                    ))
                    return

                # 异步处理媒体文件，使用编辑模式
                asyncio.create_task(self.handle_media_edit(
                    message=message,
                    channel_id=channel_id,
                    media_type=media_type,
                    forwarded_msg=forwarded_msg,  # 传递已转发的消息对象
                    from_chat=from_chat
                ))
        except Exception as e:
            logging.error(get_text('en', 'forward_message_error', error=str(e)))
            logging.error(get_text('en', 'error_details', details=traceback.format_exc()))
            raise

    async def cleanup_file(self, file_path: str, force: bool = False):
        """清理单个文件"""
        try:
            if not file_path:
                return

            # Keep cached media files for other in-flight forwarding tasks.
            if not force and self._is_cached_media_path(file_path):
                return

            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(get_text('en', 'file_cleanup_success', file_path=file_path))
            self.temp_files.pop(file_path, None)
        except Exception as e:
            logging.error(get_text('en', 'file_cleanup_error',
                                 file_path=file_path,
                                 error=str(e)))

    async def download_progress_callback(self, current, total):
        """下载进度回调"""
        if total:
            percentage = current * 100 / total
            if percentage % 20 == 0:  # 每20%记录一次
                logging.info(get_text('en', 'download_progress', percentage=percentage))

    async def handle_edited_message(self, event):
        """处理消息编辑事件"""
        try:
            # 获取编辑后的消息
            message = event.message
            if not message:
                return

            # 获取频道信息
            chat = await event.get_chat()
            channel_info = self.db.get_channel_info(chat.id)

            if not channel_info or not channel_info.get('is_active'):
                return

            # 获取所有转发频道
            forward_channels = self.db.get_all_forward_channels(chat.id)
            if not forward_channels:
                return

            # 获取消息内容
            content = ""
            if hasattr(message, 'text') and message.text:
                content = message.text
            elif hasattr(message, 'caption') and message.caption:
                content = message.caption

            logging.info(f"编辑消息内容: {content}")
            if not content:
                return

            # 获取用户语言
            lang = self.db.get_user_language(chat.id) or 'en'

            # 获取编辑事件时间
            edit_time = getattr(message, 'edit_date', None) or datetime.now()

            # 向所有转发频道发送编辑通知
            for channel in forward_channels:
                try:
                    # 手动添加 -100 前缀
                    original_channel_id = channel.get('channel_id')
                    channel_id = int("-100" + str(original_channel_id))
                    logging.info(f"处理频道ID(编辑消息): 原始值={original_channel_id}, 处理后={channel_id}")

                    # 尝试找到原始消息的转发消息
                    forwarded_msg = None
                    try:
                        # 在数据库中查找这条消息是否已经转发过
                        if hasattr(message, 'id'):
                            forwarded_msg = self.db.get_forwarded_message(chat.id, message.id, channel_id)
                            if forwarded_msg:
                                logging.info(f"找到原始消息的转发记录: {forwarded_msg['forwarded_message_id']}")
                    except Exception as e:
                        logging.warning(f"获取原始消息的转发记录失败: {e}")

                    # 准备编辑通知消息
                    edit_notice = get_text(lang, 'edited_message')
                    edit_text = f"{edit_notice}\n\n{content}"

                    # 添加时间戳footer
                    edit_text += self._build_reply_timing_footer(edit_time)

                    # 发送编辑通知，使用Markdown格式
                    send_kwargs = {
                        'chat_id': channel_id,
                        'text': edit_text,
                        'parse_mode': 'Markdown',  # 使用Markdown格式化
                        'disable_web_page_preview': True
                    }

                    # 如果找到了原消息的转发记录，使用回复形式
                    if forwarded_msg:
                        send_kwargs['reply_to_message_id'] = forwarded_msg['forwarded_message_id']
                        logging.info("使用回复形式发送编辑通知")

                    try:
                        await self.bot.send_message(**send_kwargs)
                        logging.info(f"成功发送编辑通知到频道 {channel_id}")
                    except Exception as e:
                        # 如果Markdown解析失败，尝试使用纯文本
                        logging.warning(f"使用Markdown发送编辑通知失败: {e}")
                        send_kwargs['parse_mode'] = None
                        await self.bot.send_message(**send_kwargs)

                except Exception as e:
                    logging.error(f"发送编辑通知到频道 {channel.get('channel_id')} 失败: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"处理消息编辑事件时出错: {str(e)}")
            logging.error(f"错误详情: {traceback.format_exc()}")

    async def handle_deleted_message(self, event):
        """处理消息删除事件"""
        try:
            # 注意：删除的消息事件只包含消息ID，不包含内容
            # 我们需要从事件中获取频道ID和消息ID

            # 获取频道ID
            chat_id = event.chat_id

            # 获取删除的消息ID
            deleted_ids = getattr(event, 'deleted_ids', [])
            if not deleted_ids and hasattr(event, 'deleted_id'):
                deleted_ids = [event.deleted_id]

            # logging.info(f"检测到消息删除事件: 频道ID={chat_id}, 消息ID={deleted_ids}")

            if not chat_id:
                logging.warning("MessageDeleted 事件没有频道ID，无法处理")
                return

            if not deleted_ids:
                logging.warning("MessageDeleted 事件没有删除的消息ID，无法处理")
                return

            # 获取频道信息
            channel_info = self.db.get_channel_info(chat_id)
            if not channel_info or not channel_info.get('is_active'):
                return

            # 获取所有转发频道
            forward_channels = self.db.get_all_forward_channels(chat_id)
            if not forward_channels:
                return

            # 获取用户语言
            lang = self.db.get_user_language(chat_id) or 'en'

            # 构建删除通知消息
            delete_notice = get_text(lang, 'deleted_message')
            logging.info(f"准备发送删除通知: {delete_notice}")

            # 向所有转发频道发送删除通知
            for channel in forward_channels:
                try:
                    # 手动添加 -100 前缀
                    original_channel_id = channel.get('channel_id')
                    channel_id = int("-100" + str(original_channel_id))
                    logging.info(f"处理频道ID(删除消息): 原始值={original_channel_id}, 处理后={channel_id}")

                    # 尝试找到原始消息的转发消息
                    forwarded_msg = None
                    original_message_content = None

                    try:
                        # 在数据库中查找这条消息是否已经转发过
                        if hasattr(event, 'deleted_ids') and event.deleted_ids:
                            for msg_id in event.deleted_ids:
                                forwarded_msg = self.db.get_forwarded_message(chat_id, msg_id, channel_id)
                                if forwarded_msg:
                                    logging.info(f"找到原始消息的转发记录: {forwarded_msg['forwarded_message_id']}")

                                    # 尝试获取原始消息内容（如果有缓存）
                                    # 注意：这里我们无法获取原始内容，因为消息已被删除
                                    # 如果需要实现这个功能，需要在转发时将消息内容保存到数据库
                                    break
                    except Exception as e:
                        logging.warning(f"获取原始消息的转发记录失败: {e}")

                    # 构建删除通知文本，添加时间戳footer
                    delete_text = delete_notice + (f"\n\n原消息内容: {original_message_content}" if original_message_content else "")
                    delete_text += self._build_reply_timing_footer()

                    # 发送删除通知
                    send_kwargs = {
                        'chat_id': channel_id,
                        'text': delete_text,
                        'parse_mode': 'Markdown'
                    }

                    # 如果找到了原消息的转发记录，使用回复形式
                    if forwarded_msg:
                        try:
                            send_kwargs['reply_to_message_id'] = forwarded_msg['forwarded_message_id']
                            logging.info("使用回复形式发送删除通知")
                            await self.bot.send_message(**send_kwargs)
                            logging.info(f"成功发送删除通知到频道 {channel_id}")
                        except Exception as reply_error:
                            # 如果回复失败（可能原消息已被删除），发送普通消息
                            logging.warning(f"回复原消息失败，发送普通消息: {reply_error}")
                            del send_kwargs['reply_to_message_id']
                            try:
                                await self.bot.send_message(**send_kwargs)
                                logging.info(f"成功发送删除通知到频道 {channel_id} (普通消息)")
                            except Exception as e:
                                # 如果Markdown解析失败，尝试使用纯文本
                                logging.warning(f"使用Markdown发送删除通知失败: {e}")
                                send_kwargs['parse_mode'] = None
                                await self.bot.send_message(**send_kwargs)
                    else:
                        # 如果没有找到原消息记录，直接发送普通消息
                        try:
                            await self.bot.send_message(**send_kwargs)
                            logging.info(f"成功发送删除通知到频道 {channel_id}")
                        except Exception as e:
                            # 如果Markdown解析失败，尝试使用纯文本
                            logging.warning(f"使用Markdown发送删除通知失败: {e}")
                            send_kwargs['parse_mode'] = None
                            await self.bot.send_message(**send_kwargs)

                except Exception as e:
                    logging.error(f"发送删除通知到频道 {channel.get('channel_id')} 失败: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"处理消息删除事件时出错: {str(e)}")
            logging.error(f"错误详情: {traceback.format_exc()}")

    async def download_media_file(self, message, media_type: str = None) -> dict:
        """下载媒体文件并返回相关信息"""
        # 如果没有指定媒体类型，自动检测
        if media_type is None:
            media_type = self.get_media_type(message)

        # 生成媒体ID
        media_id = self.get_media_id(message)

        # 检查缓存
        cached_media = self.media_cache.get(media_id)
        if cached_media:
            cached_path = cached_media.get('file_path')
            if cached_path and os.path.exists(cached_path):
                cached_media['timestamp'] = datetime.now()
                self.temp_files[cached_path] = datetime.now()
                logging.info(f"使用缓存的媒体文件: {media_id}")
                return cached_media

            # Cache entry exists but local file is gone, force re-download.
            logging.warning(f"缓存文件不存在，重新下载: {media_id}, path={cached_path}")
            self.media_cache.pop(media_id, None)

        tmp = None
        file_path = None
        chunk_size = 20 * 1024 * 1024  # 20MB 分块

        try:
            # 获取文件大小
            file_size = getattr(message.media, 'file_size', 0) or getattr(message.media, 'size', 0)
            logging.info(f"开始下载媒体文件，大小: {file_size / (1024*1024):.2f}MB")

            # 确保媒体缓存目录存在
            media_cache_dir = "data/media_cache"
            os.makedirs(media_cache_dir, exist_ok=True)

            # 创建临时文件
            tmp = NamedTemporaryFile(delete=False, prefix='tg_', suffix=f'.{media_type}', dir=media_cache_dir)
            file_path = tmp.name

            # 使用分块下载
            downloaded_size = 0
            async for chunk in self.client.iter_download(message.media, chunk_size=chunk_size):
                if chunk:
                    tmp.write(chunk)
                    downloaded_size += len(chunk)
                    if downloaded_size % (50 * 1024 * 1024) == 0:
                        progress = (downloaded_size / file_size) * 100 if file_size else 0
                        logging.info(f"下载进度: {progress:.1f}% ({downloaded_size/(1024*1024):.1f}MB/{file_size/(1024*1024):.1f}MB)")

                    if downloaded_size % (100 * 1024 * 1024) == 0:
                        tmp.flush()
                        os.fsync(tmp.fileno())

            tmp.close()
            logging.info("媒体文件下载完成")

            if not os.path.exists(file_path):
                raise FileNotFoundError(get_text('en', 'downloaded_file_not_found', file_path=file_path))

            # 记录临时文件
            self.temp_files[file_path] = datetime.now()

            # 收集媒体信息
            media_info = {
                'file_path': file_path,
                'file_size': file_size,
                'media_type': media_type,
                'timestamp': datetime.now()
            }

            # 收集特定媒体类型的额外信息
            if media_type == 'video' and hasattr(message.media, 'document'):
                # 从属性中获取视频信息
                if hasattr(message.media.document, 'attributes'):
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'w'):
                            media_info['width'] = attr.w
                        if hasattr(attr, 'h'):
                            media_info['height'] = attr.h
                        if hasattr(attr, 'duration'):
                            media_info['duration'] = attr.duration
                        if hasattr(attr, 'file_name'):
                            media_info['filename'] = attr.file_name

                # 如果有缩略图
                if hasattr(message.media.document, 'thumb') and message.media.document.thumb:
                    try:
                        thumb_path = await self.client.download_media(message.media.document.thumb)
                        media_info['thumb_path'] = thumb_path
                    except Exception as e:
                        logging.warning(f"无法下载视频缩略图: {str(e)}")

            # 如果是音频
            elif media_type == 'audio' and hasattr(message.media, 'document'):
                if hasattr(message.media.document, 'attributes'):
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'duration'):
                            media_info['duration'] = attr.duration
                        if hasattr(attr, 'performer'):
                            media_info['performer'] = attr.performer
                        if hasattr(attr, 'title'):
                            media_info['title'] = attr.title
                        if hasattr(attr, 'file_name'):
                            media_info['filename'] = attr.file_name

            # 如果是动画
            elif media_type == 'animation' and hasattr(message.media, 'document'):
                if hasattr(message.media.document, 'attributes'):
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'w'):
                            media_info['width'] = attr.w
                        if hasattr(attr, 'h'):
                            media_info['height'] = attr.h
                        if hasattr(attr, 'duration'):
                            media_info['duration'] = attr.duration
                        if hasattr(attr, 'file_name'):
                            media_info['filename'] = attr.file_name

                # 如果有缩略图
                if hasattr(message.media.document, 'thumb') and message.media.document.thumb:
                    try:
                        thumb_path = await self.client.download_media(message.media.document.thumb)
                        media_info['thumb_path'] = thumb_path
                    except Exception as e:
                        logging.warning(f"无法下载动画缩略图: {str(e)}")

            # 如果是文档，获取文件名
            elif media_type == 'document' and hasattr(message.media, 'document'):
                if hasattr(message.media.document, 'attributes'):
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'file_name'):
                            media_info['filename'] = attr.file_name
                            break

                # 获取MIME类型
                if hasattr(message.media.document, 'mime_type'):
                    media_info['mime_type'] = message.media.document.mime_type

            # 将结果存入缓存
            self.media_cache[media_id] = media_info

            # 设置缓存过期时间（例如10分钟后自动清理）
            asyncio.create_task(self.clear_media_cache(media_id, 600))

            return media_info

        except Exception as e:
            logging.error(f"下载媒体文件时出错: {str(e)}")
            if file_path and file_path in self.temp_files:
                await self.cleanup_file(file_path)
            return {}

    async def handle_media_group(self, message, channel_id, forwarded_msg=None, from_chat=None):
        """处理媒体组（多张图片或视频）"""
        try:
            # 获取媒体组ID
            group_id = getattr(message, 'grouped_id', None)
            if not group_id:
                # 如果不是媒体组，使用普通媒体处理
                media_type = self.get_media_type(message)
                if forwarded_msg:
                    # 使用编辑模式
                    await self.handle_media_edit(message, channel_id, media_type, forwarded_msg, from_chat)
                else:
                    # 使用回复模式
                    reply_to_message_id = forwarded_msg.message_id if forwarded_msg else None
                    await self.handle_media_send(message, channel_id, media_type, reply_to_message_id=reply_to_message_id)
                return

            # 检查是否已经处理过这个媒体组
            processed_group_key = (group_id, channel_id)
            if processed_group_key in self.processed_media_groups:
                logging.info(f"媒体组 {group_id} 已经处理过，跳过")
                return

            # 标记为已处理
            self.processed_media_groups.add(processed_group_key)
            logging.info(f"开始处理媒体组: {group_id}")

            # 获取同一组的所有媒体消息
            # 先获取当前消息之前的消息
            media_messages_before = await self.client.get_messages(
                message.chat_id,
                limit=20,  # 增加限制以确保获取到全部媒体
                offset_id=message.id,
                reverse=True
            )

            # 再获取当前消息及之后的消息
            media_messages_after = await self.client.get_messages(
                message.chat_id,
                limit=20,  # 增加限制以确保获取到全部媒体
                min_id=message.id - 1  # 从当前消息开始
            )

            # 合并所有消息
            media_messages = media_messages_before + media_messages_after
            logging.info(f"获取到媒体消息总数: {len(media_messages)}个")

            # 过滤出同一组的媒体
            group_media = [msg for msg in media_messages if hasattr(msg, 'grouped_id') and msg.grouped_id == group_id]
            logging.info(f"找到媒体组消息: {len(group_media)} 个")

            # 准备媒体列表
            media_list = []
            for msg in group_media:
                media_type = self.get_media_type(msg)
                media_info = await self.download_media_file(msg, media_type)
                if media_info:
                    # 安全获取消息标题，确保属性存在
                    caption = None
                    if hasattr(msg, 'text') and msg.text:
                        caption = msg.text
                    elif hasattr(msg, 'caption') and msg.caption:
                        caption = msg.caption

                    media_list.append({
                        'type': media_type,
                        'path': media_info['file_path'],
                        'caption': caption,
                        'media_info': media_info
                    })

            # 发送媒体组
            if media_list:
                if forwarded_msg:
                    # 如果有转发消息，尝试使用第一个媒体编辑原消息，然后发送其余媒体
                    try:
                        # 获取原消息文本
                        original_text = forwarded_msg.text or forwarded_msg.caption or ""

                        if len(media_list) == 1:
                            # 如果只有一个媒体，直接编辑原消息
                            media = media_list[0]
                            await self.edit_message_with_media(
                                channel_id=channel_id,
                                message_id=forwarded_msg.message_id,
                                text=original_text,
                                media_path=media['path'],
                                media_type=media['type'],
                                media_info=media['media_info']
                            )
                            logging.info(f"成功编辑原消息添加媒体: {forwarded_msg.message_id}")
                        else:
                            # 如果有多个媒体，编辑第一个，其余作为新消息发送
                            # 将原消息文本添加到第一个媒体
                            first_media = media_list[0]
                            remaining_media = media_list[1:]

                            # 编辑第一个媒体到原消息
                            await self.edit_message_with_media(
                                channel_id=channel_id,
                                message_id=forwarded_msg.message_id,
                                text=original_text,
                                media_path=first_media['path'],
                                media_type=first_media['type'],
                                media_info=first_media['media_info']
                            )
                            logging.info(f"成功编辑原消息添加第一个媒体: {forwarded_msg.message_id}")

                            # 发送剩余媒体作为媒体组
                            if remaining_media:
                                # 创建媒体文件的副本，防止并发删除
                                safe_media_list = []
                                for media in remaining_media:
                                    try:
                                        # 检查文件是否存在
                                        if os.path.exists(media['path']):
                                            # 确保媒体缓存目录存在
                                            media_cache_dir = "data/media_cache"
                                            os.makedirs(media_cache_dir, exist_ok=True)

                                            # 创建新的临时文件
                                            with open(media['path'], 'rb') as src_file:
                                                content = src_file.read()
                                                tmp = NamedTemporaryFile(delete=False, prefix='tg_copy_', suffix=f'.{media["type"]}', dir=media_cache_dir)
                                                tmp.write(content)
                                                tmp.close()

                                                # 更新媒体路径
                                                new_media = media.copy()
                                                new_media['path'] = tmp.name
                                                # 记录临时文件
                                                self.temp_files[tmp.name] = datetime.now()
                                                safe_media_list.append(new_media)
                                                logging.info(f"创建媒体文件副本: {tmp.name}")
                                        else:
                                            logging.warning(f"媒体文件不存在，无法创建副本: {media['path']}")
                                            # 尝试使用缓存文件
                                            if 'cache_path' in media and os.path.exists(media['cache_path']):
                                                new_media = media.copy()
                                                new_media['path'] = media['cache_path']
                                                safe_media_list.append(new_media)
                                                logging.info(f"使用缓存文件作为副本: {media['cache_path']}")
                                    except Exception as e:
                                        logging.error(f"创建媒体文件副本失败: {str(e)}")

                                # 使用安全的媒体列表发送
                                if safe_media_list:
                                    try:
                                        # 尝试作为媒体组发送
                                        # 使用编辑模式而不是回复模式
                                        # 创建一个新的文本消息用于编辑
                                        try:
                                            # 发送一个空消息用于编辑，使用回复形式
                                            temp_msg = await self.bot.send_message(
                                                chat_id=channel_id,
                                                text="正在加载媒体...",
                                                disable_web_page_preview=True,
                                                reply_to_message_id=forwarded_msg.message_id  # 使用回复形式
                                            )

                                            # 编辑第一个媒体
                                            if safe_media_list and len(safe_media_list) > 0:
                                                first_media = safe_media_list[0]
                                                await self.edit_message_with_media(
                                                    channel_id=channel_id,
                                                    message_id=temp_msg.message_id,
                                                    text="",  # 空文本
                                                    media_path=first_media['path'],
                                                    media_type=first_media['type'],
                                                    media_info=first_media['media_info']
                                                )

                                                # 如果有多个媒体，发送剩余的
                                                if len(safe_media_list) > 1:
                                                    remaining_media = safe_media_list[1:]
                                                    # 使用回复形式发送剩余媒体
                                                    await self.send_media_group(channel_id, remaining_media, forwarded_msg.message_id)

                                            logging.info(f"成功发送剩余{len(safe_media_list)}个媒体作为媒体组")
                                        except Exception as edit_error:
                                            logging.error(f"编辑模式发送失败，尝试回复模式: {str(edit_error)}")
                                            # 如果编辑模式失败，回退到回复模式
                                            await self.send_media_group(channel_id, safe_media_list, forwarded_msg.message_id)
                                            logging.info(f"成功发送剩余{len(safe_media_list)}个媒体作为回复")
                                    except Exception as e:
                                        logging.error(f"发送媒体组失败，尝试逐个发送: {str(e)}")
                                        # 如果媒体组发送失败，尝试逐个发送
                                        for media in safe_media_list:
                                            try:
                                                # 发送单个媒体
                                                with open(media['path'], 'rb') as media_file:
                                                    file_data = media_file.read()
                                                    send_kwargs = {
                                                        'chat_id': channel_id,
                                                        'caption': media['caption'],
                                                        'reply_to_message_id': forwarded_msg.message_id,
                                                        'read_timeout': 1800,
                                                        'write_timeout': 1800
                                                    }

                                                    if media['type'] == 'photo':
                                                        send_kwargs['photo'] = file_data
                                                        await self.bot.send_photo(**send_kwargs)
                                                    elif media['type'] == 'video':
                                                        send_kwargs['video'] = file_data
                                                        send_kwargs['supports_streaming'] = True

                                                        # 添加视频参数
                                                        media_info = media['media_info']
                                                        if 'width' in media_info:
                                                            send_kwargs['width'] = media_info['width']
                                                        if 'height' in media_info:
                                                            send_kwargs['height'] = media_info['height']
                                                        if 'duration' in media_info:
                                                            send_kwargs['duration'] = media_info['duration']

                                                        await self.bot.send_video(**send_kwargs)
                                                    elif media['type'] == 'document':
                                                        send_kwargs['document'] = file_data
                                                        if 'filename' in media['media_info']:
                                                            send_kwargs['filename'] = media['media_info']['filename']
                                                        await self.bot.send_document(**send_kwargs)
                                                logging.info("成功发送单个媒体作为回复")
                                            except Exception as e2:
                                                logging.error(f"发送单个媒体失败: {str(e2)}")
                    except Exception as e:
                        logging.error(f"编辑原消息失败，尝试作为回复发送媒体组: {str(e)}")
                        # 如果编辑失败，尝试作为回复发送整个媒体组
                        await self.send_media_group(channel_id, media_list, forwarded_msg.message_id)
                else:
                    # 如果没有转发消息，直接发送媒体组
                    reply_to_message_id = forwarded_msg.message_id if forwarded_msg else None
                    await self.send_media_group(channel_id, media_list, reply_to_message_id)

        except Exception as e:
            logging.error(f"处理媒体组时出错: {str(e)}")
            logging.error(traceback.format_exc())

    async def send_media_group(self, channel_id, media_list, reply_to_message_id=None):
        """发送媒体组"""
        try:
            # 如果只有一个媒体文件，使用单个发送
            if len(media_list) == 1:
                media = media_list[0]
                with open(media['path'], 'rb') as media_file:
                    file_data = media_file.read()
                    send_kwargs = {
                        'chat_id': channel_id,
                        'caption': media['caption'],
                        'read_timeout': 1800,
                        'write_timeout': 1800
                    }

                    if reply_to_message_id:
                        send_kwargs['reply_to_message_id'] = reply_to_message_id

                    if media['type'] == 'photo':
                        send_kwargs['photo'] = file_data
                        await self.bot.send_photo(**send_kwargs)
                    elif media['type'] == 'video':
                        send_kwargs['video'] = file_data
                        send_kwargs['supports_streaming'] = True

                        # 添加视频参数
                        media_info = media['media_info']
                        if 'width' in media_info:
                            send_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            send_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']

                        await self.bot.send_video(**send_kwargs)
                    elif media['type'] == 'document':
                        send_kwargs['document'] = file_data
                        if 'filename' in media['media_info']:
                            send_kwargs['filename'] = media['media_info']['filename']
                        await self.bot.send_document(**send_kwargs)

            # 如果有多个媒体文件，使用媒体组发送
            else:
                # 准备媒体输入列表
                from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio, InputMediaAnimation
                input_media = []

                for i, media in enumerate(media_list):
                    with open(media['path'], 'rb') as media_file:
                        file_data = media_file.read()
                        caption = media['caption'] if i == 0 else None  # 只在第一个媒体上显示标题

                        if media['type'] == 'photo':
                            input_media.append(InputMediaPhoto(
                                media=file_data,
                                caption=caption,
                                parse_mode='Markdown' if caption else None
                            ))
                        elif media['type'] == 'video':
                            media_kwargs = {
                                'media': file_data,
                                'caption': caption,
                                'parse_mode': 'Markdown' if caption else None,
                                'supports_streaming': True
                            }

                            # 添加视频参数
                            media_info = media['media_info']
                            if 'width' in media_info:
                                media_kwargs['width'] = media_info['width']
                            if 'height' in media_info:
                                media_kwargs['height'] = media_info['height']
                            if 'duration' in media_info:
                                media_kwargs['duration'] = media_info['duration']

                            input_media.append(InputMediaVideo(**media_kwargs))
                        elif media['type'] == 'audio':
                            audio_kwargs = {
                                'media': file_data,
                                'caption': caption,
                                'parse_mode': 'Markdown' if caption else None
                            }

                            # 添加音频参数
                            media_info = media['media_info']
                            if 'duration' in media_info:
                                audio_kwargs['duration'] = media_info['duration']
                            if 'performer' in media_info:
                                audio_kwargs['performer'] = media_info['performer']
                            if 'title' in media_info:
                                audio_kwargs['title'] = media_info['title']

                            input_media.append(InputMediaAudio(**audio_kwargs))
                        elif media['type'] == 'animation':
                            anim_kwargs = {
                                'media': file_data,
                                'caption': caption,
                                'parse_mode': 'Markdown' if caption else None
                            }

                            # 添加动画参数
                            media_info = media['media_info']
                            if 'width' in media_info:
                                anim_kwargs['width'] = media_info['width']
                            if 'height' in media_info:
                                anim_kwargs['height'] = media_info['height']
                            if 'duration' in media_info:
                                anim_kwargs['duration'] = media_info['duration']

                            input_media.append(InputMediaAnimation(**anim_kwargs))
                        elif media['type'] == 'document':
                            doc_kwargs = {
                                'media': file_data,
                                'caption': caption,
                                'parse_mode': 'Markdown' if caption else None
                            }

                            if 'filename' in media['media_info']:
                                doc_kwargs['filename'] = media['media_info']['filename']
                            if 'mime_type' in media['media_info']:
                                doc_kwargs['mime_type'] = media['media_info']['mime_type']

                            input_media.append(InputMediaDocument(**doc_kwargs))
                        else:
                            # 如果是未知类型，默认作为文档处理
                            input_media.append(InputMediaDocument(
                                media=file_data,
                                caption=caption,
                                parse_mode='Markdown' if caption else None
                            ))

                # 发送媒体组
                await self.bot.send_media_group(
                    chat_id=channel_id,
                    media=input_media,
                    reply_to_message_id=reply_to_message_id,
                    read_timeout=1800,
                    write_timeout=1800
                )

            # 清理媒体文件
            for media in media_list:
                await self.cleanup_file(media['path'])

            logging.info(f"媒体组发送成功，共 {len(media_list)} 个文件")

        except Exception as e:
            logging.error(f"发送媒体组时出错: {str(e)}")
            logging.error(traceback.format_exc())

            # 如果失败，尝试逐个发送
            try:
                for media in media_list:
                    with open(media['path'], 'rb') as media_file:
                        file_data = media_file.read()
                        send_kwargs = {
                            'chat_id': channel_id,
                            'caption': media['caption'],
                            'reply_to_message_id': reply_to_message_id,
                            'read_timeout': 1800,
                            'write_timeout': 1800
                        }

                        if media['type'] == 'photo':
                            send_kwargs['photo'] = file_data
                            await self.bot.send_photo(**send_kwargs)
                        elif media['type'] == 'video':
                            send_kwargs['video'] = file_data
                            send_kwargs['supports_streaming'] = True
                            await self.bot.send_video(**send_kwargs)
                        elif media['type'] == 'audio':
                            send_kwargs['audio'] = file_data
                            await self.bot.send_audio(**send_kwargs)
                        elif media['type'] == 'animation':
                            send_kwargs['animation'] = file_data
                            await self.bot.send_animation(**send_kwargs)
                        elif media['type'] == 'document':
                            send_kwargs['document'] = file_data
                            await self.bot.send_document(**send_kwargs)

                    # 清理媒体文件
                    await self.cleanup_file(media['path'])

            except Exception as e2:
                logging.error(f"备用方法发送媒体失败: {str(e2)}")
                # 清理媒体文件
                for media in media_list:
                    await self.cleanup_file(media['path'])

    async def handle_sticker_send(self, message, channel_id, from_chat, reply_to_message_id=None):
        """处理贴图发送"""
        try:
            logging.info("开始处理贴图发送")

            # 下载贴图文件
            sticker_path = await self.client.download_media(message.media)
            logging.info(f"贴图下载完成: {sticker_path}")

            # 构建用户名部分
            username = f"(@{from_chat.username})" if getattr(from_chat, 'username', None) else ""

            # 使用简化的模板作为贴图标题
            caption = f"📨 转发自 {getattr(from_chat, 'title', 'Unknown Channel')} {username}"

            # 发送贴图
            with open(sticker_path, 'rb') as sticker_file:
                await self.bot.send_sticker(
                    chat_id=channel_id,
                    sticker=sticker_file.read(),
                    reply_to_message_id=reply_to_message_id
                )

                # 如果有标题且没有回复消息，发送标题
                if caption and not reply_to_message_id:
                    await self.bot.send_message(
                        chat_id=channel_id,
                        text=caption,
                        disable_web_page_preview=True
                    )

            # 清理临时文件
            if sticker_path and os.path.exists(sticker_path):
                os.remove(sticker_path)
                logging.info(f"贴图文件已清理: {sticker_path}")

            logging.info("贴图发送成功")

        except Exception as e:
            logging.error(f"发送贴图时出错: {str(e)}")
            logging.error(f"错误详情: {traceback.format_exc()}")

            # 如果失败，发送错误消息
            try:
                await self.bot.send_message(
                    chat_id=channel_id,
                    text=f"⚠️ 贴图发送失败: {str(e)}",
                    reply_to_message_id=reply_to_message_id
                )
            except Exception as e2:
                logging.error(f"发送错误消息失败: {str(e2)}")

            # 清理临时文件
            if 'sticker_path' in locals() and sticker_path and os.path.exists(sticker_path):
                try:
                    os.remove(sticker_path)
                except Exception as e3:
                    logging.error(f"清理贴图文件失败: {str(e3)}")

    async def handle_custom_emoji(self, message, channel_id):
        """处理自定义表情和特殊格式消息"""
        try:
            # 检查消息中的自定义表情实体
            has_custom_emoji = False
            has_special_format = False
            content = ""

            # 获取消息内容
            if hasattr(message, 'text') and message.text:
                content = message.text
            elif hasattr(message, 'caption') and message.caption:
                content = message.caption

            # 检查内容是否包含特殊格式
            if content and ('$' in content or '@' in content or '#' in content or
                           '\ud83c' in content or '\ud83d' in content or '\ud83e' in content):
                has_special_format = True

            # 检查 entities 属性
            if hasattr(message, 'entities') and message.entities:
                for entity in message.entities:
                    if hasattr(entity, 'CONSTRUCTOR_ID') and entity.CONSTRUCTOR_ID == 0x81ccf4d:  # MessageEntityCustomEmoji
                        has_custom_emoji = True
                        break
                    elif hasattr(entity, '__class__') and 'MessageEntityCustomEmoji' in str(entity.__class__):
                        has_custom_emoji = True
                        break
                    # 检查是否有代码块、表格等复杂格式
                    elif hasattr(entity, 'type') and entity.type in ['code', 'pre', 'text_link', 'mention']:
                        has_special_format = True

            # 检查 caption_entities 属性
            if not has_custom_emoji and hasattr(message, 'caption_entities') and message.caption_entities:
                for entity in message.caption_entities:
                    if hasattr(entity, 'CONSTRUCTOR_ID') and entity.CONSTRUCTOR_ID == 0x81ccf4d:  # MessageEntityCustomEmoji
                        has_custom_emoji = True
                        break
                    elif hasattr(entity, '__class__') and 'MessageEntityCustomEmoji' in str(entity.__class__):
                        has_custom_emoji = True
                        break
                    # 检查是否有代码块、表格等复杂格式
                    elif hasattr(entity, 'type') and entity.type in ['code', 'pre', 'text_link', 'mention']:
                        has_special_format = True

            # 检查是否有加密货币交易信号的特征
            if content and ('SIGNAL' in content.upper() or 'LONG' in content.upper() or 'SHORT' in content.upper() or
                           'ENTRY' in content.upper() or 'STOPLOSS' in content.upper() or
                           'BUY' in content.upper() or 'SELL' in content.upper()):
                has_special_format = True

            if has_custom_emoji:
                logging.info("检测到自定义表情，添加提示消息并禁用Markdown解析")
                await self.bot.send_message(
                    chat_id=channel_id,
                    text="ℹ️ 原消息包含自定义表情，可能无法完全显示。"
                )
                return True
            elif has_special_format:
                logging.info("检测到特殊格式消息，禁用Markdown解析")
                return True

            return False
        except Exception as e:
            logging.error(f"处理自定义表情时出错: {str(e)}")
            return False

    async def handle_media_edit(self, message, channel_id, media_type, forwarded_msg, from_chat=None):
        """处理媒体消息编辑，将媒体添加到已转发的文本消息中"""
        try:
            logging.info(f"开始处理媒体编辑: 类型={media_type}, 消息 ID={forwarded_msg.message_id}")

            # 下载媒体文件
            media_info = await self.download_media_file(message, media_type)
            if not media_info:
                logging.error("媒体文件下载失败")
                return

            file_path = media_info.get('file_path')
            media_type = media_info.get('media_type')

            # 获取原消息文本
            original_text = forwarded_msg.text or forwarded_msg.caption or ""

            # 检查原消息是否包含可能导致解析错误的内容
            has_potential_parsing_issues = False
            if original_text and ('$' in original_text or '@' in original_text or '#' in original_text or
                                 '\ud83c' in original_text or '\ud83d' in original_text or '\ud83e' in original_text):
                has_potential_parsing_issues = True
                logging.info("检测到可能导致解析问题的内容，将使用更安全的处理方式")

            if has_potential_parsing_issues:
                # 对于可能有解析问题的消息，直接使用回复方式发送媒体
                try:
                    await self.handle_media_send(
                        message=message,
                        channel_id=channel_id,
                        media_type=media_type,
                        reply_to_message_id=forwarded_msg.message_id,
                        from_chat=from_chat
                    )
                    logging.info("使用回复方式成功发送媒体")
                    return
                except Exception as reply_error:
                    logging.error(f"回复方式发送媒体失败: {str(reply_error)}")
                    # 继续尝试编辑方式

            # 使用编辑消息方式添加媒体
            try:
                await self.edit_message_with_media(
                    channel_id=channel_id,
                    message_id=forwarded_msg.message_id,
                    text=original_text,
                    media_path=file_path,
                    media_type=media_type,
                    media_info=media_info
                )
                logging.info(f"成功将媒体添加到消息: {forwarded_msg.message_id}")
            except telegram_error.BadRequest as br_error:
                # 处理特定的错误
                if "can't parse entities" in str(br_error).lower():
                    logging.warning(f"编辑消息时出现实体解析错误，尝试使用纯文本: {br_error}")
                    # 尝试使用纯文本编辑
                    try:
                        await self.edit_message_with_media(
                            channel_id=channel_id,
                            message_id=forwarded_msg.message_id,
                            text=original_text,
                            media_path=file_path,
                            media_type=media_type,
                            media_info=media_info,
                            force_plain_text=True
                        )
                        logging.info("使用纯文本模式成功编辑消息添加媒体")
                    except Exception as plain_error:
                        logging.error(f"纯文本编辑也失败，尝试回复方式: {str(plain_error)}")
                        raise  # 继续抛出异常，让下面的代码处理
                else:
                    # 其他BadRequest错误，继续抛出
                    raise

        except Exception as e:
            logging.error(f"处理媒体编辑时出错: {str(e)}")
            logging.error(traceback.format_exc())

            # 如果编辑失败，尝试作为回复发送媒体
            try:
                if 'file_path' in locals() and os.path.exists(file_path):
                    await self.handle_media_send(
                        message=message,
                        channel_id=channel_id,
                        media_type=media_type,
                        reply_to_message_id=forwarded_msg.message_id,
                        from_chat=from_chat
                    )
                    logging.info("使用回复方式成功发送媒体（作为备用方法）")
            except Exception as e2:
                logging.error(f"备用方法发送媒体失败: {str(e2)}")

    async def edit_message_with_media(self, channel_id, message_id, text, media_path, media_type, media_info, force_plain_text=False):
        """编辑消息以包含媒体文件

        Args:
            channel_id: 频道ID
            message_id: 消息ID
            text: 消息文本
            media_path: 媒体文件路径
            media_type: 媒体类型
            media_info: 媒体信息字典
            force_plain_text: 是否强制使用纯文本模式（不使用Markdown）
        """
        try:
            logging.info(f"开始编辑消息添加媒体: 消息 ID={message_id}, 媒体类型={media_type}")

            # 使用编辑消息API而不是删除重发
            with open(media_path, 'rb') as media_file:
                file_data = media_file.read()

                try:
                    # 检查消息是否包含自定义表情或强制纯文本
                    has_custom_emoji = False
                    if text and ('\ud83c' in text or '\ud83d' in text or '\ud83e' in text):
                        # 简单检测是否可能包含表情
                        has_custom_emoji = True
                        logging.info("检测到可能包含表情的消息，禁用Markdown解析")

                    # 确定是否使用Markdown
                    use_markdown = not (has_custom_emoji or force_plain_text)
                    if force_plain_text:
                        logging.info("强制使用纯文本模式")

                    # 准备媒体对象
                    if media_type == 'photo':
                        from telegram import InputMediaPhoto
                        media = InputMediaPhoto(
                            media=file_data,
                            caption=text,
                            parse_mode='Markdown' if use_markdown else None
                        )
                    elif media_type == 'video':
                        from telegram import InputMediaVideo
                        media_kwargs = {
                            'media': file_data,
                            'caption': text,
                            'parse_mode': 'Markdown' if use_markdown else None,
                            'supports_streaming': True
                        }

                        # 添加视频参数
                        if 'width' in media_info:
                            media_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            media_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            media_kwargs['duration'] = media_info['duration']

                        media = InputMediaVideo(**media_kwargs)
                    elif media_type == 'audio':
                        from telegram import InputMediaAudio
                        audio_kwargs = {
                            'media': file_data,
                            'caption': text,
                            'parse_mode': 'Markdown' if use_markdown else None
                        }

                        # 添加音频参数
                        if 'duration' in media_info:
                            audio_kwargs['duration'] = media_info['duration']
                        if 'performer' in media_info:
                            audio_kwargs['performer'] = media_info['performer']
                        if 'title' in media_info:
                            audio_kwargs['title'] = media_info['title']

                        media = InputMediaAudio(**audio_kwargs)
                    elif media_type == 'animation':
                        from telegram import InputMediaAnimation
                        anim_kwargs = {
                            'media': file_data,
                            'caption': text,
                            'parse_mode': 'Markdown' if use_markdown else None
                        }

                        # 添加动画参数
                        if 'width' in media_info:
                            anim_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            anim_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            anim_kwargs['duration'] = media_info['duration']

                        media = InputMediaAnimation(**anim_kwargs)
                    elif media_type == 'document':
                        from telegram import InputMediaDocument
                        doc_kwargs = {
                            'media': file_data,
                            'caption': text,
                            'parse_mode': 'Markdown' if use_markdown else None
                        }

                        if 'filename' in media_info:
                            doc_kwargs['filename'] = media_info['filename']

                        media = InputMediaDocument(**doc_kwargs)
                    else:
                        # 如果是未知类型，默认作为文档处理
                        from telegram import InputMediaDocument
                        media = InputMediaDocument(
                            media=file_data,
                            caption=text,
                            parse_mode='Markdown' if use_markdown else None
                        )

                    # 编辑消息媒体
                    await self.bot.edit_message_media(
                        chat_id=channel_id,
                        message_id=message_id,
                        media=media,
                        read_timeout=1800,
                        write_timeout=1800
                    )

                    logging.info(f"成功编辑消息并添加{media_type}")
                except Exception as edit_error:
                    logging.error(f"编辑消息媒体失败，尝试删除重发: {str(edit_error)}")

                    # 删除原消息并重新发送
                    await self.bot.delete_message(
                        chat_id=channel_id,
                        message_id=message_id
                    )

                    # 根据媒体类型发送新消息
                    send_kwargs = {
                        'chat_id': channel_id,
                        'caption': text,
                        'parse_mode': 'Markdown' if use_markdown else None,
                        'read_timeout': 1800,
                        'write_timeout': 1800
                    }

                    if media_type == 'photo':
                        send_kwargs['photo'] = file_data
                        await self.bot.send_photo(**send_kwargs)
                    elif media_type == 'video':
                        send_kwargs['video'] = file_data
                        send_kwargs['supports_streaming'] = True

                        # 添加视频参数
                        if 'width' in media_info:
                            send_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            send_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            with open(media_info['thumb_path'], 'rb') as thumb_file:
                                send_kwargs['thumb'] = thumb_file.read()

                        await self.bot.send_video(**send_kwargs)

                        # 清理缩略图
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            os.remove(media_info['thumb_path'])
                    elif media_type == 'audio':
                        send_kwargs['audio'] = file_data

                        # 添加音频参数
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']
                        if 'performer' in media_info:
                            send_kwargs['performer'] = media_info['performer']
                        if 'title' in media_info:
                            send_kwargs['title'] = media_info['title']

                        await self.bot.send_audio(**send_kwargs)
                    elif media_type == 'animation':
                        send_kwargs['animation'] = file_data

                        # 添加动画参数
                        if 'width' in media_info:
                            send_kwargs['width'] = media_info['width']
                        if 'height' in media_info:
                            send_kwargs['height'] = media_info['height']
                        if 'duration' in media_info:
                            send_kwargs['duration'] = media_info['duration']
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            with open(media_info['thumb_path'], 'rb') as thumb_file:
                                send_kwargs['thumb'] = thumb_file.read()

                        await self.bot.send_animation(**send_kwargs)

                        # 清理缩略图
                        if 'thumb_path' in media_info and os.path.exists(media_info['thumb_path']):
                            os.remove(media_info['thumb_path'])
                    elif media_type == 'document':
                        send_kwargs['document'] = file_data
                        if 'filename' in media_info:
                            send_kwargs['filename'] = media_info['filename']
                        await self.bot.send_document(**send_kwargs)

                    logging.info(f"使用删除重发方式成功添加{media_type}")

            # 清理媒体文件
            await self.cleanup_file(media_path)

        except Exception as e:
            logging.error(f"编辑消息添加媒体失败: {str(e)}")
            # 如果失败，尝试恢复原消息
            try:
                # 检查消息是否包含自定义表情
                has_emoji = False
                if text and ('\ud83c' in text or '\ud83d' in text or '\ud83e' in text):
                    has_emoji = True

                await self.bot.send_message(
                    chat_id=channel_id,
                    text=text + "\n\n⚠️ " + ("媒体文件加载失败" if has_emoji else "*媒体文件加载失败*"),
                    parse_mode=None if has_emoji else 'Markdown',
                    disable_web_page_preview=True
                )
            except Exception as e2:
                logging.error(f"恢复消息失败: {str(e2)}")

            # 清理媒体文件
            await self.cleanup_file(media_path)
