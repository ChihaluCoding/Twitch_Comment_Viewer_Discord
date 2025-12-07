"""DiscordとTwitchを連携させる橋渡しBot"""
# 日本語コメント: 非同期処理や環境変数、ロギングを扱うための標準ライブラリ
import asyncio
import logging
import os
import time
from dataclasses import dataclass
from contextlib import suppress
from typing import Awaitable, Callable
from asyncio import QueueEmpty

# 日本語コメント: Helix APIへHTTPリクエストを送るためのaiohttpを読み込む
import aiohttp

# 日本語コメント: DiscordとTwitchの各種クライアントライブラリを読み込む
import discord
from discord.abc import Messageable
from discord.ext import commands
from discord.utils import escape_mentions
from dotenv import load_dotenv
from twitchio import Client
from twitchio.message import Message


# 日本語コメント: 必須設定が不足している際に使う例外クラス
class MissingSettingError(RuntimeError):
    """必須の環境変数が見つからない場合に投げる例外"""


# 日本語コメント: 文字列の環境変数を取得し、存在しなければ例外とする
def require_env(name: str) -> str:
    """環境変数を取得し、未設定ならMissingSettingErrorを投げる"""
    value = os.getenv(name)
    if not value:
        raise MissingSettingError(f"環境変数 {name} が設定されていません。")
    return value


# 日本語コメント: 数値の環境変数を整数として取得するユーティリティ
def require_int_env(name: str) -> int:
    """整数環境変数を検証付きで取得する"""
    raw = require_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise MissingSettingError(f"環境変数 {name} には整数を設定してください。") from exc


# 日本語コメント: カンマ区切りのチャンネル一覧文字列を正規化してリスト化
def parse_channel_list(raw: str) -> list[str]:
    """チャンネル名のカンマ区切り文字列を重複なしのリストへ変換"""
    parts = [part.strip() for part in raw.split(",")]
    normalized: list[str] = []
    for part in parts:
        if not part:
            continue
        lower = part.lower()
        if lower not in normalized:
            normalized.append(lower)
    if not normalized:
        raise MissingSettingError("TWITCH_CHANNELS には少なくとも1件のチャンネル名を指定してください。")
    return normalized


# 日本語コメント: TwitchのemoteタグをURLへ展開し、本文に貼り付けてDiscordで見やすくする
def expand_twitch_emotes(content: str, emotes_tag: str | None) -> str:
    """emotesタグを基に本文へCDNのサムネイルURLを埋め込む"""
    if not emotes_tag:
        return content
    # 日本語コメント: tag形式例 "25:0-4/1902:6-10" をパースして位置情報を抽出
    entries: list[tuple[int, int, str]] = []
    for group in emotes_tag.split("/"):
        if not group or ":" not in group:
            continue
        emote_id, positions = group.split(":", 1)
        for pos in positions.split(","):
            if "-" not in pos:
                continue
            start_s, end_s = pos.split("-", 1)
            try:
                start = int(start_s)
                end = int(end_s)
            except ValueError:
                continue
            entries.append((start, end, emote_id))
    if not entries:
        return content
    # 日本語コメント: 位置順にソートしてテキストを組み立てつつURLを挿入
    parts: list[str] = []
    cursor = 0
    for start, end, emote_id in sorted(entries, key=lambda item: item[0]):
        if cursor < start:
            parts.append(content[cursor:start])
        name = content[start : end + 1]
        url = f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/1.0"
        parts.append(f"{name} {url}")
        cursor = end + 1
    if cursor < len(content):
        parts.append(content[cursor:])
    return "".join(parts)


# 日本語コメント: Helix Streams APIから取得したライブ配信情報のコンテナ
@dataclass
class StreamInfo:
    """配信中チャンネルの基本情報を保持するデータクラス"""

    user_login: str
    game_name: str
    thumbnail_url: str
    title: str
    started_at: str


# 日本語コメント: Discordへ送るために整形済みのメッセージ要素を保持するデータクラス
@dataclass
class RelayMessage:
    """Discord送信用に整形前のTwitchメッセージ要素を保持"""

    display_name: str
    channel_name: str
    content: str
    emotes_tag: str | None


# 日本語コメント: Twitchのチャットを購読してコールバックへ流すClient拡張クラス
class TwitchMessageClient(Client):
    """TwitchのチャットメッセージをDiscord側へ渡す仲介クライアント"""

    def __init__(
        self,
        twitch_token: str,
        twitch_client_secret: str | None,
        channel_names: list[str],
        on_message: Callable[[Message], Awaitable[None]],
        on_ready: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        # 日本語コメント: 親クラスへ複数チャンネル分の接続情報を渡して初期化
        sanitized_channels = [name.strip().lower() for name in channel_names if name.strip()]
        super().__init__(
            token=twitch_token,
            initial_channels=sanitized_channels,
            client_secret=twitch_client_secret,
        )
        # 日本語コメント: メッセージ受信時に呼び出すコールバック
        self._on_message = on_message
        # 日本語コメント: 接続完了時に呼び出すコールバック
        self._on_ready = on_ready

    async def event_ready(self) -> None:
        """Twitch側と接続できた際に呼ばれるイベント"""
        logging.info("Twitchへの接続が完了しました。")
        if self._on_ready:
            await self._on_ready()

    async def event_message(self, message: Message) -> None:
        """Twitchからメッセージを受信した際のイベント"""
        if message.echo:
            return
        if not message.content:
            return
        await self._on_message(message)


# 日本語コメント: DiscordとTwitch間の橋渡しを担うメインクラス
class TwitchDiscordBridge:
    """TwitchチャットをDiscordチャンネルへ転送する橋渡しBot"""

    def __init__(
        self,
        discord_token: str,
        discord_channel_id: int,
        twitch_token: str,
        twitch_client_id: str,
        twitch_client_secret: str,
        twitch_channels: list[str],
    ) -> None:
        # 日本語コメント: DiscordとTwitch双方の認証情報を保持
        self._discord_token = discord_token
        self._discord_channel_id = discord_channel_id
        self._twitch_client_id = twitch_client_id
        self._twitch_client_secret = twitch_client_secret
        self._twitch_channels = twitch_channels
        # 日本語コメント: Twitchからのメッセージを一時的に溜めるキュー
        self._message_queue: asyncio.Queue[RelayMessage] = asyncio.Queue()
        # 日本語コメント: 中継の開始/停止を制御する同期用Event
        self._relay_gate = asyncio.Event()
        # 日本語コメント: Discord Botの基本設定（メッセージ内容の取得を有効化）
        intents = discord.Intents.default()
        intents.message_content = True
        self._discord_bot = commands.Bot(command_prefix="!", intents=intents)
        # 日本語コメント: Twitchメッセージクライアントを生成
        self._twitch_client = TwitchMessageClient(
            twitch_token=twitch_token,
            twitch_client_secret=twitch_client_secret,
            channel_names=twitch_channels,
            on_message=self.enqueue_twitch_message,
        )
        # 日本語コメント: 起動中のタスク参照を保持
        self._relay_task: asyncio.Task[None] | None = None
        self._twitch_task: asyncio.Task[None] | None = None
        self._stream_monitor_task: asyncio.Task[None] | None = None
        # 日本語コメント: スラッシュコマンド同期状態を保持
        self._tree_synced = False
        # 日本語コメント: 手動停止フラグと配信状況に基づく停止フラグ
        self._is_relay_paused = False
        self._is_live_blocked = False
        self._is_stream_status_known = False
        self._live_channels: set[str] = set()
        # 日本語コメント: 最新の配信情報を保持して表示用のメタデータを提供
        self._live_streams: dict[str, StreamInfo] = {}
        # 日本語コメント: Helix APIポーリング間隔とHTTPセッション関連
        self._stream_check_interval = 60
        self._http_session: aiohttp.ClientSession | None = None
        self._app_access_token: str | None = None
        self._app_access_token_expires_at = 0.0
        # 日本語コメント: カスタムエモート名と画像URLのキャッシュ（公式以外用）
        self._custom_emotes: dict[str, dict[str, str]] = {}
        # 日本語コメント: エモートIDとDiscordカスタム絵文字IDの対応キャッシュ
        self._emoji_cache: dict[str, int] = {}
        # 日本語コメント: 初期状態のゲートを正しい状態へ整える
        self._refresh_relay_gate()
        # 日本語コメント: Discordイベントやスラッシュコマンドを登録
        self._register_events()

    def _register_events(self) -> None:
        """Discord側のイベントとスラッシュコマンドを登録"""
        @self._discord_bot.event
        async def on_ready() -> None:
            logging.info("Discordへの接続が完了しました。")
            if not self._tree_synced:
                await self._discord_bot.tree.sync()
                self._tree_synced = True
            # 日本語コメント: 公式以外のカスタムエモートを事前取得
            await self._warm_custom_emotes()
            if self._twitch_task is None:
                self._twitch_task = asyncio.create_task(self._twitch_client.connect())
            if self._relay_task is None:
                self._relay_task = asyncio.create_task(self._relay_loop())
            if self._stream_monitor_task is None:
                self._stream_monitor_task = asyncio.create_task(self._stream_status_loop())

        @self._discord_bot.tree.command(name="watch", description="Twitch取得Botの稼働状況を確認します")
        async def watch_command(interaction: discord.Interaction) -> None:
            """簡易ヘルスチェック用コマンド"""
            status = "配信情報確認中" if not self._is_stream_status_known else (
                "配信停止中" if not self._is_live_blocked else "配信中のため中継停止"
            )
            embeds = [self._build_status_embed(channel) for channel in self._twitch_channels]
            await interaction.response.send_message(
                f"Twitch取得Botは稼働中です（状態: {status}）。",
                embeds=embeds,
                ephemeral=True,
            )

        @self._discord_bot.tree.command(name="start", description="TwitchからDiscordへの中継を開始します")
        async def start_command(interaction: discord.Interaction) -> None:
            """中継を再開するスラッシュコマンド"""
            if not self._is_stream_status_known:
                await interaction.response.send_message("配信状況を確認中です。しばらくお待ちください。", ephemeral=True)
                return
            if not self._is_relay_paused:
                if self._is_live_blocked:
                    await interaction.response.send_message("現在Twitchで配信中のため、中継は一時停止中です。", ephemeral=True)
                else:
                    await interaction.response.send_message("既に中継は開始されています。", ephemeral=True)
                return
            self._is_relay_paused = False
            self._refresh_relay_gate()
            if self._is_live_blocked:
                await interaction.response.send_message("中継を再開しましたが、現在は配信中のため自動停止状態です。", ephemeral=True)
            else:
                await interaction.response.send_message("TwitchからDiscordへの中継を開始しました。", ephemeral=True)

        @self._discord_bot.tree.command(name="stop", description="TwitchからDiscordへの中継を停止します")
        async def stop_command(interaction: discord.Interaction) -> None:
            """中継を停止するスラッシュコマンド"""
            if self._is_relay_paused:
                await interaction.response.send_message("中継は既に停止しています。", ephemeral=True)
                return
            self._is_relay_paused = True
            self._refresh_relay_gate()
            self._clear_message_queue()
            await interaction.response.send_message("TwitchからDiscordへの中継を停止しました。", ephemeral=True)

    def _refresh_relay_gate(self) -> None:
        """中継ゲートの開閉を状態に応じて制御"""
        if self._is_stream_status_known and not self._is_relay_paused and not self._is_live_blocked:
            self._relay_gate.set()
        else:
            self._relay_gate.clear()

    def _clear_message_queue(self) -> None:
        """停止中に溜まったメッセージを破棄"""
        while True:
            try:
                self._message_queue.get_nowait()
            except QueueEmpty:
                break
            else:
                self._message_queue.task_done()

    async def enqueue_twitch_message(self, message: Message) -> None:
        """Twitchから受信したメッセージをキューへ追加"""
        if self._is_relay_paused or self._is_live_blocked:
            logging.debug("停止/配信ブロック中のコメントを破棄しました")
            return
        display_name = message.author.display_name if message.author else "Unknown"
        channel_name = message.channel.name if message.channel else "UnknownChannel"
        emotes_tag = message.tags.get("emotes") if message.tags else None
        if not emotes_tag:
            # 日本語コメント: 公式Twitchエモート以外（BTTV/7TVなど）の場合、emotesタグが付かず変換できない
            logging.debug("emotesタグなしのためエモート置換をスキップしました（content=%s）", message.content)
        # 日本語コメント: darkmasuotvなど対象外チャンネルのチャットは中継しない
        if channel_name.lower() != "hikakin":
            logging.debug("対象外チャンネルのコメントを破棄しました（channel=%s）", channel_name)
            return
        # 日本語コメント: エモートは送信時にDiscord絵文字へ差し替えるため、元の文字列とタグを保存
        relay = RelayMessage(
            display_name=display_name,
            channel_name=channel_name,
            content=message.content,
            emotes_tag=emotes_tag,
        )
        await self._message_queue.put(relay)

    async def _relay_loop(self) -> None:
        """キューからDiscordへメッセージを転送するループ"""
        while True:
            relay = await self._message_queue.get()
            try:
                await self._relay_gate.wait()
                channel = await self._resolve_channel()
                formatted = await self._format_message_with_emotes(relay, channel)
                await channel.send(formatted)
            finally:
                self._message_queue.task_done()

    async def _resolve_channel(self) -> Messageable:
        """送信先のDiscordチャンネルを取得"""
        channel = self._discord_bot.get_channel(self._discord_channel_id)
        if channel is None:
            channel = await self._discord_bot.fetch_channel(self._discord_channel_id)
        if not isinstance(channel, Messageable):
            raise RuntimeError("指定したDiscordチャンネルはメッセージ送信に対応していません。")
        return channel

    async def _stream_status_loop(self) -> None:
        """Helix APIを使い配信状況を定期的に確認するループ"""
        while True:
            try:
                live_streams = await self._fetch_stream_live_streams()
                live_channels = set(live_streams.keys())
                previous_live_channels = self._live_channels
                self._live_channels = live_channels
                self._live_streams = live_streams
                previous = self._is_live_blocked
                self._is_live_blocked = bool(live_channels)
                self._is_stream_status_known = True
                if self._is_live_blocked:
                    self._clear_message_queue()
                if self._is_live_blocked and not previous:
                    joined = ", ".join(sorted(live_channels))
                    logging.info("Twitch配信が開始されたため中継を停止します。（ライブ中: %s）", joined)
                elif not self._is_live_blocked and previous:
                    joined = ", ".join(sorted(previous_live_channels))
                    logging.info("Twitch配信が終了したため中継を再開します。（終了: %s）", joined)
                self._refresh_relay_gate()
            except Exception:
                logging.exception("Twitchの配信状況チェックに失敗しました。一定時間後に再試行します。")
            await asyncio.sleep(self._stream_check_interval)

    async def _fetch_stream_live_streams(self) -> dict[str, StreamInfo]:
        """Helix Streams APIから対象チャンネルの配信状況を取得"""
        session = self._ensure_http_session()
        token = await self._get_app_access_token()
        headers = {
            "Client-ID": self._twitch_client_id,
            "Authorization": f"Bearer {token}",
        }
        live_streams: dict[str, StreamInfo] = {}
        for channel in self._twitch_channels:
            params = {"user_login": channel}
            async with session.get("https://api.twitch.tv/helix/streams", headers=headers, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise RuntimeError(f"Helix APIから配信情報を取得できませんでした: {error_text}")
                payload = await response.json()
            data = payload.get("data") or []
            for entry in data:
                if entry.get("type") == "live":
                    user_login = entry.get("user_login")
                    if user_login:
                        login = str(user_login).lower()
                        thumbnail_url = str(entry.get("thumbnail_url") or "")
                        if thumbnail_url:
                            thumbnail_url = (
                                thumbnail_url.replace("{width}", "1280").replace("{height}", "720")
                                + f"?t={int(time.time())}"
                            )
                        stream_info = StreamInfo(
                            user_login=login,
                            game_name=str(entry.get("game_name") or "未設定"),
                            thumbnail_url=thumbnail_url,
                            title=str(entry.get("title") or ""),
                            started_at=str(entry.get("started_at") or ""),
                        )
                        live_streams[login] = stream_info
        return live_streams

    async def _warm_custom_emotes(self) -> None:
        """チャンネルごとのカスタムエモート（BTTV/7TV）を事前取得"""
        for channel in self._twitch_channels:
            try:
                emotes = await self._fetch_custom_emotes(channel)
                self._custom_emotes[channel] = emotes
                logging.info("カスタムエモートを読み込みました: %s (%d件)", channel, len(emotes))
            except Exception:
                logging.exception("カスタムエモートの取得に失敗しました: %s", channel)

    async def _fetch_custom_emotes(self, channel_login: str) -> dict[str, str]:
        """BTTV/7TVのカスタムエモートをまとめて取得"""
        session = self._ensure_http_session()
        token = await self._get_app_access_token()
        headers = {
            "Client-ID": self._twitch_client_id,
            "Authorization": f"Bearer {token}",
        }
        params = {"login": channel_login}
        async with session.get("https://api.twitch.tv/helix/users", headers=headers, params=params) as response:
            if response.status != 200:
                raise RuntimeError(f"TwitchユーザーIDを取得できませんでした: {await response.text()}")
            payload = await response.json()
        data = payload.get("data") or []
        if not data:
            raise RuntimeError("指定チャンネルのユーザーIDが見つかりませんでした。")
        user_id = str(data[0].get("id") or "")
        if not user_id:
            raise RuntimeError("TwitchユーザーIDが空です。")

        emote_map: dict[str, str] = {}

        # 日本語コメント: BTTVのチャンネル/共有エモートを取得
        try:
            async with session.get(f"https://api.betterttv.net/3/cached/users/twitch/{user_id}") as response:
                if response.status == 200:
                    bttv_payload = await response.json()
                    for group in ("channelEmotes", "sharedEmotes"):
                        for emote in bttv_payload.get(group, []):
                            code = emote.get("code")
                            emote_id = emote.get("id")
                            if code and emote_id:
                                emote_map[code] = f"https://cdn.betterttv.net/emote/{emote_id}/3x"
                else:
                    logging.debug("BTTV取得をスキップ（status=%s）", response.status)
        except Exception:
            logging.exception("BTTVエモートの取得に失敗しました")

        # 日本語コメント: 7TVのチャンネルエモートを取得
        try:
            async with session.get(f"https://7tv.io/v3/users/twitch/{user_id}") as response:
                if response.status == 200:
                    seven_payload = await response.json()
                    emote_set = (seven_payload.get("emote_set") or {})
                    for emote in emote_set.get("emotes", []):
                        name = emote.get("name")
                        data = emote.get("data") or {}
                        host = data.get("host") or {}
                        base_url = host.get("url") or ""
                        files = host.get("files") or []
                        # 日本語コメント: 最大サイズのファイルパスを採用
                        chosen = ""
                        if isinstance(files, list) and files:
                            chosen = sorted(files, key=lambda f: f.get("width", 0), reverse=True)[0].get("name") or ""
                        url = base_url
                        if chosen:
                            url = f"{base_url}/{chosen}"
                        if url and name:
                            # 日本語コメント: 先頭のスキーム不足を補う
                            if url.startswith("//"):
                                url = "https:" + url
                            emote_map[name] = url
                else:
                    logging.debug("7TV取得をスキップ（status=%s）", response.status)
        except Exception:
            logging.exception("7TVエモートの取得に失敗しました")

        return emote_map

    def _ensure_http_session(self) -> aiohttp.ClientSession:
        """Helix API用のHTTPセッションを遅延生成"""
        if self._http_session is None or self._http_session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._http_session = aiohttp.ClientSession(timeout=timeout)
        return self._http_session

    async def _format_message_with_emotes(self, relay: RelayMessage, channel: Messageable) -> str:
        """Twitch絵文字をDiscordカスタム絵文字へ差し替えて整形"""
        guild = getattr(channel, "guild", None)
        # 日本語コメント: ギルドが取れない場合はエスケープのみで返す
        if guild is None:
            base = relay.content if relay.channel_name.lower() == "hikakin" else escape_mentions(relay.content)
            return f"{relay.display_name}: {base}"

        parts: list[str] = []
        content = relay.content

        # 日本語コメント: 公式Twitchエモートをemotesタグで差し替え
        entries: list[tuple[int, int, str]] = []
        if relay.emotes_tag:
            for group in relay.emotes_tag.split("/"):
                if not group or ":" not in group:
                    continue
                emote_id, positions = group.split(":", 1)
                for pos in positions.split(","):
                    if "-" not in pos:
                        continue
                    start_s, end_s = pos.split("-", 1)
                    try:
                        start = int(start_s)
                        end = int(end_s)
                    except ValueError:
                        continue
                    entries.append((start, end, emote_id))
        cursor = 0
        for start, end, emote_id in sorted(entries, key=lambda item: item[0]):
            if cursor < start:
                raw = content[cursor:start]
                parts.append(self._replace_custom_tokens(raw, relay.channel_name, guild))
            name = content[start : end + 1]
            emoji_str = await self._ensure_discord_emoji(emote_id, name, guild, None)
            if emoji_str:
                parts.append(emoji_str)
            else:
                parts.append(self._safe_text(name, relay.channel_name))
            cursor = end + 1
        if cursor < len(content):
            tail = content[cursor:]
            parts.append(self._replace_custom_tokens(tail, relay.channel_name, guild))
        return f"{relay.display_name}: {''.join(parts)}"

    def _safe_text(self, text: str, channel_name: str) -> str:
        """チャンネル別のメンションエスケープ適用"""
        return text if channel_name.lower() == "hikakin" else escape_mentions(text)

    def _replace_custom_tokens(self, text: str, channel_name: str, guild: discord.Guild) -> str:
        """公式タグ以外のカスタムエモート名をDiscord絵文字へ置換"""
        tokens = text.split(" ")
        replaced: list[str] = []
        emote_map = self._custom_emotes.get(channel_name.lower()) or {}
        for token in tokens:
            url = emote_map.get(token)
            if url:
                emoji_str = self._get_cached_custom_emoji(token, url, guild)
                if emoji_str:
                    replaced.append(emoji_str)
                    continue
            replaced.append(self._safe_text(token, channel_name))
        return " ".join(replaced)

    def _get_cached_custom_emoji(self, name: str, url: str, guild: discord.Guild) -> str | None:
        """既存のカスタム絵文字をURLキーで検索"""
        cache_key = f"custom:{name.lower()}:{url}"
        if cache_key in self._emoji_cache:
            emoji_id = self._emoji_cache[cache_key]
            emoji = discord.utils.get(guild.emojis, id=emoji_id)
            if emoji:
                return str(emoji)
        for emoji in guild.emojis:
            if emoji.name == f"tw_{name.lower()}":
                self._emoji_cache[cache_key] = emoji.id
                return str(emoji)
        return None

    async def _ensure_discord_emoji(
        self,
        emote_id: str,
        emote_name: str,
        guild: discord.Guild,
        image_url: str | None,
    ) -> str | None:
        """TwitchエモートID（またはカスタム名）に対応するDiscordカスタム絵文字を確保"""
        # 日本語コメント: キャッシュ優先
        cache_key = emote_id if image_url is None else f"custom:{emote_name.lower()}:{image_url}"
        if cache_key in self._emoji_cache:
            emoji_id = self._emoji_cache[cache_key]
            existing = discord.utils.get(guild.emojis, id=emoji_id)
            if existing:
                return str(existing)

        # 日本語コメント: 既存の絵文字を名前ベースで検索（再生成を避ける）
        for emoji in guild.emojis:
            if emoji.name == f"tw_{emote_id}" or (image_url and emoji.name == f"tw_{emote_name.lower()}"):
                self._emoji_cache[cache_key] = emoji.id
                return str(emoji)

        # 日本語コメント: 画像をダウンロードし、サイズを調整して登録
        session = self._ensure_http_session()
        url_candidates = (
            [
                f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/3.0",
                f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/2.0",
                f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/1.0",
            ]
            if image_url is None
            else [image_url]
        )
        image_bytes: bytes | None = None
        for url in url_candidates:
            async with session.get(url) as response:
                if response.status != 200:
                    continue
                data = await response.read()
            # 日本語コメント: Discordのカスタム絵文字上限(256KB)を満たすか確認
            if len(data) <= 256 * 1024:
                image_bytes = data
                break
        if image_bytes is None:
            logging.warning("エモート画像の取得に失敗しました: %s", emote_id)
            return None

        safe_base = emote_name.lower()
        safe_name = f"tw_{safe_base}"
        try:
            emoji = await guild.create_custom_emoji(name=safe_name, image=image_bytes, reason="Twitch emote import")
        except Exception:
            logging.exception("Discord絵文字の作成に失敗しました（emote_id=%s）", emote_id)
            return None
        self._emoji_cache[cache_key] = emoji.id
        return str(emoji)

    def _build_status_embed(self, channel_name: str) -> discord.Embed:
        """配信状況をDiscord向けにEmbed化（視聴者数は非表示）"""
        login = channel_name.lower()
        info = self._live_streams.get(login)
        if info:
            description = info.title or "（タイトル未設定）"
            embed = discord.Embed(
                title="配信中",
                description=description,
                color=discord.Color.green(),
            )
            embed.add_field(name="カテゴリ", value=info.game_name or "未設定", inline=True)
            embed.add_field(name="状態", value="ライブ中", inline=True)
            if info.thumbnail_url:
                embed.set_image(url=info.thumbnail_url)
        else:
            embed = discord.Embed(
                title="オフライン",
                description="現在配信は行われていません。",
                color=discord.Color.dark_gray(),
            )
            embed.add_field(name="カテゴリ", value="なし", inline=True)
            embed.add_field(name="状態", value="オフライン", inline=True)
        embed.set_author(name=channel_name)
        return embed

    async def _get_app_access_token(self) -> str:
        """Client Credentials FlowでApp Access Tokenを取得"""
        now = time.monotonic()
        if self._app_access_token and now < self._app_access_token_expires_at:
            return self._app_access_token
        session = self._ensure_http_session()
        params = {
            "client_id": self._twitch_client_id,
            "client_secret": self._twitch_client_secret,
            "grant_type": "client_credentials",
        }
        async with session.post("https://id.twitch.tv/oauth2/token", params=params) as response:
            if response.status != 200:
                error_text = await response.text()
                raise RuntimeError(f"Twitchのトークンを取得できませんでした: {error_text}")
            payload = await response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Twitch APIから有効なトークンが返却されませんでした。")
        expires_in = int(payload.get("expires_in", 0))
        self._app_access_token = token
        self._app_access_token_expires_at = now + max(expires_in - 60, 0)
        return token

    async def _close_http_session(self) -> None:
        """HTTPセッションをクローズ"""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        self._http_session = None

    async def start(self) -> None:
        """Discord Botを起動"""
        await self._discord_bot.start(self._discord_token)

    async def close(self) -> None:
        """起動した各種タスクと接続を安全に終了"""
        await self._discord_bot.close()
        if self._twitch_task:
            self._twitch_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._twitch_task
        if self._relay_task:
            self._relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._relay_task
        if self._stream_monitor_task:
            self._stream_monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._stream_monitor_task
        await self._twitch_client.close()
        await self._close_http_session()


# 日本語コメント: エントリーポイント。環境変数を読み込みBotを起動
async def main() -> None:
    """環境変数を読み込み橋渡しBotを起動"""
    load_dotenv()
    discord_token = require_env("DISCORD_BOT_TOKEN")
    discord_channel_id = require_int_env("DISCORD_CHANNEL_ID")
    twitch_token = require_env("TWITCH_OAUTH_TOKEN")
    twitch_client_id = require_env("TWITCH_CLIENT_ID")
    twitch_client_secret = require_env("TWITCH_CLIENT_SECRET")
    raw_twitch_channels = os.getenv("TWITCH_CHANNELS", "").strip()
    if raw_twitch_channels:
        twitch_channels = parse_channel_list(raw_twitch_channels)
    else:
        twitch_channels = parse_channel_list(require_env("TWITCH_CHANNEL"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    bridge = TwitchDiscordBridge(
        discord_token=discord_token,
        discord_channel_id=discord_channel_id,
        twitch_token=twitch_token,
        twitch_client_id=twitch_client_id,
        twitch_client_secret=twitch_client_secret,
        twitch_channels=twitch_channels,
    )

    try:
        await bridge.start()
    except KeyboardInterrupt:
        logging.info("手動停止が要求されたため安全に終了します。")
    finally:
        await bridge.close()


if __name__ == "__main__":
    asyncio.run(main())
