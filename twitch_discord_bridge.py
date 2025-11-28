"""DiscordとTwitchを連携させる橋渡しBot"""
# 日本語コメント: 非同期制御や環境変数操作、ロギングを扱うための標準ライブラリ
import asyncio
import logging
import os
import time
from contextlib import suppress
from typing import Awaitable, Callable

# 日本語コメント: Helix APIへHTTPリクエストを送るためのaiohttpを読み込む
import aiohttp

# 日本語コメント: DiscordとTwitchの各種クライアントライブラリを読み込む
import discord
from discord.abc import Messageable
from discord.ext import commands
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


# 日本語コメント: Twitchのチャットを購読してコールバックへ流すClient拡張クラス
class TwitchMessageClient(Client):
    """TwitchのチャットメッセージをDiscord側へ渡す仲介クライアント"""

    def __init__(
        self,
        twitch_token: str,
        twitch_client_secret: str | None,
        channel_name: str,
        on_message: Callable[[Message], Awaitable[None]],
        on_ready: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        # 日本語コメント: 親クラスへ接続情報を渡して初期化
        super().__init__(
            token=twitch_token,
            initial_channels=[channel_name],
            client_secret=twitch_client_secret,
        )
        # 日本語コメント: メッセージ受信時に呼び出すコールバック
        self._on_message = on_message
        # 日本語コメント: 接続完了時に呼び出すコールバック
        self._on_ready = on_ready

    async def event_ready(self) -> None:
        """Twitch側と接続できた際に呼ばれるイベント"""
        # 日本語コメント: ログ出力と、登録済みコールバックの実行
        logging.info("Twitchへの接続が完了しました。")
        if self._on_ready:
            await self._on_ready()

    async def event_message(self, message: Message) -> None:
        """Twitchからメッセージを受信した際のイベント"""
        # 日本語コメント: エコーメッセージは無視して多重投稿を防止
        if message.echo:
            return
        # 日本語コメント: 空メッセージも無視
        if not message.content:
            return
        # 日本語コメント: 正常なメッセージをDiscord側へ橋渡し
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
        twitch_channel: str,
    ) -> None:
        # 日本語コメント: DiscordとTwitch双方の認証情報を保持
        self._discord_token = discord_token
        self._discord_channel_id = discord_channel_id
        self._twitch_client_id = twitch_client_id
        self._twitch_client_secret = twitch_client_secret
        self._twitch_channel = twitch_channel
        # 日本語コメント: Twitchからのメッセージを一時的に溜めるキュー
        self._message_queue: asyncio.Queue[str] = asyncio.Queue()
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
            channel_name=twitch_channel,
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
        # 日本語コメント: Helix APIポーリング間隔とHTTPセッション関連
        self._stream_check_interval = 60
        self._http_session: aiohttp.ClientSession | None = None
        self._app_access_token: str | None = None
        self._app_access_token_expires_at = 0.0
        # 日本語コメント: 初期状態のゲートを正しい状態へ整える
        self._refresh_relay_gate()
        # 日本語コメント: Discordイベントやスラッシュコマンドを登録
        self._register_events()

    def _register_events(self) -> None:
        """Discord側のイベントとスラッシュコマンドを登録"""
        # 日本語コメント: Discord接続完了時に各種タスクを起動
        @self._discord_bot.event
        async def on_ready() -> None:
            logging.info("Discordへの接続が完了しました。")
            if not self._tree_synced:
                await self._discord_bot.tree.sync()
                self._tree_synced = True
            if self._twitch_task is None:
                self._twitch_task = asyncio.create_task(self._twitch_client.connect())
            if self._relay_task is None:
                self._relay_task = asyncio.create_task(self._relay_loop())
            if self._stream_monitor_task is None:
                self._stream_monitor_task = asyncio.create_task(self._stream_status_loop())

        # 日本語コメント: 動作確認用の/watchコマンド
        @self._discord_bot.tree.command(name="watch", description="Twitch取得Botの稼働状況を確認します")
        async def watch_command(interaction: discord.Interaction) -> None:
            """簡易ヘルスチェック用コマンド"""
            status = "配信情報確認中" if not self._is_stream_status_known else (
                "配信停止中" if not self._is_live_blocked else "配信中のため中継停止"
            )
            await interaction.response.send_message(f"Twitch取得Botは稼働中です（状態: {status}）。", ephemeral=True)

        # 日本語コメント: 中継を開始する/startコマンド
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

        # 日本語コメント: 中継を停止する/stopコマンド
        @self._discord_bot.tree.command(name="stop", description="TwitchからDiscordへの中継を停止します")
        async def stop_command(interaction: discord.Interaction) -> None:
            """中継を停止するスラッシュコマンド"""
            if self._is_relay_paused:
                await interaction.response.send_message("中継は既に停止しています。", ephemeral=True)
                return
            self._is_relay_paused = True
            self._refresh_relay_gate()
            await interaction.response.send_message("TwitchからDiscordへの中継を停止しました。", ephemeral=True)

    def _refresh_relay_gate(self) -> None:
        """中継ゲートの開閉を状態に応じて制御"""
        # 日本語コメント: 配信状況未確認または停止条件がある場合はEventを閉じる
        if self._is_stream_status_known and not self._is_relay_paused and not self._is_live_blocked:
            self._relay_gate.set()
        else:
            self._relay_gate.clear()

    async def enqueue_twitch_message(self, message: Message) -> None:
        """Twitchから受信したメッセージをキューへ追加"""
        # 日本語コメント: 投稿者名とメッセージ本文を整形しキューへ投入
        display_name = message.author.display_name if message.author else "Unknown"
        formatted = f"{display_name}: {message.content}"
        await self._message_queue.put(formatted)

    async def _relay_loop(self) -> None:
        """キューからDiscordへメッセージを転送するループ"""
        # 日本語コメント: Bot終了まで永続的に動作する
        while True:
            content = await self._message_queue.get()
            try:
                # 日本語コメント: Eventが開くまで待機し、許可されたら送信
                await self._relay_gate.wait()
                channel = await self._resolve_channel()
                await channel.send(content)
            finally:
                # 日本語コメント: キュー処理完了を通知
                self._message_queue.task_done()

    async def _resolve_channel(self) -> Messageable:
        """送信先のDiscordチャンネルを取得"""
        # 日本語コメント: キャッシュを利用しつつ、未取得ならAPIからフェッチ
        channel = self._discord_bot.get_channel(self._discord_channel_id)
        if channel is None:
            channel = await self._discord_bot.fetch_channel(self._discord_channel_id)
        if not isinstance(channel, Messageable):
            raise RuntimeError("指定したDiscordチャンネルはメッセージ送信に対応していません。")
        return channel

    async def _stream_status_loop(self) -> None:
        """Helix APIを使い配信状況を定期的に確認するループ"""
        # 日本語コメント: 成功するまで繰り返し問い合わせ、状態変化ならログ出力とゲート更新
        while True:
            try:
                is_live = await self._fetch_stream_live_status()
                previous = self._is_live_blocked
                self._is_live_blocked = is_live
                self._is_stream_status_known = True
                if is_live and not previous:
                    logging.info("Twitch配信が開始されたため中継を停止します。")
                elif not is_live and previous:
                    logging.info("Twitch配信が終了したため中継を再開します。")
                self._refresh_relay_gate()
            except Exception:
                logging.exception("Twitchの配信状況チェックに失敗しました。一定時間後に再試行します。")
            await asyncio.sleep(self._stream_check_interval)

    async def _fetch_stream_live_status(self) -> bool:
        """Helix Streams APIから対象チャンネルの配信状況を取得"""
        # 日本語コメント: アプリケーショントークンを付与してStreamsエンドポイントを叩く
        session = self._ensure_http_session()
        token = await self._get_app_access_token()
        headers = {
            "Client-ID": self._twitch_client_id,
            "Authorization": f"Bearer {token}",
        }
        params = {"user_login": self._twitch_channel}
        async with session.get("https://api.twitch.tv/helix/streams", headers=headers, params=params) as response:
            if response.status != 200:
                error_text = await response.text()
                raise RuntimeError(f"Helix APIから配信情報を取得できませんでした: {error_text}")
            payload = await response.json()
        data = payload.get("data") or []
        return bool(data)

    def _ensure_http_session(self) -> aiohttp.ClientSession:
        """Helix API用のHTTPセッションを遅延生成"""
        # 日本語コメント: セッションが存在しないか閉じている場合のみ新規作成
        if self._http_session is None or self._http_session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._http_session = aiohttp.ClientSession(timeout=timeout)
        return self._http_session

    async def _get_app_access_token(self) -> str:
        """Client Credentials FlowでApp Access Tokenを取得"""
        # 日本語コメント: 有効期限内ならキャッシュを再利用、期限切れなら再取得
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
        # 日本語コメント: セッションが存在する場合のみ閉じる
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        self._http_session = None

    async def start(self) -> None:
        """Discord Botを起動"""
        # 日本語コメント: Discordへの接続を開始するとon_readyでTwitch接続も起動する
        await self._discord_bot.start(self._discord_token)

    async def close(self) -> None:
        """起動した各種タスクと接続を安全に終了"""
        # 日本語コメント: Discord Bot本体を停止し、並列タスクも順次キャンセル
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
    # 日本語コメント: .env読み込みと必須設定取得
    load_dotenv()
    discord_token = require_env("DISCORD_BOT_TOKEN")
    discord_channel_id = require_int_env("DISCORD_CHANNEL_ID")
    twitch_token = require_env("TWITCH_OAUTH_TOKEN")
    twitch_client_id = require_env("TWITCH_CLIENT_ID")
    twitch_client_secret = require_env("TWITCH_CLIENT_SECRET")
    twitch_channel = require_env("TWITCH_CHANNEL")

    # 日本語コメント: ログ初期化
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 日本語コメント: ブリッジを生成して起動を管理
    bridge = TwitchDiscordBridge(
        discord_token=discord_token,
        discord_channel_id=discord_channel_id,
        twitch_token=twitch_token,
        twitch_client_id=twitch_client_id,
        twitch_client_secret=twitch_client_secret,
        twitch_channel=twitch_channel,
    )

    try:
        await bridge.start()
    except KeyboardInterrupt:
        logging.info("手動停止が要求されたため安全に終了します。")
    finally:
        await bridge.close()


# 日本語コメント: スクリプトとして実行された場合のみmainを走らせる
if __name__ == "__main__":
    asyncio.run(main())
