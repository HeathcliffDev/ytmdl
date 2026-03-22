from __future__ import annotations

import base64
import datetime
import functools
import io
import re
import shutil
import subprocess
import typing
from pathlib import Path

import requests
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from mutagen.mp4 import MP4, MP4Cover
from mutagen.oggopus import OggOpus
from mutagen.flac import Picture
from PIL import Image
from yt_dlp import YoutubeDL
from yt_dlp.extractor.youtube import YoutubeTabIE
from ytmusicapi import YTMusic

from .constants import IMAGE_FILE_EXTENSION_MAP, MP4_TAGS_MAP, PREMIUM_FORMATS
from .enums import CoverFormat, DownloadMode

# Constantes para formatos Opus
OPUS_FORMATS = ["251", "250", "249", "774"]


class Downloader:
    def __init__(
        self,
        output_path: Path = Path("./YouTube Music"),
        temp_path: Path = Path("./temp"),
        cookies_path: Path = None,
        ffmpeg_path: str = "ffmpeg",
        aria2c_path: str = "aria2c",
        itag: str = "140",
        download_mode: DownloadMode = DownloadMode.YTDLP,
        po_token: str = None,
        cover_size: int = 1400,
        cover_format: CoverFormat = CoverFormat.PNG,
        cover_quality: int = 100,
        template_folder: str = "",
        template_file: str = "{track:02d} {artist} {title}",
        template_date: str = "%Y-%m-%dT%H:%M:%SZ",
        exclude_tags: str = None,
        truncate: int = None,
        silent: bool = False,
    ):
        self.output_path = output_path
        self.temp_path = temp_path
        self.cookies_path = cookies_path
        self.ffmpeg_path = ffmpeg_path
        self.aria2c_path = aria2c_path
        self.itag = itag
        self.download_mode = download_mode
        self.po_token = po_token
        self.cover_size = cover_size
        self.cover_format = cover_format
        self.cover_quality = cover_quality
        self.template_folder = template_folder
        self.template_file = template_file
        self.template_date = template_date
        self.exclude_tags = exclude_tags
        self.truncate = truncate
        self.silent = silent
        self._set_ytmusic_instance()
        self._set_ytdlp_options()
        self._set_exclude_tags()
        self._set_truncate()

    def _set_ytmusic_instance(self):
        self.ytmusic = YTMusic()

    def _set_ytdlp_options(self):
        """
        Configura yt-dlp para que use el cliente web_music cuando se proporcionen
        cookies o PO Token, simulando el comportamiento de YouTube Music en navegador.
        """
        self.ytdlp_options = {
            "quiet": True,
            "no_warnings": True,
            "noprogress": self.silent,
            "allowed_extractors": ["youtube", "youtube:tab"],
        }
        
        # Configurar extractor_args para usar web_music si hay cookies o PO Token
        if self.cookies_path is not None or self.po_token is not None:
            self.ytdlp_options["extractor_args"] = {
                "youtube": {
                    "player_client": ["web_music"],  # Forzar uso de web_music
                }
            }
            
            # Añadir PO Token si está disponible - Formato correcto: web_music.gvs+TOKEN
            if self.po_token is not None:
                # Si el token ya incluye el prefijo, no lo dupliques
                if self.po_token.startswith("web_music.gvs+"):
                    token_value = self.po_token
                else:
                    token_value = f"web_music.gvs+{self.po_token}"
                
                self.ytdlp_options["extractor_args"]["youtube"]["po_token"] = [token_value]
        
        # Añadir cookies si están disponibles
        if self.cookies_path is not None:
            self.ytdlp_options["cookiefile"] = str(self.cookies_path)

    def _set_exclude_tags(self):
        self.exclude_tags = (
            [i.lower() for i in self.exclude_tags.split(",")]
            if self.exclude_tags is not None
            else []
        )

    def _set_truncate(self):
        if self.truncate is not None:
            self.truncate = None if self.truncate < 4 else self.truncate

    @functools.lru_cache()
    def _get_ytdlp_info(self, url: str) -> dict:
        with YoutubeDL(
            {
                **self.ytdlp_options,
                "extract_flat": True,
            }
        ) as ydl:
            return ydl.extract_info(url, download=False)

    def get_download_queue(
        self,
        url: str,
    ) -> typing.Generator[dict, None, None]:
        artist_match = re.match(YoutubeTabIE._VALID_URL, url)
        if artist_match and artist_match.group("channel_type") == "channel":
            yield from self._get_download_queue_artist(artist_match.group("id"))
        else:
            yield from self._get_download_queue_url(url)

    def _get_download_queue_url(
        self,
        url: str,
    ) -> typing.Generator[dict, None, None]:
        ytdlp_info = self._get_ytdlp_info(url.split("&")[0])
        if "MPREb_" in ytdlp_info["webpage_url_basename"]:
            ytdlp_info = self._get_ytdlp_info(ytdlp_info["url"])
        if "playlist" in ytdlp_info["webpage_url_basename"]:
            for entry in ytdlp_info["entries"]:
                yield entry
        if "watch" in ytdlp_info["webpage_url_basename"]:
            yield ytdlp_info

    def _get_download_queue_artist(
        self,
        channel_id: str,
    ) -> typing.Generator[dict, None, None]:
        artist = self.ytmusic.get_artist(channel_id)
        media_type = inquirer.select(
            message=f'Select which type to download for artist "{artist["name"]}":',
            choices=[
                Choice(
                    name="Albums",
                    value="albums",
                ),
                Choice(
                    name="Singles",
                    value="singles",
                ),
            ],
            validate=lambda result: artist.get(result, {}).get("results"),
            invalid_message="The artist doesn't have any items of this type",
        ).execute()
        artist_albums = (
            self.ytmusic.get_artist_albums(
                artist[media_type]["browseId"], artist[media_type]["params"]
            )
            if artist[media_type].get("browseId") and artist[media_type].get("params")
            else artist[media_type]["results"]
        )
        choices = [
            Choice(
                name=" | ".join(
                    [
                        album.get("year", "Unknown"),
                        album["title"],
                    ]
                ),
                value=album,
            )
            for album in artist_albums
        ]
        selected = inquirer.select(
            message="Select which items to download: (Year | Title)",
            choices=choices,
            multiselect=True,
        ).execute()
        for album in selected:
            yield from self._get_download_queue_url(
                "https://music.youtube.com/browse/" + album["browseId"]
            )

    @staticmethod
    def _get_artist(artist_list: dict) -> str:
        if len(artist_list) == 1:
            return artist_list[0]["name"]
        return (
            ", ".join([i["name"] for i in artist_list][:-1])
            + f' & {artist_list[-1]["name"]}'
        )

    def get_ytmusic_watch_playlist(self, video_id: str) -> dict | None:
        ytmusic_watch_playlist = self.ytmusic.get_watch_playlist(video_id)
        if not ytmusic_watch_playlist["tracks"][0].get("album"):
            return None
        return ytmusic_watch_playlist

    @functools.lru_cache()
    def get_ytmusic_album(self, browse_id: str) -> dict:
        return self.ytmusic.get_album(browse_id)

    @staticmethod
    def _get_datetime_obj(date: str) -> datetime.datetime:
        return datetime.datetime.strptime(date, "%Y")

    def get_tags(self, ytmusic_watch_playlist: dict) -> dict:
        video_id = ytmusic_watch_playlist["tracks"][0]["videoId"]
        ytmusic_album = self.get_ytmusic_album(
            ytmusic_watch_playlist["tracks"][0]["album"]["id"]
        )
        tags = {
            "album": ytmusic_album["title"],
            "album_artist": self._get_artist(ytmusic_album["artists"]),
            "artist": self._get_artist(ytmusic_watch_playlist["tracks"][0]["artists"]),
            "url": f"https://music.youtube.com/watch?v={video_id}",
            "media_type": 1,
            "title": ytmusic_watch_playlist["tracks"][0]["title"],
            "track_total": ytmusic_album["trackCount"],
            "video_id": video_id,
        }
        for index, entry in enumerate(
            self._get_ytdlp_info(
                f'https://www.youtube.com/playlist?list={ytmusic_album["audioPlaylistId"]}'
            )["entries"]
        ):
            if entry["id"] == video_id:
                if ytmusic_album["tracks"][index]["isExplicit"]:
                    tags["rating"] = 1
                else:
                    tags["rating"] = 0
                tags["track"] = index + 1
                break
        if ytmusic_watch_playlist.get("lyrics"):
            lyrics_ytmusic = self.ytmusic.get_lyrics(ytmusic_watch_playlist["lyrics"])
            if lyrics_ytmusic is not None and lyrics_ytmusic.get("lyrics"):
                tags["lyrics"] = lyrics_ytmusic["lyrics"]
        datetime_obj = (
            self._get_datetime_obj(ytmusic_album["year"])
            if ytmusic_album.get("year")
            else None
        )
        if datetime_obj:
            tags["date"] = datetime_obj.strftime(self.template_date)
        return tags

    def get_lyrics_synced_timestamp_lrc(self, time: int) -> str:
        lrc_timestamp = datetime.datetime.fromtimestamp(
            time / 1000.0,
            tz=datetime.timezone.utc,
        )
        return lrc_timestamp.strftime("%M:%S.%f")[:-4]

    def get_synced_lyrics(self, ytmusic_watch_playlist: dict) -> str:
        try:
            lyrics_ytmusic = self.ytmusic.get_lyrics(
                ytmusic_watch_playlist["lyrics"],
                True,
            )
            if (
                lyrics_ytmusic is not None
                and lyrics_ytmusic.get("lyrics")
                and lyrics_ytmusic.get("hasTimestamps")
            ):
                return (
                    "\n".join(
                        [
                            f"[{self.get_lyrics_synced_timestamp_lrc(i.start_time)}]{i.text}"
                            for i in lyrics_ytmusic["lyrics"]
                        ]
                    )
                    + "\n"
                )
        except Exception:
            # Silenciosamente ignorar errores de letras (no afecta la descarga)
            pass
        return None
        
    def get_synced_lyrics_path(self, final_path: Path) -> Path:
        return final_path.with_suffix(".lrc")

    def save_synced_lyrics(self, synced_lyrics_path: Path, synced_lyrics: str):
        if synced_lyrics:
            synced_lyrics_path.parent.mkdir(parents=True, exist_ok=True)
            synced_lyrics_path.write_text(synced_lyrics, encoding="utf8")

    def get_sanitized_string(self, dirty_string: str, is_folder: bool) -> str:
        dirty_string = re.sub(r'[\\/:*?"<>|;]', "_", dirty_string)
        if is_folder:
            dirty_string = dirty_string[: self.truncate]
            if dirty_string.endswith("."):
                dirty_string = dirty_string[:-1] + "_"
        else:
            if self.truncate is not None:
                dirty_string = dirty_string[: self.truncate - 4]
        return dirty_string.strip()

    def get_track_temp_path(self, video_id: str) -> Path:
        if self.itag in OPUS_FORMATS:
            # yt-dlp descarga Opus en contenedor webm
            return self.temp_path / f"{video_id}_temp.webm"
        else:
            return self.temp_path / f"{video_id}_temp.m4a"

    def get_remuxed_path(self, video_id: str) -> Path:
        if self.itag in OPUS_FORMATS:
            # El archivo remuxed será .ogg
            return self.temp_path / f"{video_id}_remuxed.ogg"
        else:
            return self.temp_path / f"{video_id}_remuxed.m4a"

    def get_cover_path(self, final_path: Path, file_extension: str) -> Path:
        return final_path.parent / ("Cover" + file_extension)

    def get_final_path(self, tags: dict) -> Path:
        # Crear una copia de tags para manipular sin afectar los originales
        tags_for_filename = tags.copy()
    
        # Convertir valores numéricos a enteros para formateo correcto
        numeric_keys = ['track', 'track_total', 'disc', 'disc_total']
        for key in numeric_keys:
            if key in tags_for_filename:
                value = tags_for_filename[key]
                try:
                    # Si es string, intentar convertir a entero
                    if isinstance(value, str):
                        # Si tiene formato "0/1", tomar solo la primera parte
                        if '/' in value:
                            value = value.split('/')[0]
                        tags_for_filename[key] = int(value)
                    elif value is None:
                        tags_for_filename[key] = 0
                    # Si ya es número, mantenerlo
                except (ValueError, TypeError):
                    # Si falla la conversión, usar 0
                    tags_for_filename[key] = 0
                    print(f"Advertencia: No se pudo convertir {key}='{value}' a entero, usando 0")
    
        # Asegurar valores por defecto para campos críticos
        tags_for_filename.setdefault('track', 0)
        tags_for_filename.setdefault('title', 'Unknown Title')
        tags_for_filename.setdefault('artist', 'Unknown Artist')
    
        # Dividir plantillas
        final_path_folder = self.template_folder.split("/")
        final_path_file = self.template_file.split("/")
    
        # Procesar carpetas
        processed_folders = []
        for folder_part in final_path_folder:
            try:
                formatted = folder_part.format(**tags_for_filename)
            except (KeyError, ValueError) as e:
                print(f"DEBUG - Error formateando carpeta '{folder_part}': {e}")
                formatted = folder_part
            processed_folders.append(self.get_sanitized_string(formatted, True))
    
        # Procesar partes del archivo (excepto la última)
        processed_file_parts = []
        for file_part in final_path_file[:-1]:
            try:
                formatted = file_part.format(**tags_for_filename)
            except (KeyError, ValueError) as e:
                print(f"DEBUG - Error formateando parte '{file_part}': {e}")
                formatted = file_part
            processed_file_parts.append(self.get_sanitized_string(formatted, True))
    
        # Procesar última parte (nombre del archivo)
        try:
            last_part = final_path_file[-1].format(**tags_for_filename)
        except (KeyError, ValueError) as e:
            print(f"DEBUG - Error formateando última parte '{final_path_file[-1]}': {e}")
            last_part = final_path_file[-1]
    
        # Determinar extensión según el itag
        if self.itag in OPUS_FORMATS:
            extension = ".ogg"  # Usar .ogg para mejor compatibilidad en Android
        else:
            extension = ".m4a"
    
        # Añadir extensión y sanitizar
        final_filename = self.get_sanitized_string(last_part, False) + extension
        processed_file_parts.append(final_filename)
    
        # Construir y devolver ruta completa
        return self.output_path.joinpath(*processed_folders).joinpath(*processed_file_parts)

    def download(self, video_id: str, temp_path: Path, progress_callback=None):
        import os
        import sys
        from contextlib import redirect_stdout, redirect_stderr
        import subprocess
    
        # Guardar los descriptores originales
        original_stdout = sys.stdout
        original_stderr = sys.stderr
    
        try:
            # Redirigir a devnull
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')
        
            with YoutubeDL(
                {
                    **self.ytdlp_options,
                    "external_downloader": (
                        {
                            "default": self.aria2c_path,
                        }
                        if self.download_mode == DownloadMode.ARIA2C
                        else None
                    ),
                    "fixup": "never",
                    "format": self.itag,
                    "outtmpl": str(temp_path),
                    "progress_hooks": [progress_callback] if progress_callback else [],
                    "quiet": True,
                    "no_warnings": True,
                    "no_color": True,
                    "no_progress": True,
                    "progress": False,
                    "verbose": False,
                    "print": [],
                    "logger": None,  # Desactivar logger
                    "logtostderr": False,
                }
            ) as ydl:
                ydl.download("https://music.youtube.com/watch?v=" + video_id)
        finally:
            # Restaurar los descriptores originales
            sys.stdout.close()
            sys.stderr.close()
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    def remux(self, temp_path: Path, remuxed_path: Path):
        if self.itag in OPUS_FORMATS:
            # Convertir de webm a ogg (contenedor Ogg para mejor compatibilidad en Android)
            command = [
                self.ffmpeg_path,
                "-loglevel",
                "error",
                "-i",
                temp_path,        # Archivo .webm con audio Opus
                "-c",
                "copy",           # Copiar sin recodificar
                "-f",
                "ogg",            # Forzar contenedor ogg (mejor compatibilidad en Android)
                remuxed_path,     # Salida .ogg
            ]
        else:
            # Código existente para AAC/MP4
            command = [
                self.ffmpeg_path,
                "-loglevel",
                "error",
                "-i",
                temp_path,
            ]
            if self.itag not in ("141", "140", "139"):
                command.extend(
                    [
                        "-f",
                        "mp4",
                    ]
                )
            command.extend([
                "-movflags",
                "+faststart",
                "-c",
                "copy",
                remuxed_path,
            ])
        
        subprocess.run(command, check=True)

    @staticmethod
    @functools.lru_cache()
    def get_url_response_bytes(url: str) -> bytes:
        return requests.get(url).content

    def get_cover_url(self, ytmusic_watch_playlist: dict) -> str:
        return (
            f'{ytmusic_watch_playlist["tracks"][0]["thumbnail"][0]["url"].split("=")[0]}'
            + (
                "=d"
                if self.cover_format == CoverFormat.RAW
                else f'=w{self.cover_size}-l{self.cover_quality}-{"rj" if self.cover_format == CoverFormat.JPG else "rp"}'
            )
        )

    def get_cover_file_extension(self, cover_url: str) -> str:
        image_obj = Image.open(io.BytesIO(self.get_url_response_bytes(cover_url)))
        image_format = image_obj.format.lower()
        return IMAGE_FILE_EXTENSION_MAP.get(image_format, f".{image_format}")

    def apply_tags(
        self,
        path: Path,
        tags: dict,
        cover_url: str,
    ):
        if self.itag in OPUS_FORMATS:
            self._apply_tags_opus(path, tags, cover_url)
        else:
            self._apply_tags_mp4(path, tags, cover_url)

    def _apply_tags_mp4(self, path: Path, tags: dict, cover_url: str):
        to_apply_tags = [
            tag_name for tag_name in tags.keys() if tag_name not in self.exclude_tags
        ]
        mp4_tags = {}
        for tag_name in to_apply_tags:
            if tag_name in ("disc", "disc_total"):
                if mp4_tags.get("disk") is None:
                    mp4_tags["disk"] = [[0, 0]]
                if tag_name == "disc":
                    mp4_tags["disk"][0][0] = tags[tag_name]
                elif tag_name == "disc_total":
                    mp4_tags["disk"][0][1] = tags[tag_name]
            elif tag_name in ("track", "track_total"):
                if mp4_tags.get("trkn") is None:
                    mp4_tags["trkn"] = [[0, 0]]
                if tag_name == "track":
                    mp4_tags["trkn"][0][0] = tags[tag_name]
                elif tag_name == "track_total":
                    mp4_tags["trkn"][0][1] = tags[tag_name]
            if (
                MP4_TAGS_MAP.get(tag_name) is not None
                and tags.get(tag_name) is not None
            ):
                mp4_tags[MP4_TAGS_MAP[tag_name]] = [tags[tag_name]]
        if "cover" not in self.exclude_tags and self.cover_format != CoverFormat.RAW:
            mp4_tags["covr"] = [
                MP4Cover(
                    self.get_url_response_bytes(cover_url),
                    imageformat=(
                        MP4Cover.FORMAT_JPEG
                        if self.cover_format == CoverFormat.JPG
                        else MP4Cover.FORMAT_PNG
                    ),
                )
            ]
        mp4 = MP4(path)
        mp4.clear()
        mp4.update(mp4_tags)
        mp4.save()

    def _apply_tags_opus(self, path: Path, tags: dict, cover_url: str):
        """Aplicar tags a archivos Opus usando mutagen"""
        
        # Filtrar tags excluidos
        to_apply_tags = [
            tag_name for tag_name in tags.keys() 
            if tag_name not in self.exclude_tags
        ]
    
        # Mapeo de tags estándar para Opus/Ogg
        opus_tag_map = {
            "album": "album",
            "album_artist": "albumartist",
            "artist": "artist",
            "date": "date",
            "title": "title",
            "track": "tracknumber",
            "track_total": "totaltracks",
            "disc": "discnumber",
            "disc_total": "totaldiscs",
            "lyrics": "lyrics",
            "url": "url",
            "rating": "rating",
        }
    
        # Crear archivo Opus
        audio = OggOpus(path)
        audio.clear()
    
        # Aplicar tags de texto
        for tag_name in to_apply_tags:
            if opus_tag_map.get(tag_name) and tags.get(tag_name):
                value = tags[tag_name]
            
                # Formateo especial para tracknumber (ej: "1/10")
                if tag_name == "track" and tags.get("track_total"):
                    audio[opus_tag_map["track"]] = f"{value}/{tags['track_total']}"
                elif tag_name == "track":
                    audio[opus_tag_map["track"]] = str(value)
                # Formateo especial para discnumber
                elif tag_name == "disc" and tags.get("disc_total"):
                    audio[opus_tag_map["disc"]] = f"{value}/{tags['disc_total']}"
                elif tag_name == "disc":
                    audio[opus_tag_map["disc"]] = str(value)
                # Tags normales
                elif opus_tag_map[tag_name] and tag_name not in ["track_total", "disc_total"]:
                    audio[opus_tag_map[tag_name]] = str(value)
    
        # Añadir carátula si no está excluida
        if "cover" not in self.exclude_tags and cover_url:
            from mutagen.flac import Picture
        
            picture = Picture()
            picture.type = 3  # Front Cover
            picture.mime = "image/jpeg" if self.cover_format == CoverFormat.JPG else "image/png"
        
            # Obtener la imagen para sus dimensiones
            cover_bytes = self.get_url_response_bytes(cover_url)
        
            # Obtener dimensiones de la imagen usando PIL
            try:
                img = Image.open(io.BytesIO(cover_bytes))
                picture.width = img.width
                picture.height = img.height
            except Exception as e:
                # Si falla, usar valores por defecto
                picture.width = 0
                picture.height = 0
                print(f"Advertencia: No se pudieron obtener dimensiones de la imagen: {e}")
        
            picture.depth = 24  # Profundidad de color (24 bits)
            picture.data = cover_bytes
        
            # Codificar a base64 y luego a string
            picture_data = picture.write()  # Esto devuelve bytes
            picture_b64 = base64.b64encode(picture_data).decode('utf-8')  # Convertir a string base64
            audio["metadata_block_picture"] = [picture_b64]  # Lista de strings
    
        audio.save()

    def move_to_output_path(
        self,
        remuxed_path: Path,
        final_path: Path,
    ):
        final_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(remuxed_path, final_path)

    @functools.lru_cache()
    def save_cover(self, cover_path: Path, cover_url: str):
        cover_path.write_bytes(self.get_url_response_bytes(cover_url))

    def cleanup_temp_path(self):
        shutil.rmtree(self.temp_path)